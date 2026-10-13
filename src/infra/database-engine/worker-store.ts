import type { SqliteWorkerStoreOptions } from "../sqlite-worker-broker.types.js";
import type { SqliteWorkerOperations, SqliteWorkerStore } from "../sqlite-worker-contract.js";
import {
  openSqliteWorkerStore,
  reopenFailedSqliteWorkerStore,
  runSqliteWorkerStoreOperation,
  runSqliteWorkerStoreWrite,
} from "../sqlite-worker-store.js";
import type { DatabaseEngineInput } from "./config.js";
import { observeDatabaseRequest } from "./observation.js";

function observeScope<Operations extends SqliteWorkerOperations>(
  scope: Pick<SqliteWorkerStore<Operations>, "execute">,
  input: DatabaseEngineInput,
): Pick<SqliteWorkerStore<Operations>, "execute"> {
  return {
    execute: (command, options) =>
      observeDatabaseRequest(input ? "postgres" : "sqlite", () => scope.execute(command, options)),
  };
}

export async function openEngineWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions & { existingOnly?: false; target?: never },
  input: DatabaseEngineInput,
): Promise<SqliteWorkerStore<Operations>> {
  const engine = input ? "postgres" : "sqlite";
  const store = await observeDatabaseRequest(engine, () =>
    openSqliteWorkerStore<Operations>(options),
  );
  return observeEngineStore(store, engine, input);
}

/**
 * Return a usable store for the same database after its worker slot failed.
 *
 * A failed slot keeps its error, so every later call through that store throws
 * the same text until the gateway restarts: the dead actor still owns the path
 * and a plain reopen resolves to it again. Retiring that actor is what lets the
 * next open reach a fresh worker. The failed call is not replayed here — a
 * dispatched command's outcome belongs to its caller.
 */
export async function reopenFailedEngineWorkerStore<Operations extends SqliteWorkerOperations>(
  store: SqliteWorkerStore<Operations>,
  options: SqliteWorkerStoreOptions & { existingOnly?: false; target?: never },
  input: DatabaseEngineInput,
): Promise<SqliteWorkerStore<Operations>> {
  const engine = input ? "postgres" : "sqlite";
  const reopened = await observeDatabaseRequest(engine, () =>
    reopenFailedSqliteWorkerStore<Operations>(store, options),
  );
  return observeEngineStore(reopened, engine, input);
}

function observeEngineStore<Operations extends SqliteWorkerOperations>(
  store: SqliteWorkerStore<Operations>,
  engine: "sqlite" | "postgres",
  input: DatabaseEngineInput,
): SqliteWorkerStore<Operations> {
  const scope = observeScope({ execute: store.execute.bind(store) }, input);
  store.execute = scope.execute;
  const close = store.close.bind(store);
  store.close = () => observeDatabaseRequest(engine, close);
  return store;
}

/** The selected adapter owns the host admission envelope, not a store facade. */
export function runEngineStoreOperation<Operations extends SqliteWorkerOperations, T>(
  store: SqliteWorkerStore<Operations>,
  input: DatabaseEngineInput,
  writes: boolean,
  operation: (scope: Pick<SqliteWorkerStore<Operations>, "execute">) => Promise<T>,
  assertCurrent: (() => void) | undefined,
  databasePath: string,
): Promise<T> {
  const observed = (scope: Pick<SqliteWorkerStore<Operations>, "execute">) =>
    operation(observeScope(scope, input));
  if (input && writes) {
    return runSqliteWorkerStoreWrite(store, observed, assertCurrent ?? (() => {}), [databasePath]);
  }
  return assertCurrent
    ? runSqliteWorkerStoreOperation(store, observed, undefined, assertCurrent)
    : operation(store);
}

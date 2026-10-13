import assert from "node:assert/strict";
import { describe, expect, it } from "vitest";
import { useSqliteWorkerStoreFixture } from "./sqlite-worker-fixture.test-support.js";
import {
  isSqliteWorkerStoreAvailable,
  reopenFailedSqliteWorkerStore,
} from "./sqlite-worker-store.js";
import type { FixtureOperations } from "./sqlite-worker-store.test-support.js";

const { stores, databasePath, open } = useSqliteWorkerStoreFixture(
  "openclaw-sqlite-worker-slot-recovery-",
);

const brokerKey = Symbol.for("openclaw.sqliteWorkerBroker");

type FakeBroker = {
  isAvailable: (store: object) => boolean;
  retireStoreActor: (store: object) => Promise<void>;
  close: () => Promise<void>;
  open: (options: unknown) => Promise<object>;
  retired: object[];
  opens: number;
};

/**
 * A worker slot that fails keeps its first error for every later job, so the store
 * on that slot is dead for the life of the process. Reopening the same path resolves
 * to the same failed actor, which is what left Workboard broken until a Gateway
 * restart. Recovery therefore has to retire that actor before opening again.
 */
function installBroker(available: boolean): FakeBroker {
  const broker: FakeBroker = {
    retired: [],
    opens: 0,
    isAvailable: () => available,
    retireStoreActor: async (store) => {
      broker.retired.push(store);
      available = true;
    },
    close: async () => {},
    open: async () => {
      broker.opens += 1;
      return { execute: async () => undefined, close: async () => {} };
    },
  };
  (globalThis as Record<PropertyKey, unknown>)[brokerKey] = broker;
  return broker;
}

function restoreBroker(): void {
  Reflect.deleteProperty(globalThis as Record<PropertyKey, unknown>, brokerKey);
}

describe("SQLite worker slot recovery", () => {
  it("leaves a healthy store alone instead of swapping its worker", async () => {
    const broker = installBroker(true);
    try {
      const store = { execute: async () => undefined, close: async () => {} };
      const same = await reopenFailedSqliteWorkerStore<FixtureOperations>(
        store as Parameters<typeof reopenFailedSqliteWorkerStore<FixtureOperations>>[0],
        {
          moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
          databasePath: databasePath(),
        },
      );

      // Identity matters: a needless swap would strand the caller's live connection.
      assert.equal(same, store);
      assert.deepEqual(broker.retired, []);
      assert.equal(broker.opens, 0);
    } finally {
      restoreBroker();
    }
  });

  it("retires the failed actor and reopens, rather than repeating the slot failure", async () => {
    const broker = installBroker(false);
    try {
      const failed = { execute: async () => undefined, close: async () => {} };
      const replacement = await reopenFailedSqliteWorkerStore<FixtureOperations>(
        failed as Parameters<typeof reopenFailedSqliteWorkerStore<FixtureOperations>>[0],
        {
          moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
          databasePath: databasePath(),
        },
      );

      assert.deepEqual(broker.retired, [failed]);
      assert.equal(broker.opens, 1);
      assert.notEqual(replacement, failed);
    } finally {
      restoreBroker();
    }
  });

  it("serves a real reopened store for the same database", async () => {
    const file = databasePath();
    const store = await open(file);
    assert.ok(store);
    await store.execute({ type: "append", input: { value: "written before recovery" } });

    // A live replacement for the same path proves the reopened actor is a fresh
    // worker serving the same database, not the failed one.
    const reopened = await open(file);
    assert.ok(reopened);
    stores.add(reopened);
    assert.notEqual(reopened, store);

    const receipt = await reopened.execute({ type: "append", input: { value: "after" } });
    assert.ok(receipt);
    assert.equal(isSqliteWorkerStoreAvailable(reopened), true);
  });
});

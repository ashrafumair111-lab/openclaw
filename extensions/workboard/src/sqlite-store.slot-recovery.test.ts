import { SqliteWorkerError } from "openclaw/plugin-sdk/sqlite-runtime";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WorkboardSqliteOperations } from "./sqlite-store-contract.js";
import { createWorkboardSqliteStores } from "./test/database-config.js";

const { openEngineWorkerStore, reopenFailedEngineWorkerStore } = vi.hoisted(() => ({
  openEngineWorkerStore: vi.fn(),
  reopenFailedEngineWorkerStore: vi.fn(),
}));
vi.mock(import("openclaw/plugin-sdk/database-engine-runtime"), async (importOriginal) => ({
  ...(await importOriginal()),
  openEngineWorkerStore,
  reopenFailedEngineWorkerStore,
}));

const workerModuleUrl = new URL("./sqlite-store.worker.ts", import.meta.url);

type Request = {
  [K in keyof WorkboardSqliteOperations]: { type: K; input: WorkboardSqliteOperations[K]["input"] };
}[keyof WorkboardSqliteOperations];

function workerStub() {
  const execute = vi.fn(async () => ({ ok: true, value: undefined }));
  return { execute, close: vi.fn(async () => {}) };
}

beforeEach(() => {
  openEngineWorkerStore.mockReset();
  reopenFailedEngineWorkerStore.mockReset();
});

/**
 * A worker slot that fails keeps its error for every later job, and the broker keeps
 * that dead actor registered against the database path. Workboard held one store for
 * the life of the process, so every card read and write repeated the first error until
 * a Gateway restart. These prove the facade moves to a replacement worker instead.
 */
describe("Workboard SQLite worker slot recovery", () => {
  it("serves the next call from a replacement worker after a slot failure", async () => {
    const first = workerStub();
    const replacement = workerStub();
    const slotError = new SqliteWorkerError("database is locked", "unavailable");

    first.execute.mockResolvedValueOnce({ ok: true, value: { connection: 7 } });
    // The failing call the reporter saw; the facade must not replay it.
    first.execute.mockRejectedValueOnce(slotError);
    replacement.execute.mockResolvedValueOnce({ ok: true, value: { connection: 9 } });

    openEngineWorkerStore.mockResolvedValue(first);
    reopenFailedEngineWorkerStore.mockResolvedValue(replacement);

    const stores = createWorkboardSqliteStores({
      dbPath: "/unused-workboard-slot-recovery.sqlite",
      workerModuleUrl,
    });
    await expect(stores.ready).resolves.toBeUndefined();

    // The failed call still reports its own error: its outcome belongs to the caller.
    await expect(stores.cards.lookup("missing")).rejects.toBe(slotError);
    expect(reopenFailedEngineWorkerStore).toHaveBeenCalledOnce();

    // The next call runs on the replacement worker, not the dead slot.
    await expect(stores.cards.lookup("after")).resolves.toBeUndefined();
    expect(replacement.execute.mock.calls.map(([request]) => request)).toEqual([
      { type: "connection.open", input: undefined },
      { type: "cards.lookup", input: { connection: 9, args: ["after"] } },
    ]);

    await stores.close();
  });

  it("does not reopen or swap the connection while the store is still usable", async () => {
    const store = workerStub();
    store.execute.mockResolvedValueOnce({ ok: true, value: { connection: 7 } });
    store.execute.mockRejectedValueOnce(new Error("card row is malformed"));
    openEngineWorkerStore.mockResolvedValue(store);
    // Recovery returns the same store when the slot is healthy.
    reopenFailedEngineWorkerStore.mockImplementation(async (failed) => failed);

    const stores = createWorkboardSqliteStores({
      dbPath: "/unused-workboard-slot-recovery.sqlite",
      workerModuleUrl,
    });
    await expect(stores.ready).resolves.toBeUndefined();

    await expect(stores.cards.lookup("bad")).rejects.toThrow("card row is malformed");
    // A data-level failure is not a worker failure: the connection must survive it.
    await expect(stores.cards.lookup("good")).resolves.toBeUndefined();
    expect(store.execute.mock.calls.map(([request]) => request)).toEqual([
      { type: "connection.open", input: undefined },
      { type: "cards.lookup", input: { connection: 7, args: ["bad"] } },
      { type: "cards.lookup", input: { connection: 7, args: ["good"] } },
    ]);

    await stores.close();
  });

  it("reports both failures when the store cannot be recovered", async () => {
    const store = workerStub();
    const slotError = new SqliteWorkerError("database or disk is full", "unavailable");
    const healError = new Error("replacement worker refused admission");
    store.execute.mockResolvedValueOnce({ ok: true, value: { connection: 7 } });
    store.execute.mockRejectedValueOnce(slotError);
    openEngineWorkerStore.mockResolvedValue(store);
    reopenFailedEngineWorkerStore.mockRejectedValue(healError);

    const stores = createWorkboardSqliteStores({
      dbPath: "/unused-workboard-slot-recovery.sqlite",
      workerModuleUrl,
    });
    await expect(stores.ready).resolves.toBeUndefined();

    // Silent breakage is the failure mode this issue reported; say what happened.
    const failure = await stores.cards.lookup("missing").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([slotError, healError]);
    expect((failure as AggregateError).message).toContain("restart the Gateway");
  });
});

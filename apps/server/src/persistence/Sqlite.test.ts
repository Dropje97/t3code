// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import type * as NodeWorkerThreads from "node:worker_threads";

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import { migrationManifest, runMigrations } from "./Migrations.ts";
import * as SqlitePersistence from "./Sqlite.ts";

const workers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const workerData = vi.hoisted((): Array<unknown> => []);

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      workers.push(this);
      workerData.push(args[1]?.workerData);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

it.effect("keeps writes moving past a 48 MiB WAL while a reader retains its snapshot", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-burst-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");
  const persistence = SqlitePersistence.layerFromPath(dbPath).pipe(
    Layer.provide(NodeServices.layer),
  );

  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const checkpointWorker = workers.at(-1)!;
    yield* sql`CREATE TABLE burst_probe(id INTEGER PRIMARY KEY, payload BLOB)`;
    yield* sql`INSERT INTO burst_probe VALUES (1, NULL)`;
    yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
    const reader = yield* Effect.acquireRelease(
      Effect.sync(() => new NodeSqlite.DatabaseSync(dbPath, { readOnly: true })),
      (database) => Effect.sync(() => database.close()),
    );
    reader.exec("BEGIN");
    assert.equal(reader.prepare("SELECT COUNT(*) AS count FROM burst_probe").get()?.count, 1);

    yield* sql.withTransaction(
      sql`INSERT INTO burst_probe VALUES (2, zeroblob(${48 * 1024 * 1024}))`,
    );
    const state = yield* Effect.callback<{
      readonly log: number;
      readonly checkpointed: number;
      readonly blocked: boolean;
    }>((resume) => {
      const onMessage = (message: {
        readonly type?: string;
        readonly log?: number;
        readonly checkpointed?: number;
        readonly blocked?: boolean;
      }) => {
        if (
          message.type === "checkpoint" &&
          typeof message.log === "number" &&
          message.log > 10_000 &&
          typeof message.checkpointed === "number" &&
          typeof message.blocked === "boolean"
        ) {
          checkpointWorker.off("message", onMessage);
          resume(
            Effect.succeed({
              log: message.log,
              checkpointed: message.checkpointed,
              blocked: message.blocked,
            }),
          );
        }
      };
      checkpointWorker.on("message", onMessage);
      checkpointWorker.ref();
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      checkpointWorker.postMessage("barrier");
      return Effect.sync(() => checkpointWorker.off("message", onMessage));
    });
    assert.isBelow(state.checkpointed, state.log);
    assert.isFalse(state.blocked);

    yield* sql.withTransaction(sql`INSERT INTO burst_probe VALUES (3, NULL)`);
    assert.equal(reader.prepare("SELECT COUNT(*) AS count FROM burst_probe").get()?.count, 1);
    reader.exec("COMMIT");
    assert.equal(reader.prepare("SELECT COUNT(*) AS count FROM burst_probe").get()?.count, 3);
  }).pipe(
    Effect.scoped,
    Effect.provide(persistence),
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

// Linux names an open, already-deleted file "<path> (deleted)" in /proc/self/fd.
// SQLite names its temp files "etilqs_*" and, without TMPDIR, puts them in
// /var/tmp while os.tmpdir() is /tmp, so match the name, not the directory.
const openDeletedTempFiles = () =>
  NodeFS.readdirSync("/proc/self/fd").flatMap((fd) => {
    try {
      const target = NodeFS.readlinkSync(`/proc/self/fd/${fd}`);
      return target.includes("/etilqs_") && target.endsWith(" (deleted)") ? [target] : [];
    } catch {
      return [];
    }
  });

it.effect.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
  "keeps a write transaction's statement journal off the temp directory",
  () => {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-temp-"));
    const persistence = SqlitePersistence.layerFromPath(
      NodePath.join(tempDir, "state.sqlite"),
    ).pipe(Layer.provide(NodeServices.layer));

    return Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE journal_probe(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)`;
      for (let id = 0; id < 64; id++) {
        yield* sql`INSERT INTO journal_probe VALUES (${id}, ${"x".repeat(4096)})`;
      }
      const before = new Set(openDeletedTempFiles());
      const opened = yield* sql.withTransaction(
        Effect.gen(function* () {
          // A multi-row update can fail halfway, so SQLite journals every page
          // it changes, here well past the 64 KiB it holds in memory by default.
          yield* sql`UPDATE journal_probe SET payload = payload || 'y'`;
          return openDeletedTempFiles().filter((target) => !before.has(target));
        }),
      );
      assert.deepEqual(opened, []);
    }).pipe(
      Effect.provide(persistence),
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
    );
  },
);

it.effect("drops planner statistics a database already has", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-stats-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");
  const persistence = () =>
    SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer));

  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE walk_probe(node_id TEXT PRIMARY KEY, parent_node_id TEXT)`;
      yield* sql`INSERT INTO walk_probe VALUES ('node-1', NULL), ('node-2', 'node-1')`;
      yield* sql`ANALYZE walk_probe`;
    }).pipe(Effect.provide(persistence()));

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const stats = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM sqlite_stat1
      `;
      assert.equal(stats[0]?.count, 0);
      // A connection still using the old stats scans the table at each step.
      const plan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        WITH RECURSIVE retained(node_id) AS (
          SELECT 'node-2'
          UNION
          SELECT parent.parent_node_id FROM walk_probe AS parent
          INNER JOIN retained ON parent.node_id = retained.node_id
        )
        SELECT node_id FROM retained
      `;
      assert.include(
        plan.map((row) => row.detail),
        "SEARCH parent USING INDEX sqlite_autoindex_walk_probe_1 (node_id=?)",
      );
    }).pipe(Effect.provide(persistence()));
  }).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

it.effect("upgrades an existing database on a read-write worker connection", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-upgrade-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");

  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA journal_mode = WAL;`;
      yield* runMigrations({ toMigrationInclusive: 81 });
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
    const startedBefore = workerData.length;

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        rows.map((row) => [row.migration_id, row.name] as const),
        migrationManifest,
      );
    }).pipe(
      Effect.provide(
        SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer)),
      ),
    );
    assert.deepInclude(workerData.slice(startedBefore), {
      filename: dbPath,
      readOnly: false,
      allowExtension: false,
      prepareCacheSize: 200,
    });
  }).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

it.effect("names a database file that is not SQLite", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-notadb-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");
  NodeFS.writeFileSync(dbPath, "not a db\n");

  return Effect.gen(function* () {
    const opened = yield* Effect.void.pipe(
      Effect.provide(
        SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer)),
      ),
      Effect.result,
    );

    assert.equal(opened._tag, "Failure");
    if (opened._tag === "Failure") {
      assert.isTrue(SqlError.isSqlError(opened.failure));
      assert.include(opened.failure.message, dbPath);
    }
  }).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

const ownT3Home = Effect.fn(function* () {
  const { serverRuntimeStatePath } = yield* ServerConfig.ServerConfig;
  // An empty home, so no boot service installed on this machine is read.
  const home = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped();
  yield* ServerOwnership.acquireServerOwnership(serverRuntimeStatePath).pipe(
    Effect.provide(ProcessRunner.layer),
    Effect.provideService(HostProcess.HomeDirectory, home),
    Effect.provideService(HostProcess.Environment, {}),
  );
});

const migrateTo = (toMigrationInclusive?: number) =>
  Effect.gen(function* () {
    const { dbPath } = yield* ServerConfig.ServerConfig;
    yield* runMigrations({ toMigrationInclusive }).pipe(
      Effect.provide(NodeSqliteClient.layer({ filename: dbPath })),
    );
  });

const latestRecordedMigrationId = Effect.gen(function* () {
  const { dbPath } = yield* ServerConfig.ServerConfig;
  const rows = yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return yield* sql<{ readonly id: number }>`
      SELECT MAX(migration_id) AS id FROM effect_sql_migrations
    `;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
  return rows[0]?.id;
});

const layerT3Home = ServerConfig.layerTest(process.cwd(), { prefix: "t3-sqlite-cli-" });

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))("layerCliConfig", (it) => {
  it.effect("does not migrate a T3 home a running server owns", () =>
    Effect.gen(function* () {
      const recorded = Math.max(...migrationManifest.map(([id]) => id)) - 1;
      yield* migrateTo(recorded);
      yield* ownT3Home();

      const opened = yield* Effect.void.pipe(
        Effect.provide(SqlitePersistence.layerCliConfig),
        Effect.result,
      );

      assert.equal(yield* latestRecordedMigrationId, recorded);
      assert.equal(opened._tag, "Failure");
      if (opened._tag === "Failure") {
        assert.instanceOf(opened.failure, SqlitePersistence.ServerOwnsDatabaseError);
      }
    }).pipe(Effect.scoped, Effect.provide(layerT3Home)),
  );

  it.effect("opens a current T3 home while a running server owns it", () =>
    Effect.gen(function* () {
      yield* migrateTo();
      const { dbPath } = yield* ServerConfig.ServerConfig;
      yield* Effect.void.pipe(Effect.provide(SqlitePersistence.layerFromPath(dbPath)));
      yield* ownT3Home();

      yield* Effect.void.pipe(Effect.provide(SqlitePersistence.layerCliConfig));
    }).pipe(Effect.scoped, Effect.provide(layerT3Home)),
  );

  it.effect.each(["missing", "obsolete"] as const)(
    "does not change %s local schema while a running server owns a current database",
    (state) =>
      Effect.gen(function* () {
        yield* migrateTo();
        const { dbPath } = yield* ServerConfig.ServerConfig;
        if (state === "obsolete") {
          yield* Effect.void.pipe(Effect.provide(SqlitePersistence.layerFromPath(dbPath)));
          const database = new NodeSqlite.DatabaseSync(dbPath);
          try {
            database.exec(previousLocalCountsSql);
          } finally {
            database.close();
          }
        }
        const schema = () => {
          const database = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
          try {
            return database
              .prepare("SELECT type, name, sql FROM sqlite_schema ORDER BY name")
              .all();
          } finally {
            database.close();
          }
        };
        const before = schema();
        yield* ownT3Home();
        const opened = yield* Effect.void.pipe(
          Effect.provide(SqlitePersistence.layerCliConfig),
          Effect.result,
        );
        assert.equal(opened._tag, "Failure");
        if (opened._tag === "Failure") {
          assert.instanceOf(opened.failure, SqlitePersistence.ServerOwnsDatabaseError);
        }
        assert.deepEqual(schema(), before);
        assert.equal(
          yield* latestRecordedMigrationId,
          Math.max(...migrationManifest.map(([id]) => id)),
        );
      }).pipe(Effect.scoped, Effect.provide(layerT3Home)),
  );
});

// Installed by the previous deployed build, before main migrations 81–85.
const previousLocalIndexSql = `
CREATE INDEX IF NOT EXISTS local_projection_threads_active_updated_idx
      ON orchestration_v2_projection_threads(updated_at, thread_id)
      WHERE deleted_at IS NULL AND archived_at IS NULL;
CREATE INDEX IF NOT EXISTS local_projection_threads_pull_requests_guarded_idx
      ON orchestration_v2_projection_threads(updated_at, thread_id)
      WHERE deleted_at IS NULL AND archived_at IS NULL
        AND CASE WHEN json_valid(payload_json)
          THEN json_array_length(payload_json, '$.pullRequests') > 0 ELSE 0 END;
CREATE INDEX IF NOT EXISTS local_projection_provider_turns_live_idx
      ON orchestration_v2_projection_provider_turns(thread_id, status, run_attempt_id)
      WHERE status IN ('pending', 'starting', 'running', 'waiting');
CREATE INDEX IF NOT EXISTS local_projection_subagents_recovery_private_idx
      ON orchestration_v2_projection_subagents(thread_id, status, provider_thread_id)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_turn_items_type_private_idx
      ON orchestration_v2_projection_turn_items(thread_id, type, ordinal, turn_item_id)
      WHERE type <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_message_item_order_private_idx
      ON orchestration_v2_projection_turn_items(thread_id, json_extract(payload_json, '$.messageId'), ordinal)
      WHERE type IN ('user_message', 'assistant_message') AND type <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_runs_completed_private_idx
      ON orchestration_v2_projection_runs(thread_id, completed_at, ordinal, run_id)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_threads_limit_wake_guarded_idx
      ON orchestration_v2_projection_threads(
        (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.limitRecovery.resetAt') END),
        (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.snoozedUntil') END))
      WHERE deleted_at IS NULL AND CASE WHEN json_valid(payload_json)
        THEN json_extract(payload_json, '$.limitRecovery.autoResume') = 1 ELSE 0 END;
CREATE INDEX IF NOT EXISTS local_projection_turn_items_timeline_private_idx
      ON orchestration_v2_projection_turn_items(thread_id, ordinal, turn_item_id, run_id, node_id,
        parent_item_id, type, status,
        (CASE WHEN type IN ('user_message', 'assistant_message') THEN json_extract(payload_json, '$.messageId') END),
        (CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.inputIntent') END),
        (CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.createdBy') END),
        (CASE WHEN type = 'dynamic_tool' AND (payload_json LIKE '%htmlRender%' OR payload_json LIKE '%t3McpApp%' OR payload_json LIKE '%"viewedImagePath"%' OR payload_json LIKE '%"toolName":"%delegate_task"%') THEN 1 ELSE 0 END))
      WHERE type <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_nodes_root_status_private_idx
      ON orchestration_v2_projection_nodes(node_id, root_node_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_runs_timeline_metadata_private_idx
      ON orchestration_v2_projection_runs(thread_id, ordinal, run_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_attempts_timeline_metadata_ids_private_idx
      ON orchestration_v2_projection_run_attempts(thread_id, run_id, attempt_ordinal, root_node_id, attempt_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_nodes_thread_root_status_private_idx
      ON orchestration_v2_projection_nodes(thread_id, node_id, root_node_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_run_attempts_latest_private_idx
      ON orchestration_v2_projection_run_attempts(run_id, root_node_id, attempt_ordinal, attempt_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_runs_status_private_idx
      ON orchestration_v2_projection_runs(run_id, status)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_messages_compact_private_idx
      ON orchestration_v2_projection_messages(thread_id, lower(trim(json_extract(payload_json, '$.text'),
        char(9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200,
          8201, 8202, 8232, 8233, 8239, 8287, 12288, 65279))))
      WHERE role = 'user' AND role <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_nodes_structural_idx
      ON orchestration_v2_projection_nodes(thread_id)
      WHERE kind NOT IN ('tool_call', 'assistant_message', 'reasoning')
        OR status NOT IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
        OR run_id IS NULL;
CREATE INDEX IF NOT EXISTS local_projection_turn_items_subagent_private_idx
      ON orchestration_v2_projection_turn_items(thread_id,
        (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.subagentId') END),
        ordinal, turn_item_id)
      WHERE type = 'subagent' AND type <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_attempts_live_private_idx
      ON orchestration_v2_projection_run_attempts(thread_id, root_node_id)
      WHERE status IN ('pending','starting','running','waiting') AND status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_provider_turns_node_private_idx
      ON orchestration_v2_projection_provider_turns(thread_id, node_id)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_provider_turns_usage_private_idx
      ON orchestration_v2_projection_provider_turns(thread_id,
        (CASE WHEN json_valid(payload_json)
          THEN julianday(json_extract(payload_json, '$.tokenUsage.updatedAt')) END) DESC,
        provider_thread_id, ordinal)
      WHERE (CASE WHEN json_valid(payload_json)
        THEN json_extract(payload_json, '$.tokenUsage') END) IS NOT NULL;
CREATE INDEX IF NOT EXISTS local_projection_provider_turns_attempt_private_idx
      ON orchestration_v2_projection_provider_turns(thread_id, run_attempt_id, provider_thread_id,
        ordinal)
      WHERE status <> 'local-index-private';
CREATE INDEX IF NOT EXISTS local_projection_checkpoints_window_private_idx
      ON orchestration_v2_projection_checkpoints(thread_id, status, run_id, node_id,
        ordinal_within_scope, checkpoint_id)
      WHERE status <> 'local-index-private';
`;

const previousLocalCountsSql = `
CREATE TABLE IF NOT EXISTS local_turn_item_counts (
    thread_id TEXT NOT NULL,
    run_key TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    PRIMARY KEY (thread_id, run_key)
  ) WITHOUT ROWID;
CREATE TRIGGER local_turn_item_counts_insert
          AFTER INSERT ON orchestration_v2_projection_turn_items
          BEGIN
            INSERT INTO local_turn_item_counts (thread_id, run_key, item_count)
            VALUES (NEW.thread_id, COALESCE(NEW.run_id, ''), 1)
            ON CONFLICT (thread_id, run_key) DO UPDATE SET item_count = item_count + 1;
          END;
CREATE TRIGGER local_turn_item_counts_delete
          AFTER DELETE ON orchestration_v2_projection_turn_items
          BEGIN
            UPDATE local_turn_item_counts SET item_count = item_count - 1
            WHERE thread_id = OLD.thread_id AND run_key = COALESCE(OLD.run_id, '');
            DELETE FROM local_turn_item_counts
            WHERE thread_id = OLD.thread_id AND run_key = COALESCE(OLD.run_id, '')
              AND item_count <= 0;
          END;
CREATE TRIGGER local_turn_item_counts_move
          AFTER UPDATE OF thread_id, run_id ON orchestration_v2_projection_turn_items
          WHEN OLD.thread_id IS NOT NEW.thread_id OR OLD.run_id IS NOT NEW.run_id
          BEGIN
            UPDATE local_turn_item_counts SET item_count = item_count - 1
            WHERE thread_id = OLD.thread_id AND run_key = COALESCE(OLD.run_id, '');
            DELETE FROM local_turn_item_counts
            WHERE thread_id = OLD.thread_id AND run_key = COALESCE(OLD.run_id, '')
              AND item_count <= 0;
            INSERT INTO local_turn_item_counts (thread_id, run_key, item_count)
            VALUES (NEW.thread_id, COALESCE(NEW.run_id, ''), 1)
            ON CONFLICT (thread_id, run_key) DO UPDATE SET item_count = item_count + 1;
          END;
`;

const localIndexNames = [
  "local_projection_threads_active_updated_idx",
  "local_projection_threads_pull_requests_guarded_idx",
  "local_projection_provider_turns_live_idx",
  "local_projection_turn_items_type_private_idx",
  "local_projection_message_item_order_private_idx",
  "local_projection_threads_limit_wake_guarded_idx",
  "local_projection_runs_status_private_idx",
  "local_projection_messages_compact_private_idx",
  "local_projection_attempts_live_private_idx",
  "local_projection_provider_turns_node_private_idx",
  "local_projection_provider_turns_usage_now_private_idx",
  "local_projection_provider_turns_attempt_private_idx",
  "local_projection_checkpoints_window_private_idx",
  "local_projection_subagents_run_private_idx",
  "local_projection_subagents_routing_private_idx",
  "local_projection_threads_worktree_guarded_idx",
  "local_projection_turn_items_timeline_private_idx",
  "local_projection_nodes_root_status_private_idx",
  "local_projection_runs_timeline_metadata_private_idx",
  "local_projection_attempts_timeline_metadata_ids_private_idx",
  "local_projection_nodes_thread_root_status_private_idx",
  "local_projection_run_attempts_latest_private_idx",
  "local_projection_turn_items_subagent_private_idx",
];

// A kept production index must have the definition startup would create: IF NOT EXISTS keeps
// the installed one, so a changed definition needs a new name.
const normalizedCreates = (source: string) =>
  new Map(
    [...source.matchAll(/CREATE INDEX IF NOT EXISTS (local_projection_\w+)([^;`]*)/g)].map(
      (match) => [match[1]!, match[2]!.replace(/\s+/g, " ").replace(/\( /g, "(").trim()],
    ),
  );

it("keeps the deployed definitions of retained local indexes", () => {
  const deployed = normalizedCreates(previousLocalIndexSql);
  const installed = normalizedCreates(
    NodeFS.readFileSync(NodePath.join(import.meta.dirname, "Sqlite.ts"), "utf8"),
  );
  assert.deepEqual([...installed.keys()].toSorted(), localIndexNames.toSorted());
  for (const [name, definition] of installed) {
    if (deployed.has(name)) assert.equal(definition, deployed.get(name), name);
  }
});

it.effect(
  "replaces the deployed local schema after upstream migrations and remains idempotent",
  () => {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-local-upgrade-"));
    const dbPath = NodePath.join(tempDir, "state.sqlite");
    const persistence = () =>
      SqlitePersistence.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer));

    return Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 80 }).pipe(
        Effect.provide(NodeSqliteClient.layer({ filename: dbPath })),
      );
      const legacy = new NodeSqlite.DatabaseSync(dbPath);
      try {
        legacy.exec(previousLocalIndexSql);
        legacy.exec(previousLocalCountsSql);
        legacy.exec(`INSERT INTO orchestration_v2_projection_turn_items
        (turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json)
        VALUES ('retained-item', 'retained-thread', 'retained-run', 1, 'user_message', 'completed', '2026-11-12T00:00:00.000Z', '{}')`);
        assert.equal(
          legacy
            .prepare(
              "SELECT COUNT(*) AS n FROM sqlite_schema WHERE type = 'index' AND name GLOB 'local_projection_*'",
            )
            .get()?.n,
          23,
        );
        assert.equal(
          legacy.prepare("SELECT SUM(item_count) AS n FROM local_turn_item_counts").get()?.n,
          1,
        );
        // Earlier development definitions need only their names to exercise startup cleanup.
        for (const suffix of [
          "threads_pull_requests_idx",
          "turn_items_live_idx",
          "subagents_recovery_idx",
          "turn_items_thread_type_idx",
          "message_item_order_idx",
          "threads_limit_wake_private_idx",
          "threads_limit_wake_idx",
          "nodes_root_private_idx",
          "run_attempts_root_private_idx",
        ]) {
          legacy.exec(
            `CREATE INDEX local_projection_${suffix} ON orchestration_v2_projection_threads(thread_id)`,
          );
        }
      } finally {
        legacy.close();
      }

      let installedSchema:
        | ReadonlyArray<{
            readonly type: string;
            readonly name: string;
            readonly sql: string | null;
          }>
        | undefined;
      for (const restart of [0, 1]) {
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const localIndexes = yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema WHERE type = 'index' AND name GLOB 'local_projection_*' ORDER BY name
        `;
          assert.deepEqual(
            localIndexes.map((row) => row.name),
            localIndexNames.toSorted(),
          );
          const localCounts =
            yield* sql`SELECT name FROM sqlite_schema WHERE name GLOB 'local_turn_item_counts*'`;
          assert.deepEqual(localCounts, []);
          const upstreamCounts = yield* sql<{ readonly item_count: number }>`
          SELECT item_count FROM orchestration_v2_projection_turn_item_counts
          WHERE thread_id = 'retained-thread' AND run_key = 'retained-run'
        `;
          assert.equal(upstreamCounts[0]?.item_count, restart === 0 ? 1 : 2);
          const upstreamTriggers = yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema WHERE type = 'trigger' AND name GLOB 'orchestration_v2_turn_item_counts_*' ORDER BY name
        `;
          assert.deepEqual(
            upstreamTriggers.map((row) => row.name),
            [
              "orchestration_v2_turn_item_counts_delete",
              "orchestration_v2_turn_item_counts_insert",
              "orchestration_v2_turn_item_counts_update",
            ],
          );
          const schema = yield* sql<{
            readonly type: string;
            readonly name: string;
            readonly sql: string | null;
          }>`
          SELECT type, name, sql FROM sqlite_schema ORDER BY name
        `;
          if (installedSchema === undefined) installedSchema = schema;
          else assert.deepEqual(schema, installedSchema);
          if (restart === 0) {
            yield* sql`INSERT INTO orchestration_v2_projection_turn_items
            (turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json)
            VALUES ('new-item', 'retained-thread', 'retained-run', 2, 'user_message', 'completed', '2026-11-12T00:00:00.000Z', '{}')`;
          }
        }).pipe(Effect.provide(persistence()));
      }
    }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
    );
  },
);

// Migration 85's count triggers miss REPLACE's implicit delete when recursive_triggers is off.
it("turn-item writers preserve the upstream count triggers", () => {
  const sources = [
    NodePath.resolve(import.meta.dirname, "../orchestration-v2/ProjectionStore.ts"),
    NodePath.resolve(import.meta.dirname, "../orchestration-v2/EventSink.ts"),
  ];
  const visitMigrations = (directory: string) => {
    for (const entry of NodeFS.readdirSync(directory, { withFileTypes: true })) {
      const path = NodePath.join(directory, entry.name);
      if (entry.isDirectory()) visitMigrations(path);
      else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")) sources.push(path);
    }
  };
  visitMigrations(NodePath.join(import.meta.dirname, "Migrations"));
  const offenders = sources.filter((path) => {
    const source = NodeFS.readFileSync(path, "utf8");
    return (
      /(?:INSERT\s+OR\s+REPLACE|REPLACE)\s+INTO\s+orchestration_v2_projection_turn_items\b/i.test(
        source,
      ) ||
      /orchestration_v2_projection_turn_items[\s\S]{0,200}ON\s+CONFLICT[^;]*?DO\s+REPLACE/i.test(
        source,
      )
    );
  });
  assert.deepEqual(offenders, []);
});

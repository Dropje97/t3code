import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { hasPendingMigrations, runMigrations } from "./Migrations.ts";
import { initializeV2Database } from "./initializeV2Database.ts";
import * as ServerConfig from "../config.ts";
import * as ServerOwnership from "../serverOwnership.ts";

const WAL_SIZE_LIMIT_BYTES = 128 * 1024 * 1024;

export class ServerOwnsDatabaseError extends Schema.TaggedError<ServerOwnsDatabaseError>()(
  "ServerOwnsDatabaseError",
  { stateDir: Schema.String },
) {
  override get message(): string {
    return `Another T3 Code process, usually a running server, owns ${this.stateDir}, and this CLI would change its database schema. Use the \`t3\` that matches the running server, or stop that server, then retry. The database was not changed.`;
  }
}

// SQLITE_CORRUPT and SQLITE_NOTADB; node:sqlite reports extended codes whose low byte is the primary code.
const DAMAGED_SQLITE_CODES = new Set([11, 26]);

const isDamagedDatabase = (error: SqlError.SqlError): boolean => {
  const seen = new Set<unknown>();
  let cause: unknown = error.cause;
  while (Predicate.isObject(cause) && !seen.has(cause)) {
    seen.add(cause);
    if (typeof cause.errcode === "number" && DAMAGED_SQLITE_CODES.has(cause.errcode & 0xff)) {
      return true;
    }
    cause = cause.cause;
  }
  return false;
};

// Keeps the SqlError type so callers' error channels do not change; the original stays as the cause.
const damagedDatabaseError = (dbPath: string, cause: SqlError.SqlError) =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      cause,
      operation: "open",
      message: `The T3 Code database at ${dbPath} is damaged or is not a SQLite database, so T3 Code cannot start. To recover, quit T3 Code, then move that file and any -wal and -shm files next to it out of the folder, or replace them with a backup. T3 Code creates a new database on the next start.`,
    }),
  });

const LOCAL_READ_INDEX_NAMES = [
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
  "local_projection_provider_turns_usage_dated_private_idx",
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
] as const;
const OBSOLETE_LOCAL_SCHEMA_NAMES = [
  "local_turn_item_counts_insert",
  "local_turn_item_counts_delete",
  "local_turn_item_counts_move",
  "local_turn_item_counts",
  "local_projection_provider_turns_usage_private_idx",
  "local_projection_message_item_order_idx",
  "local_projection_nodes_root_private_idx",
  "local_projection_nodes_structural_idx",
  "local_projection_run_attempts_root_private_idx",
  "local_projection_runs_completed_private_idx",
  "local_projection_subagents_recovery_idx",
  "local_projection_subagents_recovery_private_idx",
  "local_projection_threads_limit_wake_idx",
  "local_projection_threads_limit_wake_private_idx",
  "local_projection_threads_pull_requests_idx",
  "local_projection_turn_items_live_idx",
  "local_projection_turn_items_thread_type_idx",
] as const;

const setupLocalReadIndexes = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Local read paths are installed after upstream migrations. Definition changes get new
  // names because IF NOT EXISTS preserves production's installed definitions. Private partial
  // indexes use closed-domain sentinels so unmodified upstream queries keep their plans.
  yield* sql`DROP TRIGGER IF EXISTS local_turn_item_counts_insert`;
  yield* sql`DROP TRIGGER IF EXISTS local_turn_item_counts_delete`;
  yield* sql`DROP TRIGGER IF EXISTS local_turn_item_counts_move`;
  yield* sql`DROP TABLE IF EXISTS local_turn_item_counts`;
  yield* sql`DROP INDEX IF EXISTS local_projection_provider_turns_usage_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_message_item_order_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_nodes_root_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_nodes_structural_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_run_attempts_root_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_runs_completed_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_subagents_recovery_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_subagents_recovery_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_threads_limit_wake_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_threads_limit_wake_private_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_threads_pull_requests_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_turn_items_live_idx`;
  yield* sql`DROP INDEX IF EXISTS local_projection_turn_items_thread_type_idx`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_threads_active_updated_idx
  ON orchestration_v2_projection_threads(updated_at, thread_id)
  WHERE deleted_at IS NULL AND archived_at IS NULL`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_threads_pull_requests_guarded_idx
  ON orchestration_v2_projection_threads(updated_at, thread_id)
  WHERE deleted_at IS NULL AND archived_at IS NULL
    AND CASE WHEN json_valid(payload_json)
      THEN json_array_length(payload_json, '$.pullRequests') > 0 ELSE 0 END`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_provider_turns_live_idx
  ON orchestration_v2_projection_provider_turns(thread_id, status, run_attempt_id)
  WHERE status IN ('pending', 'starting', 'running', 'waiting')`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_turn_items_type_private_idx
  ON orchestration_v2_projection_turn_items(thread_id, type, ordinal, turn_item_id)
  WHERE type <> 'local-index-private'`;
  // Production's definition; main writes valid JSON, so unguarded expressions stay writable.
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_message_item_order_private_idx
  ON orchestration_v2_projection_turn_items(thread_id, json_extract(payload_json, '$.messageId'), ordinal)
  WHERE type IN ('user_message', 'assistant_message') AND type <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_threads_limit_wake_guarded_idx
  ON orchestration_v2_projection_threads(
    (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.limitRecovery.resetAt') END),
    (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.snoozedUntil') END))
  WHERE deleted_at IS NULL AND CASE WHEN json_valid(payload_json)
    THEN json_extract(payload_json, '$.limitRecovery.autoResume') = 1 ELSE 0 END`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_runs_status_private_idx
  ON orchestration_v2_projection_runs(run_id, status)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_messages_compact_private_idx
  ON orchestration_v2_projection_messages(thread_id, lower(trim(json_extract(payload_json, '$.text'),
    char(9, 10, 11, 12, 13, 32, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200,
      8201, 8202, 8232, 8233, 8239, 8287, 12288, 65279))))
  WHERE role = 'user' AND role <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_attempts_live_private_idx
  ON orchestration_v2_projection_run_attempts(thread_id, root_node_id)
  WHERE status IN ('pending','starting','running','waiting') AND status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_provider_turns_node_private_idx
  ON orchestration_v2_projection_provider_turns(thread_id, node_id)
  WHERE status <> 'local-index-private'`;
  // julianday of 'now', 'subsec' or 'subsecond' reads the clock, which an index may not evaluate.
  // Times that start with a digit are fixed dates or day numbers; other rows stay unkeyed.
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_provider_turns_usage_dated_private_idx
  ON orchestration_v2_projection_provider_turns(thread_id,
    (CASE WHEN json_valid(payload_json) AND substr(json_extract(payload_json, '$.tokenUsage.updatedAt'), 1, 1) BETWEEN '0' AND '9' THEN julianday(json_extract(payload_json, '$.tokenUsage.updatedAt')) END) DESC,
    provider_thread_id, ordinal)
  WHERE status <> 'local-index-private' AND (CASE WHEN json_valid(payload_json)
    THEN json_extract(payload_json, '$.tokenUsage') END) IS NOT NULL`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_provider_turns_attempt_private_idx
  ON orchestration_v2_projection_provider_turns(thread_id, run_attempt_id, provider_thread_id,
    ordinal)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_checkpoints_window_private_idx
  ON orchestration_v2_projection_checkpoints(thread_id, status, run_id, node_id,
    ordinal_within_scope, checkpoint_id)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_subagents_run_private_idx
  ON orchestration_v2_projection_subagents(thread_id, run_id)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_subagents_routing_private_idx
  ON orchestration_v2_projection_subagents(thread_id, status, subagent_id, child_thread_id, provider_thread_id)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_threads_worktree_guarded_idx
  ON orchestration_v2_projection_threads(thread_id,
    (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.worktreePath') END))
  WHERE deleted_at IS NULL
    AND (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.worktreePath') END) IS NOT NULL`;
  // History metadata (readTimelineIndex, retainedWindowRows): item, run, attempt and node columns
  // without reading payload rows. Expressions must match those reads exactly.
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_turn_items_timeline_private_idx
  ON orchestration_v2_projection_turn_items(thread_id, ordinal, turn_item_id, run_id, node_id,
    parent_item_id, type, status,
    (CASE WHEN type IN ('user_message', 'assistant_message') THEN json_extract(payload_json, '$.messageId') END),
    (CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.inputIntent') END),
    (CASE WHEN type = 'user_message' THEN json_extract(payload_json, '$.createdBy') END),
    (CASE WHEN type = 'dynamic_tool' AND (payload_json LIKE '%htmlRender%' OR payload_json LIKE '%t3McpApp%' OR payload_json LIKE '%"viewedImagePath"%' OR payload_json LIKE '%"toolName":"%delegate_task"%') THEN 1 ELSE 0 END))
  WHERE type <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_nodes_root_status_private_idx
  ON orchestration_v2_projection_nodes(node_id, root_node_id, status)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_runs_timeline_metadata_private_idx
  ON orchestration_v2_projection_runs(thread_id, ordinal, run_id, status)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_attempts_timeline_metadata_ids_private_idx
  ON orchestration_v2_projection_run_attempts(thread_id, run_id, attempt_ordinal, root_node_id, attempt_id, status)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_nodes_thread_root_status_private_idx
  ON orchestration_v2_projection_nodes(thread_id, node_id, root_node_id, status)
  WHERE status <> 'local-index-private'`;
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_run_attempts_latest_private_idx
  ON orchestration_v2_projection_run_attempts(run_id, root_node_id, attempt_ordinal, attempt_id, status)
  WHERE status <> 'local-index-private'`;
  // Delegated finalization: a parent's subagent items by task ID (turnItemSubagentIds).
  yield* sql`CREATE INDEX IF NOT EXISTS local_projection_turn_items_subagent_private_idx
  ON orchestration_v2_projection_turn_items(thread_id,
    (CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.subagentId') END),
    ordinal, turn_item_id)
  WHERE type = 'subagent' AND type <> 'local-index-private'`;
});

const hasPendingLocalSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const installed = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_schema
    WHERE name IN ${sql.in([...LOCAL_READ_INDEX_NAMES, ...OBSOLETE_LOCAL_SCHEMA_NAMES])}
  `;
  const names = new Set(installed.map((row) => row.name));
  return (
    LOCAL_READ_INDEX_NAMES.some((name) => !names.has(name)) ||
    OBSOLETE_LOCAL_SCHEMA_NAMES.some((name) => names.has(name))
  );
});

// CLI commands open the server's database from another process; migrating behind its owner changes the schema it runs on.
const runMigrationsAsOwner = Effect.fn("SqlitePersistence.runMigrationsAsOwner")(function* () {
  const pendingMigrations = yield* hasPendingMigrations();
  const localSchemaPending = yield* hasPendingLocalSchema;
  if (!pendingMigrations && !localSchemaPending) return yield* runMigrations();
  const { serverRuntimeStatePath } = yield* ServerConfig.ServerConfig;
  yield* ServerOwnership.holdServerOwnershipLock(serverRuntimeStatePath).pipe(
    Effect.catchTags({
      ServerAlreadyRunningError: ({ stateDir }) =>
        Effect.fail(new ServerOwnsDatabaseError({ stateDir })),
    }),
  );
  yield* runMigrations();
  yield* setupLocalReadIndexes;
}, Effect.scoped);

// Index builds over large tables take minutes; on the main connection they block the event loop,
// so requests hang instead of getting the listener's 503. A fresh database stays on the main
// connection, which must create the first table for its auto_vacuum setting to apply.
// Local t3-code-nightly patch: local read indexes install here too.
const runMigrationsOffMainThread = Effect.fn("SqlitePersistence.runMigrationsOffMainThread")(
  function* (dbPath: string) {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql<{
      readonly count: number;
    }>`SELECT COUNT(*) AS count FROM sqlite_schema WHERE type = 'table'`;
    const migrate = runMigrations().pipe(Effect.andThen(setupLocalReadIndexes));
    if (!tables[0]?.count || (!(yield* hasPendingMigrations()) && !(yield* hasPendingLocalSchema)))
      return yield* migrate;
    return yield* Effect.gen(function* () {
      const worker = yield* SqlClient.SqlClient;
      yield* worker`PRAGMA foreign_keys = ON;`;
      yield* worker`PRAGMA synchronous = NORMAL;`;
      yield* worker`PRAGMA temp_store = MEMORY;`;
      return yield* migrate;
    }).pipe(Effect.provide(NodeSqliteClient.layerWorker(dbPath)));
  },
);

const makeLayerSetup = <E, R>(migrate: Effect.Effect<unknown, E, R>) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // CLI and server write from separate processes; wait rather than fail with SQLITE_BUSY.
      yield* sql`PRAGMA busy_timeout = 5000;`;
      yield* sql`PRAGMA foreign_keys = ON;`;
      // Existing databases need doctor's offline VACUUM to add pointer maps for this mode.
      const tables = yield* sql<{
        readonly count: number;
      }>`SELECT COUNT(*) AS count FROM sqlite_schema WHERE type = 'table'`;
      if (tables[0]?.count === 0) yield* sql`PRAGMA auto_vacuum = INCREMENTAL;`;
      yield* sql`PRAGMA journal_mode = WAL;`;
      // FULL fsyncs every event commit on the main thread; NORMAL syncs only at WAL checkpoints.
      // App crashes lose nothing; OS crashes or power loss can roll back post-checkpoint commits without corruption.
      yield* sql`PRAGMA synchronous = NORMAL;`;
      // macOS fsync misses drive caches; F_FULLFSYNC flushes them at checkpoints and is ignored elsewhere.
      yield* sql`PRAGMA checkpoint_fullfsync = ON;`;
      // Keep statement journals and sort spills off the main thread's disk path.
      // Large statements and open savepoints can increase peak memory use.
      yield* sql`PRAGMA temp_store = MEMORY;`;
      // PASSIVE checkpoints retain peak WAL size until the last connection closes.
      yield* sql.unsafe(`PRAGMA journal_size_limit = ${WAL_SIZE_LIMIT_BYTES};`);
      yield* migrate;
      // Collecting stats scans every page, and stale small-table stats cause scans after growth; use indexes by default.
      // Connections cache stats; reload after deleting them.
      const statTables = yield* sql`SELECT 1 FROM sqlite_schema WHERE name = 'sqlite_stat1'`;
      if (statTables.length > 0) {
        yield* sql`DELETE FROM sqlite_stat1;`;
        yield* sql`ANALYZE sqlite_schema;`;
      }
    }).pipe(Effect.withSpan("Sqlite.setup")),
  );

const layerSetup = makeLayerSetup(runMigrations().pipe(Effect.andThen(setupLocalReadIndexes)));

const makeLayerFromPath = <E, R>(makeSetup: (dbPath: string) => Layer.Layer<never, E, R>) =>
  Effect.fn("makeSqlitePersistenceLive")(function* (dbPath: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(path.dirname(dbPath), { recursive: true });

    return Layer.provideMerge(
      makeSetup(dbPath),
      NodeSqliteClient.layer({
        filename: dbPath,
        readerWorker: true,
        // Run checkpoint fsyncs off the event loop so slow disk flushes cannot stall clients.
        checkpointWorker: true,
        spanAttributes: {
          "db.name": path.basename(dbPath),
          "service.name": "t3code-server",
        },
      }),
    ).pipe(
      Layer.catchTag("SqlError", (error) =>
        Layer.effect(
          SqlClient.SqlClient,
          Effect.fail(
            SqlError.isSqlError(error) && isDamagedDatabase(error)
              ? damagedDatabaseError(dbPath, error)
              : error,
          ),
        ),
      ),
    );
  }, Layer.unwrap);

export const layerFromPath = makeLayerFromPath((dbPath) =>
  makeLayerSetup(runMigrationsOffMainThread(dbPath)),
);

export const layerMemory = Layer.provideMerge(
  layerSetup,
  NodeSqliteClient.layer({ filename: ":memory:" }),
);

const makeLayerConfig = <E, R>(
  fromPath: (dbPath: string) => Layer.Layer<SqlClient.SqlClient, E, R>,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { dbPath } = yield* ServerConfig.ServerConfig;
      yield* initializeV2Database(dbPath);
      return fromPath(dbPath);
    }),
  );

export const layerConfig = makeLayerConfig(layerFromPath);

/** For CLI commands: pending migrations run only while no server owns the T3 home. */
export const layerCliConfig = makeLayerConfig(
  makeLayerFromPath(() => makeLayerSetup(runMigrationsAsOwner())),
);

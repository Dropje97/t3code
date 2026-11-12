import { expect, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  OrchestrationV2TurnItemJson,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as ReadFixture from "./SendContinuationReads.testkit.ts";

const decodeItem = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestrationV2TurnItemJson));

it.effect.each(["all", "empty", "single", "multiple", "duplicate", "missing"] as const)(
  "retry history preserves main's results and byte ordering for %s run selection",
  (selection) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const sql = yield* SqlClient.SqlClient;
      const first = fixture.run(1).id;
      const second = fixture.run(2).id;
      for (const [id, runId] of [
        ["ä-tie", first],
        ["z-tie", second],
        ["A-tie", first],
        ["runless", null],
      ] as const) {
        yield* fixture.putItem(
          fixture.item(id, 50, MessageId.make(`message:${id}`), runId, "turn_start"),
        );
      }
      const base = fixture.item(
        "assistant-history",
        51,
        MessageId.make("message:assistant-history"),
        first,
        "turn_start",
      );
      yield* fixture.putItem({
        ...base,
        type: "assistant_message",
        text: "Assistant history",
        streaming: false,
      });
      yield* fixture.putItem({
        ...base,
        id: fixture.item("excluded-compaction", 52, base.messageId, first, "turn_start").id,
        type: "compaction",
        driver: null,
      });
      const runIds =
        selection === "all"
          ? undefined
          : selection === "empty"
            ? []
            : selection === "single"
              ? [first]
              : selection === "multiple"
                ? [second, first]
                : selection === "duplicate"
                  ? [first, first]
                  : [RunId.make("missing-run")];
      // Main's original SQL is the oracle; only the optimized call carries the index hint.
      const baseline = yield* sql<{ readonly payload_json: string }>`
        SELECT payload_json FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${fixture.threadId}
          AND type IN ('user_message','assistant_message','command_execution','error',
            'run_interrupt_result','file_change','proposed_plan')
          AND ${runIds === undefined ? sql`1` : sql`run_id IN ${sql.in(runIds)}`}
        ORDER BY ordinal ASC, turn_item_id ASC
      `;
      const trace = ReadFixture.traceSql();
      const actual = yield* fixture.store
        .getTurnStartHistory(fixture.threadId, runIds)
        .pipe(Effect.withTracer(trace.tracer));
      expect(actual).toEqual(baseline.map(({ payload_json }) => decodeItem(payload_json)));
      if (selection === "multiple") {
        expect(actual.filter((item) => item.ordinal === 50).map((item) => item.id)).toEqual([
          "A-tie",
          "z-tie",
          "ä-tie",
        ]);
      }
      if (runIds !== undefined) {
        const query = trace.queries.find(({ query }) =>
          query.includes("FROM orchestration_v2_projection_turn_items"),
        )!.query;
        const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
        expect(
          plan.some(({ detail }) =>
            /turn_items_thread_run_idx \(thread_id=\? AND run_id=\?\)/.test(detail),
          ),
        ).toBe(true);
        expect(plan.some(({ detail }) => /turn_items_thread_ordinal_idx/.test(detail))).toBe(false);
      }
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

const checkpointCases = [
  "missing-linked",
  "missing-runless",
  "missing-zero-linked",
  "shared-scope",
  "missing-run",
] as const;
it.effect.each(checkpointCases)("checkpoint probe preserves main's anchors for %s", (scenario) =>
  Effect.gen(function* () {
    const fixture = yield* ReadFixture.seed();
    const { store, threadId, now } = fixture;
    const sql = yield* SqlClient.SqlClient;
    const run = fixture.run(10);
    const scopeId = CheckpointScopeId.make("scope:stage1:shared");
    let eventOrdinal = 0;
    const apply = (event: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">) =>
      store.apply({
        ...event,
        id: EventId.make(`event:stage1:checkpoint:${eventOrdinal++}`),
        occurredAt: now,
      } as OrchestrationV2DomainEvent);
    yield* apply({
      type: "checkpoint-scope.created",
      threadId,
      payload: {
        id: scopeId,
        threadId,
        runId: null,
        nodeId: run.rootNodeId!,
        parentScopeId: null,
        providerThreadId: null,
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/workspace/checkpoint",
        createdAt: now,
      },
    });
    const otherThreadId = ThreadId.make("thread:stage1:scope-peer");
    const entries = [
      {
        ordinal: 0,
        status:
          scenario === "missing-runless" || scenario === "missing-zero-linked"
            ? ("missing" as const)
            : ("ready" as const),
        runId: scenario === "missing-zero-linked" ? run.id : null,
        owner: threadId,
      },
      {
        ordinal: 9,
        status:
          scenario === "missing-linked"
            ? ("missing" as const)
            : scenario === "missing-runless"
              ? ("stale" as const)
              : ("ready" as const),
        runId: run.id,
        owner: scenario === "shared-scope" ? otherThreadId : threadId,
      },
      {
        ordinal: 10,
        status:
          scenario === "missing-linked"
            ? ("missing" as const)
            : scenario === "missing-zero-linked"
              ? ("stale" as const)
              : ("ready" as const),
        runId: scenario === "missing-linked" ? null : run.id,
        owner: threadId,
      },
      { ordinal: 7, status: "ready" as const, runId: run.id, owner: threadId },
      { ordinal: 11, status: "ready" as const, runId: run.id, owner: threadId },
    ];
    for (const entry of entries) {
      yield* apply({
        type: "checkpoint.captured",
        threadId: entry.owner,
        payload: {
          id: CheckpointId.make(`checkpoint:stage1:${entry.ordinal}`),
          threadId: entry.owner,
          scopeId,
          runId: entry.runId,
          nodeId: run.rootNodeId!,
          parentCheckpointId: null,
          ordinalWithinScope: entry.ordinal,
          appRunOrdinal: null,
          ref: CheckpointRef.make(`checkpoint-ref:stage1:${entry.ordinal}`),
          status: entry.status,
          files: [],
          capturedAt: now,
        },
      });
    }
    const baseline = yield* sql<{ readonly ordinal: number }>`
      SELECT ordinal_within_scope AS ordinal FROM orchestration_v2_projection_checkpoints
      WHERE thread_id = ${threadId} AND scope_id = ${scopeId}
        AND (status = 'ready' OR (status = 'missing' AND run_id IS NOT NULL))
    `;
    const trace = ReadFixture.traceSql();
    const targetRunId = scenario === "missing-run" ? RunId.make("run:missing") : run.id;
    const context = yield* store
      .getCheckpointCaptureContext(threadId, { runId: targetRunId, scopeId })
      .pipe(Effect.withTracer(trace.tracer));
    const expectedOrdinals = new Set([
      0,
      ...(scenario === "missing-run" ? [] : [run.ordinal - 1, run.ordinal]),
    ]);
    expect(context.preservedCheckpointOrdinals).toEqual(
      baseline
        .map(({ ordinal }) => ordinal)
        .filter((ordinal) => expectedOrdinals.has(ordinal))
        .toSorted((left, right) => left - right),
    );
    expect(context.scope?.cwd).toBe("/workspace/checkpoint");
    expect(context.run?.id).toBe(scenario === "missing-run" ? undefined : run.id);
    if (scenario === "missing-linked") expect(context.preservedCheckpointOrdinals).toEqual([0, 9]);
    if (scenario === "missing-runless") expect(context.preservedCheckpointOrdinals).toEqual([10]);
    if (scenario === "missing-zero-linked")
      expect(context.preservedCheckpointOrdinals).toEqual([0, 9]);
    if (scenario === "shared-scope") expect(context.preservedCheckpointOrdinals).toEqual([0, 10]);
    const query = trace.queries.find(({ query }) =>
      query.includes("ordinal_within_scope AS ordinal"),
    )!.query;
    const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
    expect(
      plan.some(({ detail }) =>
        /checkpoints_scope_ordinal_idx \(scope_id=\? AND ordinal_within_scope=\?\)/.test(detail),
      ),
    ).toBe(true);
    expect(plan.some(({ detail }) => /checkpoints_thread_idx/.test(detail))).toBe(false);
  }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

it.effect("guarded worktree roster preserves main's valid paths, aliases and archive choices", () =>
  Effect.gen(function* () {
    const fixture = yield* ReadFixture.seed();
    const sql = yield* SqlClient.SqlClient;
    const rows = [
      { id: "z-active", worktreePath: "/workspace/shared", archivedAt: null, deletedAt: null },
      {
        id: "a-archived-alias",
        worktreePath: "/workspace/shared-alias",
        archivedAt: fixture.now,
        deletedAt: null,
      },
      {
        id: "deleted",
        worktreePath: "/workspace/deleted",
        archivedAt: null,
        deletedAt: fixture.now,
      },
      { id: "without-path", worktreePath: null, archivedAt: null, deletedAt: null },
      { id: "malformed", worktreePath: null, archivedAt: null, deletedAt: null },
    ];
    for (const row of rows) {
      const threadId = ThreadId.make(row.id);
      yield* fixture.store.apply({
        id: EventId.make(`event:worktree:${row.id}`),
        type: "thread.created",
        threadId,
        occurredAt: fixture.now,
        payload: { ...fixture.thread, ...row, id: threadId },
      });
    }
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{broken-json' WHERE thread_id = 'malformed'`;
    for (const includeArchived of [false, true]) {
      // Main's unguarded reader is equivalent on valid rows. The new reader deliberately skips malformed JSON.
      const baseline = yield* sql<{ readonly thread_id: string; readonly worktree_path: string }>`
        SELECT thread_id, json_extract(payload_json, '$.worktreePath') AS worktree_path
        FROM orchestration_v2_projection_threads NOT INDEXED
        WHERE deleted_at IS NULL ${includeArchived ? sql`` : sql`AND archived_at IS NULL`}
          AND json_valid(payload_json)
          AND json_extract(payload_json, '$.worktreePath') IS NOT NULL
      `;
      const trace = ReadFixture.traceSql();
      const actual = yield* fixture.store
        .getThreadWorktreePaths({ includeArchived })
        .pipe(Effect.withTracer(trace.tracer));
      expect(actual).toEqual(
        baseline.map((row) => ({ threadId: row.thread_id, worktreePath: row.worktree_path })),
      );
      expect(
        actual.some(
          (row) =>
            row.threadId === "deleted" ||
            row.threadId === "without-path" ||
            row.threadId === "malformed",
        ),
      ).toBe(false);
      expect(actual.some((row) => row.threadId === "a-archived-alias")).toBe(includeArchived);
      if (includeArchived) {
        const query = trace.queries.find(({ query }) => query.includes("worktree_path"))!.query;
        const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
        expect(
          plan.some(({ detail }) =>
            detail.includes("local_projection_threads_worktree_guarded_idx"),
          ),
        ).toBe(true);
      }
    }
  }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

it.effect.each(["all", "empty", "one-run", "two-runs", "missing", "combined-id"] as const)(
  "canonical subagent run cohort equals main's ordered roster for %s",
  (selection) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const sql = yield* SqlClient.SqlClient;
      const first = fixture.run(1).id;
      const second = fixture.run(2).id;
      for (const [index, [name, runId, started, status]] of (
        [
          ["ä", first, false, "completed"],
          ["z", second, false, "running"],
          ["A", first, false, "cancelled"],
          ["runless", null, false, "waiting"],
          ["later", first, true, "failed"],
        ] as const
      ).entries()) {
        const id = NodeId.make(`task:${name}`);
        yield* fixture.store.apply({
          id: EventId.make(`event:cohort:${index}`),
          type: "subagent.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            id,
            threadId: fixture.threadId,
            runId,
            parentNodeId: NodeId.make("parent"),
            origin: "app_owned",
            createdBy: "agent",
            driver: fixture.driver,
            providerInstanceId: fixture.providerInstanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Prompt",
            title: name,
            model: null,
            status,
            result: null,
            startedAt: started ? fixture.now : null,
            completedAt: null,
            updatedAt: fixture.now,
          },
        });
      }
      const broad = yield* fixture.store.getThreadRecords(fixture.threadId, ["subagents"]);
      const runIds =
        selection === "all"
          ? undefined
          : selection === "empty"
            ? []
            : selection === "two-runs"
              ? [second, first]
              : selection === "missing"
                ? [RunId.make("absent")]
                : [first];
      const ids =
        selection === "combined-id" ? [NodeId.make("task:z"), NodeId.make("task:ä")] : undefined;
      const trace = ReadFixture.traceSql();
      const narrow = yield* fixture.store
        .getThreadRecords(fixture.threadId, ["subagents"], {
          ...(runIds === undefined ? {} : { subagentRunIds: runIds }),
          ...(ids === undefined ? {} : { subagentIds: ids }),
        })
        .pipe(Effect.withTracer(trace.tracer));
      expect(narrow.subagents).toEqual(
        broad.subagents.filter(
          (task) =>
            (runIds === undefined || (task.runId !== null && runIds.includes(task.runId))) &&
            (ids === undefined || ids.includes(task.id)),
        ),
      );
      if (selection === "two-runs")
        expect(narrow.subagents.map((task) => task.id)).toEqual([
          "task:A",
          "task:z",
          "task:ä",
          "task:later",
        ]);
      if (runIds !== undefined && runIds.length > 0 && ids === undefined) {
        const query = trace.queries.find(({ query }) =>
          query.includes("FROM orchestration_v2_projection_subagents"),
        )!.query;
        const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
        expect(
          plan.some(({ detail }) =>
            /local_projection_subagents_run_private_idx \(thread_id=\? AND run_id=\?\)/.test(
              detail,
            ),
          ),
        ).toBe(true);
      }
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

it.effect.each(["existing", "missing", "foreign"] as const)(
  "scope-only checkpoint context equals main's scope metadata for %s scope",
  (selection) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const sql = yield* SqlClient.SqlClient;
      const scopeId = CheckpointScopeId.make("scope:stage1:one");
      const foreignScopeId = CheckpointScopeId.make("scope:stage1:foreign");
      for (const [index, id] of [
        scopeId,
        CheckpointScopeId.make("scope:stage1:two"),
        foreignScopeId,
      ].entries()) {
        const threadId =
          id === foreignScopeId ? ThreadId.make("thread:scope:foreign") : fixture.threadId;
        yield* fixture.store.apply({
          id: EventId.make(`event:scope-only:${index}`),
          type: "checkpoint-scope.created",
          threadId,
          occurredAt: fixture.now,
          payload: {
            id,
            threadId,
            runId: fixture.run(1).id,
            nodeId: NodeId.make(`node:scope:${index}`),
            parentScopeId: null,
            providerThreadId: null,
            kind: "root_run",
            ordinalWithinParent: index,
            advancesAppRunCount: true,
            cwd: `/workspace/scope-${index}`,
            createdAt: fixture.now,
          },
        });
      }
      const baseline = yield* fixture.store.getCheckpointContext(fixture.threadId);
      const selectedId =
        selection === "existing"
          ? scopeId
          : selection === "foreign"
            ? foreignScopeId
            : CheckpointScopeId.make("scope:missing");
      const trace = ReadFixture.traceSql();
      const actual = yield* fixture.store
        .getCheckpointContext(fixture.threadId, selectedId)
        .pipe(Effect.withTracer(trace.tracer));
      expect(actual.checkpointScopes).toEqual(
        baseline.checkpointScopes.filter((scope) => scope.id === selectedId),
      );
      expect(actual.runs).toEqual([]);
      expect(actual.checkpoints).toEqual([]);
      expect(
        trace.queries.some(({ query }) =>
          /FROM orchestration_v2_projection_(runs|checkpoints)\b/.test(query),
        ),
      ).toBe(false);
      const query = trace.queries.find(({ query }) =>
        query.includes("FROM orchestration_v2_projection_checkpoint_scopes"),
      )!.query;
      const plan = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`);
      expect(plan.some(({ detail }) => /checkpoint_scopes_1 \(scope_id=\?\)/.test(detail))).toBe(
        true,
      );
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

// julianday of 'now', 'subsec' or 'subsecond' reads the clock, so the usage index leaves such times
// unkeyed: storing one must not fail, and latest usage must still match main's order.
it.effect.each(["dated", "now", "NOW", "subsec", "subsecond"] as const)(
  "latest token usage matches main's order with %s usage times",
  (newest) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const sql = yield* SqlClient.SqlClient;
      const run = fixture.run(1);
      const times = [
        "2026-01-02T03:04:05Z",
        newest === "dated" ? "2026-01-03T00:00:00Z" : newest,
        "2025-12-31T00:00:00Z",
        "not a date",
      ];
      for (const [index, updatedAt] of times.entries()) {
        yield* fixture.putTurn(
          fixture.turn(run, 80 + index, "completed", {
            tokenUsage: { usedTokens: index + 1, updatedAt },
          }),
        );
      }
      const { providerTurns } = yield* fixture.store.getThreadRecords(
        fixture.threadId,
        ["providerTurns"],
        { latestProviderTurnUsageOnly: true },
      );
      const main = yield* sql<{ readonly id: string }>`
        SELECT json_extract(payload_json, '$.id') AS id
        FROM orchestration_v2_projection_provider_turns
        WHERE thread_id = ${fixture.threadId}
          AND json_extract(payload_json, '$.tokenUsage') IS NOT NULL
        ORDER BY julianday(json_extract(payload_json, '$.tokenUsage.updatedAt')) DESC,
          provider_thread_id ASC, ordinal ASC
        LIMIT 1
      `;
      expect(providerTurns.map((turn) => turn.id)).toEqual(main.map((row) => row.id));
      expect(providerTurns.map((turn) => turn.id)).toEqual(["oracle-turn-1-81"]);
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

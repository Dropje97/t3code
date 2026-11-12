import { assert, it } from "@effect/vitest";
import { NodeId, ProviderThreadId, type OrchestrationV2Run } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { layerDatabase, seed, traceSql } from "./SendContinuationReads.testkit.ts";
import type { ProjectionRecordFilter } from "./ProjectionStore.ts";

it.effect(
  "canonical item filters retain broad rows and byte order through run and node seeks",
  () =>
    Effect.gen(function* () {
      const f = yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const runId = f.run(2).id;
      const nodeId = NodeId.make("selected-node");
      for (const [id, owner, status] of [
        ["extra-\u{10000}", runId, "running"],
        ["extra-\ue000", null, "waiting"],
        ["extra-a", runId, "completed"],
      ] as const) {
        yield* f.putItem({
          ...f.item(id, 30, f.run(2).userMessageId, owner, "turn_start"),
          nodeId,
          status,
        });
      }
      const broad = (yield* f.store.getThreadRecords(f.threadId, ["turnItems"])).turnItems;
      const cases: ReadonlyArray<ProjectionRecordFilter> = [
        { turnItemRunIds: [runId] },
        { turnItemRunIds: [runId, null] },
        { turnItemRunIds: [null] },
        { turnItemRunId: runId, turnItemRunIds: [runId, null] },
        { turnItemNodeIds: [nodeId] },
        { turnItemStatuses: ["running", "waiting"] },
        { turnItemNodeIds: [nodeId], turnItemStatuses: ["completed"] },
        { turnItemTypes: ["user_message"] },
        { turnItemTypes: ["command_execution"] },
        { turnItemRunIds: [], turnItemNodeIds: [nodeId] },
      ];
      for (const filter of cases) {
        const expected = broad.filter(
          (item) =>
            (filter.turnItemRunIds === undefined || filter.turnItemRunIds.includes(item.runId)) &&
            (filter.turnItemRunId === undefined || filter.turnItemRunId === item.runId) &&
            (filter.turnItemNodeIds === undefined ||
              (item.nodeId !== null && filter.turnItemNodeIds.includes(item.nodeId))) &&
            (filter.turnItemStatuses === undefined ||
              filter.turnItemStatuses.includes(item.status)) &&
            (filter.turnItemTypes === undefined || filter.turnItemTypes.includes(item.type)),
        );
        const trace = traceSql();
        const actual = yield* f.store
          .getThreadRecords(f.threadId, ["turnItems"], filter)
          .pipe(Effect.withTracer(trace.tracer));
        assert.deepEqual(actual.turnItems, expected);
        const query = trace.queries.find(({ query }) =>
          query.includes("WITH selected AS MATERIALIZED"),
        )?.query;
        if (query === undefined) continue; // Empty selections never query items.
        const plan = (yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
          .map((row) => row.detail)
          .join("\n");
        if (filter.turnItemRunIds !== undefined || filter.turnItemRunId !== undefined) {
          assert.include(plan, "turn_items_thread_run_idx (thread_id=? AND run_id=?)");
          assert.notInclude(plan, "turn_items_thread_run_idx (thread_id=?)");
        } else if (filter.turnItemNodeIds !== undefined) {
          assert.include(plan, "turn_items_node_ordinal_idx (node_id=?)");
        }
      }
    }).pipe(Effect.provide(layerDatabase)),
);

it.effect("status plus latest run and live/attempt turns equal main's broad selections", () =>
  Effect.gen(function* () {
    const f = yield* seed();
    const sql = yield* SqlClient.SqlClient;
    const live = f.run(13, { status: "running", completedAt: null });
    const latest = f.run(14, { status: "failed" });
    yield* f.putRun(live);
    yield* f.putRun(latest);
    yield* f.putRun(f.run(15, { status: "cancelled", startedAt: null }));
    const broad = yield* f.store.getThreadRecords(f.threadId, ["runs", "providerTurns"]);
    const eligibleLatest = broad.runs.findLast(
      (run) => run.status !== "cancelled" || run.startedAt !== null,
    );
    for (const statuses of [[], ["running"], ["running", "failed"], ["completed"]] as const) {
      const selected: ReadonlyArray<OrchestrationV2Run["status"]> = statuses;
      const actual = yield* f.store.getThreadRecords(f.threadId, ["runs"], {
        runStatuses: selected,
        includeLatestRun: true,
      });
      assert.deepEqual(
        actual.runs,
        broad.runs.filter((run) => selected.includes(run.status) || run.id === eligibleLatest?.id),
      );
    }
    for (const [suffix, ordinal] of [
      ["\u{10000}", 1],
      ["\ue000", 1],
      ["a", 2],
    ] as const) {
      yield* f.putTurn(
        f.turn(live, ordinal, "running", {
          providerThreadId: ProviderThreadId.make(`live-${suffix}`),
        }),
      );
    }
    const allTurns = (yield* f.store.getThreadRecords(f.threadId, ["providerTurns"])).providerTurns;
    for (const filter of [
      { runningProviderTurnsOnly: true },
      { providerTurnAttemptIds: [live.activeAttemptId!] },
    ] as const) {
      const trace = traceSql();
      const actual = yield* f.store
        .getThreadRecords(f.threadId, ["providerTurns"], filter)
        .pipe(Effect.withTracer(trace.tracer));
      assert.deepEqual(
        actual.providerTurns,
        allTurns.filter((turn) =>
          "runningProviderTurnsOnly" in filter
            ? turn.status === "running"
            : turn.runAttemptId === live.activeAttemptId,
        ),
      );
      const query = trace.queries.find(({ query }) =>
        query.includes("FROM orchestration_v2_projection_provider_turns"),
      )!.query;
      const plan = (yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${query}`))
        .map((row) => row.detail)
        .join("\n");
      assert.include(
        plan,
        "runningProviderTurnsOnly" in filter
          ? "provider_turns_live_idx (thread_id=? AND status=?)"
          : "provider_turns_attempt_private_idx (thread_id=? AND run_attempt_id=?)",
      );
    }
  }).pipe(Effect.provide(layerDatabase)),
);

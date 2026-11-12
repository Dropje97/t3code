// Local t3-code-nightly patch: eligible sends take the staged reads, the rest the broad read.
// The read-oracle tests check that both give the same transcript.
import { assert, it } from "@effect/vitest";
import { CommandId, EventId, MessageId, type OrchestrationV2Run } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { traceSql } from "./SendContinuationReads.testkit.ts";
import {
  makeOracleLayer,
  makeRunRecords,
  persistOracleEvents,
  recordEvents,
  seedOracle,
} from "./testkit/DispatchReadOracle.ts";

const stagedOff = process.env.T3CODE_LOCAL_STAGED_COMMAND_READS === "off";

const cases: ReadonlyArray<{
  readonly name: string;
  readonly active: OrchestrationV2Run["status"] | null;
  readonly model?: string;
  readonly staged: boolean;
}> = [
  { name: "an idle send", active: null, staged: true },
  { name: "a send behind a running run", active: "running", staged: true },
  { name: "a model change", active: null, model: "gpt-5.5", staged: false },
];

it.effect.each(cases)("$name takes the staged or broad read", ({ active, model, staged }) =>
  Effect.gen(function* () {
    if (stagedOff) return;
    const fixture = yield* seedOracle({ active });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const trace = traceSql();
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("staged-send"),
        threadId: fixture.threadId,
        messageId: MessageId.make("staged-message"),
        text: "Next step",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
        ...(model === undefined
          ? {}
          : { modelSelection: { ...fixture.thread.modelSelection, model } }),
      })
      .pipe(Effect.exit, Effect.withTracer(trace.tracer));
    // The staged read alone seeks the latest user item; the broad read alone finds the
    // last compaction.
    const queries = trace.queries.map((entry) => entry.query);
    assert.equal(
      queries.some((query) => query.includes("local_projection_turn_items_type_private_idx")),
      staged,
    );
    assert.equal(
      queries.some((query) => query.includes("WITH compaction AS")),
      !staged,
    );
  }).pipe(Effect.provide(makeOracleLayer())),
);

// A watch that ended with a final notice still owes its earlier wakes; only the broad read holds
// that notice, so the staged queued start falls back.
it.effect("keeps a queued PR wake owed after its final notice has already run", () =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle({ history: 2, active: "completed" });
    const { threadId, now } = fixture;
    const stamp = {
      host: "github.com",
      repository: "owner/repo",
      number: 7,
      watchStartedAt: DateTime.formatIso(DateTime.subtract(now, { hours: 1 })),
      headSha: "abc123",
    };
    const watch = {
      startedAt: stamp.watchStartedAt,
      headSha: stamp.headSha,
      wakes: 2,
      passed: false,
      failedChecks: [],
      passedChecks: [],
      remarksThrough: stamp.watchStartedAt,
      remarkIds: [],
      conflicting: false,
    };
    const queued = makeRunRecords(threadId, 20, "queued", now, fixture.providerThread.id);
    yield* persistOracleEvents([
      {
        id: EventId.make("review:ended-watch"),
        type: "thread.metadata-updated",
        threadId,
        occurredAt: now,
        payload: {
          ...fixture.thread,
          pullRequests: [
            {
              host: stamp.host,
              repository: stamp.repository,
              number: stamp.number,
              url: "https://github.com/owner/repo/pull/7",
              source: "agent",
              linkedAt: stamp.watchStartedAt,
              snapshot: null,
              stack: null,
              endedWatch: watch,
            },
          ],
        },
      },
      {
        id: EventId.make("review:historical-ending-notice"),
        type: "message.updated",
        threadId,
        occurredAt: now,
        payload: {
          ...fixture.message,
          createdBy: "agent",
          creationSource: "server",
          notification: {
            source: { kind: "monitor", pullRequest: { ...stamp, watchEnded: true } },
            outcome: "updated",
            summary: "Watch exhausted",
          },
        },
      },
      ...recordEvents(
        {
          ...queued,
          root: { ...queued.root, status: "pending", providerTurnId: null },
          attempt: { ...queued.attempt, status: "pending", providerTurnId: null },
          message: {
            ...queued.message,
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "monitor", pullRequest: stamp },
              outcome: "updated",
              summary: "Earlier comment",
            },
          },
          item: { ...queued.item, status: "pending" },
        },
        now,
      ).filter((event) => event.type !== "provider-turn.updated"),
    ]);
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.resumeQueuedRuns;
    const after = yield* store.getThreadRecords(threadId, ["runs"]);
    assert.equal(after.runs.find((run) => run.id === queued.run.id)?.status, "starting");
  }).pipe(Effect.provide(makeOracleLayer())),
);

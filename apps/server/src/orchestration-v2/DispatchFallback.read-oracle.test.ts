import { assert, expect, it, vi } from "@effect/vitest";
import {
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2Command,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  PlanId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import {
  makeOracleLayer,
  makeRunRecords,
  otherInstanceId,
  persistOracleEvents,
  recordEvents,
  recordOracle,
  seedOracle,
  startOracleCapture,
} from "./testkit/DispatchReadOracle.ts";

type Fixture = Effect.Success<ReturnType<typeof seedOracle>>;
type Dispatch = Extract<OrchestrationV2Command, { type: "message.dispatch" }>;

const dispatch = (fixture: Fixture, fields: Partial<Dispatch> = {}): Dispatch => ({
  type: "message.dispatch",
  commandId: CommandId.make("oracle:fallback:dispatch"),
  threadId: fixture.threadId,
  messageId: MessageId.make("oracle:fallback:message"),
  text: "Continue the work with the complete saved context.",
  attachments: [],
  dispatchMode: { type: "start_immediately" },
  createdBy: "agent",
  creationSource: "server",
  ...fields,
});

const updateRun = Effect.fn("oracle.fallback.updateRun")(function* (
  fixture: Fixture,
  changes: Partial<OrchestrationV2Run>,
) {
  yield* persistOracleEvents([
    {
      id: EventId.make("oracle:fallback:run-update"),
      type: "run.updated",
      threadId: fixture.threadId,
      occurredAt: fixture.now,
      payload: { ...fixture.run, ...changes },
    },
  ]);
});

const updateThread = Effect.fn("oracle.fallback.updateThread")(function* (
  fixture: Fixture,
  changes: Partial<Fixture["thread"]>,
) {
  yield* persistOracleEvents([
    {
      id: EventId.make("oracle:fallback:thread-update"),
      type: "thread.metadata-updated",
      threadId: fixture.threadId,
      occurredAt: fixture.now,
      payload: { ...fixture.thread, ...changes },
    },
  ]);
});

const pendingRequest = Effect.fn("oracle.fallback.pendingRequest")(function* (fixture: Fixture) {
  yield* persistOracleEvents([
    {
      id: EventId.make("oracle:fallback:request"),
      type: "runtime-request.updated",
      threadId: fixture.threadId,
      occurredAt: fixture.now,
      payload: {
        id: RuntimeRequestId.make("oracle:fallback:request"),
        nodeId: fixture.root.id,
        providerTurnId: fixture.turn.id,
        nativeRequestRef: null,
        kind: "command",
        status: "pending",
        responseCapability: { type: "live", providerSessionId: fixture.session.id },
        createdAt: fixture.now,
        resolvedAt: null,
      },
    },
  ]);
});

const usageFailure = (fixture: Fixture, resetAt: string): OrchestrationV2TurnItem => ({
  id: TurnItemId.make("oracle:fallback:usage-error"),
  type: "error",
  threadId: fixture.threadId,
  runId: fixture.run.id,
  nodeId: fixture.root.id,
  providerThreadId: fixture.providerThread.id,
  providerTurnId: fixture.turn.id,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 901,
  status: "failed",
  title: "Usage limit reached",
  startedAt: fixture.now,
  completedAt: fixture.now,
  updatedAt: fixture.now,
  failure: {
    class: "usage_limit",
    message: "The saved account allowance is exhausted.",
    code: "usageLimitExceeded",
    retryable: null,
    resetAt,
  },
});

const saveUsageFailure = Effect.fn("oracle.fallback.saveUsageFailure")(function* (
  fixture: Fixture,
  resetAt: string,
) {
  yield* persistOracleEvents([
    {
      id: EventId.make("oracle:fallback:usage-error"),
      type: "turn-item.updated",
      threadId: fixture.threadId,
      occurredAt: fixture.now,
      payload: usageFailure(fixture, resetAt),
    },
  ]);
});

const manualCases = [
  "interrupted",
  "usage-limit",
  "failed-not-usage-limit",
  "higher-ordinal-completed-earlier",
  "lower-ordinal-completed-later",
  "wrong-mode",
  "missing-source",
  "completed-source",
  "not-latest-executed",
  "archived",
  "deleted",
  "pending-request",
] as const;

it.effect.each(manualCases)("fallback matrix 5: manual continuation %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* updateRun(fixture, {
      status:
        testCase === "completed-source"
          ? "completed"
          : testCase === "usage-limit" || testCase === "failed-not-usage-limit"
            ? "failed"
            : "interrupted",
      completedAt: fixture.now,
    });
    if (testCase === "usage-limit")
      yield* saveUsageFailure(
        fixture,
        DateTime.formatIso(DateTime.subtract(fixture.now, { minutes: 1 })),
      );
    if (testCase === "higher-ordinal-completed-earlier") {
      const olderEnd = makeRunRecords(
        fixture.threadId,
        99,
        "completed",
        fixture.now,
        fixture.providerThread.id,
      );
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:older-end"),
          type: "run.created",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: { ...olderEnd.run, completedAt: DateTime.subtract(fixture.now, { minutes: 1 }) },
        },
      ]);
    }
    if (testCase === "lower-ordinal-completed-later") {
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:later-end"),
          type: "run.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.history[0]!.run,
            completedAt: DateTime.add(fixture.now, { minutes: 1 }),
          },
        },
      ]);
    }
    if (testCase === "not-latest-executed") {
      const newer = makeRunRecords(
        fixture.threadId,
        99,
        "completed",
        DateTime.add(fixture.now, { seconds: 1 }),
        fixture.providerThread.id,
      );
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:newer"),
          type: "run.created",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: newer.run,
        },
      ]);
    }
    if (testCase === "archived") yield* updateThread(fixture, { archivedAt: fixture.now });
    if (testCase === "deleted") yield* updateThread(fixture, { deletedAt: fixture.now });
    if (testCase === "pending-request") yield* pendingRequest(fixture);
    expect(
      yield* recordOracle(
        orchestrator.dispatch(
          dispatch(fixture, {
            manualContinuationOfRunId:
              testCase === "missing-source" ? RunId.make("missing:source") : fixture.run.id,
            ...(testCase === "wrong-mode" ? { dispatchMode: { type: "queue_after_active" } } : {}),
          }),
        ),
      ),
    ).toMatchSnapshot();
  }).pipe(Effect.provide(makeOracleLayer())),
);

const forkCases = [
  "native",
  "portable-provider-change",
  "source-busy-after-fork",
  "weak-native-thread",
  "missing-native-thread",
  "weak-native-turn",
  "missing-native-turn-attempt-fallback",
  "side-chat-with-source",
  "side-chat-without-source",
  "side-chat-inherited-and-imported",
] as const;

it.effect.each(forkCases)("fallback matrix 7: pending fork %s", (testCase) =>
  Effect.gen(function* () {
    const noSource = testCase === "side-chat-without-source";
    const inherited = testCase === "side-chat-inherited-and-imported";
    const fixture = yield* seedOracle({
      history: noSource ? 0 : 4,
      active: noSource ? "running" : "completed",
    });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const targetThreadId = ThreadId.make("oracle:fork-target");
    let sourceThreadId = fixture.threadId;
    if (testCase === "weak-native-thread" || testCase === "missing-native-thread") {
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fork-source-ref"),
          type: "provider-thread.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.providerThread,
            nativeThreadRef:
              testCase === "missing-native-thread"
                ? null
                : { ...fixture.providerThread.nativeThreadRef, strength: "weak" },
          },
        },
      ]);
    }
    if (testCase === "weak-native-turn" || testCase === "missing-native-turn-attempt-fallback") {
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fork-source-turn-ref"),
          type: "provider-turn.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.turn,
            nativeTurnRef:
              testCase === "weak-native-turn"
                ? { ...fixture.turn.nativeTurnRef!, strength: "weak" }
                : null,
          },
        },
      ]);
    }
    if (inherited) {
      sourceThreadId = ThreadId.make("oracle:fork-imported-parent");
      yield* orchestrator.dispatch({
        type: "thread.fork",
        commandId: CommandId.make("oracle:fork-grandparent"),
        sourceThreadId: fixture.threadId,
        targetThreadId: sourceThreadId,
        sourcePoint: { type: "run", runId: fixture.run.id },
        createdBy: "user",
        creationSource: "web",
      });
      const parentThread = yield* store.getThread(sourceThreadId);
      const importedItem = {
        ...fixture.item,
        id: TurnItemId.make("oracle:imported-parent-item"),
        threadId: sourceThreadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        messageId: MessageId.make("oracle:imported-parent-message"),
        ordinal: 1,
        text: "IMPORTED_PARENT_HISTORY_MARKER",
      };
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:imported-parent"),
          type: "thread.metadata-updated",
          threadId: sourceThreadId,
          occurredAt: fixture.now,
          payload: { ...parentThread, historyOrigin: "v1_import" },
        },
        {
          id: EventId.make("oracle:imported-parent-item"),
          type: "turn-item.updated",
          threadId: sourceThreadId,
          occurredAt: fixture.now,
          payload: importedItem,
        },
      ]);
    }
    const sideChat = testCase.startsWith("side-chat");
    yield* orchestrator.dispatch({
      type: "thread.fork",
      commandId: CommandId.make("oracle:fork"),
      sourceThreadId,
      targetThreadId,
      sourcePoint:
        noSource || inherited ? { type: "latest_stable" } : { type: "run", runId: fixture.run.id },
      ...(sideChat ? { sideChat: true } : {}),
      createdBy: "user",
      creationSource: "web",
    });
    if (testCase === "source-busy-after-fork" || testCase === "side-chat-with-source") {
      const after = makeRunRecords(
        sourceThreadId,
        99,
        "running",
        fixture.now,
        fixture.providerThread.id,
      );
      yield* persistOracleEvents(
        recordEvents(
          {
            ...after,
            message: { ...after.message, text: "POST_FORK_ACTIVE_HISTORY_MARKER" },
            item: { ...after.item, text: "POST_FORK_ACTIVE_HISTORY_MARKER" },
          },
          fixture.now,
        ),
      );
    }
    const command = dispatch(fixture, {
      threadId: targetThreadId,
      createdBy: "user",
      creationSource: "web",
      ...(testCase === "portable-provider-change"
        ? { modelSelection: { instanceId: otherInstanceId, model: "gpt-5.4" } }
        : {}),
    });
    const transcript = yield* recordOracle(orchestrator.dispatch(command));
    expect(transcript).toMatchSnapshot();
    const target = yield* store.getThreadProjection(targetThreadId);
    if (testCase === "native")
      assert.isTrue(
        target.providerThreads.some(
          (thread) => thread.forkedFrom?.providerThreadId === fixture.providerThread.id,
        ),
      );
    if (
      testCase === "portable-provider-change" ||
      testCase === "side-chat-with-source" ||
      noSource ||
      inherited
    )
      assert.isNotEmpty(target.contextHandoffs);
  }).pipe(Effect.provide(makeOracleLayer())),
);

const mergeCases = [
  "single",
  "same-time-tie",
  "different-times",
  "multiple-sources",
  "provider-switch",
  "missing-source-run",
  "missing-source-provider",
] as const;

it.effect.each(mergeCases)("fallback matrix 7: pending merge-back %s", (testCase) =>
  Effect.gen(function* () {
    const target = yield* seedOracle();
    const source = yield* seedOracle({ threadId: ThreadId.make("oracle:merge-source") });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const transfer = {
      id: ContextTransferId.make("oracle:merge:a"),
      type: "merge_back" as const,
      sourceThreadId: source.threadId,
      targetThreadId: target.threadId,
      sourcePoint: {
        threadId: source.threadId,
        runId: testCase === "missing-source-run" ? RunId.make("missing:source") : source.run.id,
        providerThreadId: source.providerThread.id,
        providerTurnId: source.turn.id,
        runOrdinal: source.run.ordinal,
      },
      basePoint: null,
      sourceProviderInstanceId: source.run.providerInstanceId,
      targetProviderInstanceId: null,
      targetRunId: null,
      status: "pending" as const,
      resolution: null,
      createdBy: "user" as const,
      error: null,
      createdAt: target.now,
      updatedAt: target.now,
      consumedAt: null,
    };
    const transfers: OrchestrationV2DomainEvent[] = [
      {
        id: EventId.make("oracle:merge:a"),
        type: "context-transfer.created",
        threadId: target.threadId,
        occurredAt: target.now,
        payload: transfer,
      },
    ];
    if (
      testCase === "same-time-tie" ||
      testCase === "different-times" ||
      testCase === "multiple-sources"
    )
      transfers.push({
        id: EventId.make("oracle:merge:z"),
        type: "context-transfer.created",
        threadId: target.threadId,
        occurredAt: target.now,
        payload: {
          ...transfer,
          id: ContextTransferId.make("oracle:merge:z"),
          ...(testCase === "different-times"
            ? { updatedAt: DateTime.add(target.now, { seconds: 1 }) }
            : {}),
          ...(testCase === "multiple-sources"
            ? { sourceThreadId: ThreadId.make("oracle:other-source") }
            : {}),
        },
      });
    yield* persistOracleEvents(transfers);
    if (testCase === "missing-source-provider") {
      yield* updateRun(source, { providerThreadId: null });
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:merge:no-provider"),
          type: "thread.metadata-updated",
          threadId: source.threadId,
          occurredAt: source.now,
          payload: { ...source.thread, activeProviderThreadId: null },
        },
      ]);
    }
    expect(
      yield* recordOracle(
        orchestrator.dispatch(
          dispatch(target, {
            createdBy: "user",
            creationSource: "web",
            ...(testCase === "provider-switch"
              ? { modelSelection: { instanceId: otherInstanceId, model: "gpt-5.4" } }
              : {}),
          }),
        ),
      ),
    ).toMatchSnapshot();
  }).pipe(Effect.provide(makeOracleLayer())),
);

it.effect.each(["immediate", "deferred"] as const)(
  "fallback matrix 10: delegated request with pending child creation %s",
  (mode) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ active: "running" });
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const readSpy = vi.spyOn(store, "getThreadRecords");
      yield* Effect.gen(function* () {
        const transcript = yield* recordOracle(
          orchestrator.dispatch({
            type: "delegated_task.request",
            commandId: CommandId.make("oracle:delegated-request"),
            parentThreadId: fixture.threadId,
            parentRunId: fixture.run.id,
            parentNodeId: fixture.root.id,
            task: "Inspect all historical records before producing a regression oracle.",
            title: "Bounded regression review",
            modelSelection: fixture.thread.modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            completionWake: "always",
            createdBy: "agent",
            creationSource: "provider",
            ...(mode === "deferred"
              ? { workspaceStrategy: { type: "worktree", baseRef: "main" } }
              : {}),
          }),
        );
        assert.isTrue(
          readSpy.mock.calls.some(
            ([threadId, fields]) =>
              threadId === "thread:delegated-task:oracle%3Adelegated-request" &&
              fields.includes("contextTransfers") &&
              fields.includes("attempts") &&
              fields.includes("messages"),
          ),
        );
        assert.isFalse(Object.hasOwn(transcript.at(-1), "error"));
        expect(transcript).toMatchSnapshot();
      }).pipe(Effect.ensuring(Effect.sync(() => readSpy.mockRestore())));
    }).pipe(Effect.provide(makeOracleLayer())),
);

const switchCases = [
  "handoff",
  "model-change",
  "native-account-resume",
  "native-resume-background-busy",
  "handoff-background-busy",
  "handoff-probe-failed",
] as const;

it.effect.each(switchCases)("fallback matrix 7: provider transition %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
    const capture = yield* startOracleCapture();
    const busy = testCase.endsWith("background-busy") || testCase === "handoff-probe-failed";
    const nativeResume = testCase.startsWith("native-");
    if (nativeResume)
      Object.defineProperty(registry, "getMetadata", {
        configurable: true,
        value: (id: ProviderInstanceId) =>
          Effect.sync(() => {
            capture.transcript.push({ call: "adapter.metadata", args: [id] });
            return {
              driver: fixture.session.driver,
              continuationKey: "shared-native-account-family",
              enabled: true,
              capabilities: fixture.session.capabilities,
            };
          }),
      });
    const runtime = {
      providerSession: fixture.session,
      hasPendingBackgroundWorkForThread: (thread: Fixture["providerThread"]) =>
        Effect.sync(() => {
          capture.transcript.push({
            call: "runtime.hasPendingBackgroundWorkForThread",
            args: [thread],
          });
        }).pipe(
          Effect.andThen(
            testCase === "handoff-probe-failed"
              ? Effect.die("deterministic runtime probe failure")
              : Effect.succeed(busy),
          ),
        ),
    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const getSpy = vi.spyOn(sessions, "get").mockImplementation((id) =>
      Effect.sync(() => {
        capture.transcript.push({ call: "runtime.get", args: [id] });
        return Option.some(runtime);
      }),
    );
    yield* Effect.gen(function* () {
      const result = yield* orchestrator
        .dispatch(
          dispatch(fixture, {
            createdBy: "user",
            creationSource: "web",
            modelSelection: {
              instanceId:
                testCase === "model-change" ? fixture.run.providerInstanceId : otherInstanceId,
              model: testCase === "model-change" ? "gpt-6.1" : fixture.run.modelSelection.model,
            },
          }),
        )
        .pipe(Effect.result);
      capture.transcript.push(
        Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
      );
      expect(capture.snapshot()).toMatchSnapshot();
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          getSpy.mockRestore();
          if (nativeResume) Reflect.deleteProperty(registry, "getMetadata");
        }),
      ),
      Effect.ensuring(capture.stop),
    );
  }).pipe(Effect.provide(makeOracleLayer())),
);

const delegatedCases = [
  "settled-only-with-sibling",
  "always-live-steer",
  "mixed-live-wake-policies",
  "duplicate-mailbox-steer",
  "already-consumed",
  "missing-owner",
  "closed-owner",
  "missing-delivery",
  "generation-mismatch",
  "message-mismatch",
  "wrong-creator",
  "wrong-source",
  "wrong-mode",
  "command-task-ids-do-not-replace-cohort",
] as const;

it.effect.each(delegatedCases)("fallback matrix 5: delegated completion %s", (testCase) =>
  Effect.gen(function* () {
    const live = testCase === "always-live-steer" || testCase === "mixed-live-wake-policies";
    const fixture = yield* seedOracle({ active: live ? "running" : "completed" });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const taskId = NodeId.make("oracle:delegated:task");
    const siblingId = NodeId.make("oracle:delegated:sibling");
    const messageId = MessageId.make("oracle:delegated:delivery");
    const cohortTaskIds =
      testCase === "settled-only-with-sibling" || testCase === "mixed-live-wake-policies"
        ? [taskId, siblingId]
        : [taskId];
    yield* updateRun(fixture, {
      ...(testCase === "already-consumed" ? { userMessageId: messageId } : {}),
      delegatedCompletion: {
        disposition: testCase === "closed-owner" ? "stopped" : "open",
        nextGeneration: 2,
        delivery:
          testCase === "missing-delivery"
            ? null
            : {
                generation: 1,
                messageId:
                  testCase === "message-mismatch" ? MessageId.make("other:delivery") : messageId,
                taskIds: cohortTaskIds,
              },
      },
    });
    const task = {
      id: taskId,
      threadId: fixture.threadId,
      runId: fixture.run.id,
      parentNodeId: fixture.root.id,
      origin: "app_owned" as const,
      createdBy: "agent" as const,
      driver: fixture.session.driver,
      providerInstanceId: fixture.run.providerInstanceId,
      providerThreadId: null,
      childThreadId: null,
      nativeTaskRef: null,
      prompt: "Inspect the historical branch.",
      title: "Completed bounded task",
      model: fixture.run.modelSelection.model,
      completionWake:
        live || testCase === "duplicate-mailbox-steer"
          ? ("always" as const)
          : ("settled_only" as const),
      completionDelivery: { state: "claimed" as const, observedByRunId: null },
      status: "completed" as const,
      result: "HISTORICAL_TASK_RESULT_MARKER",
      startedAt: DateTime.subtract(fixture.now, { seconds: 30 }),
      completedAt: fixture.now,
      updatedAt: fixture.now,
    };
    yield* persistOracleEvents([
      {
        id: EventId.make("oracle:delegated:task"),
        type: "subagent.updated",
        threadId: fixture.threadId,
        occurredAt: fixture.now,
        payload: task,
      },
      {
        id: EventId.make("oracle:delegated:sibling"),
        type: "subagent.updated",
        threadId: fixture.threadId,
        occurredAt: fixture.now,
        payload: {
          ...task,
          id: siblingId,
          completionWake: "settled_only",
          completionDelivery: { state: "pending", observedByRunId: null },
          result: "SETTLED_SIBLING_RESULT_MARKER",
        },
      },
      ...(testCase === "settled-only-with-sibling"
        ? [
            {
              id: EventId.make("oracle:delegated:settled-sibling"),
              type: "subagent.updated" as const,
              threadId: fixture.threadId,
              occurredAt: fixture.now,
              payload: {
                ...task,
                id: NodeId.make("oracle:delegated:settled-sibling"),
                completionDelivery: {
                  state: "acknowledged" as const,
                  observedByRunId: fixture.run.id,
                },
                result: "OLDER_ACKNOWLEDGED_SIBLING_MARKER",
              },
            },
          ]
        : []),
    ]);
    if (testCase === "duplicate-mailbox-steer" || testCase === "already-consumed") {
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:delegated:existing-message"),
          type: "message.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.message,
            id: messageId,
            text: "Previously accepted mailbox steering message",
            createdBy: "agent",
            creationSource: "server",
            delegatedCompletion: { parentRunId: fixture.run.id, generation: 1, taskIds: [taskId] },
          },
        },
      ]);
    }
    if (testCase === "duplicate-mailbox-steer") {
      const active = makeRunRecords(
        fixture.threadId,
        99,
        "running",
        fixture.now,
        fixture.providerThread.id,
      );
      yield* persistOracleEvents(recordEvents(active, fixture.now));
    }
    const capture = yield* startOracleCapture();
    const runtime = {
      providerSession: fixture.session,
    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const getSpy = vi.spyOn(sessions, "get").mockImplementation((id) =>
      Effect.sync(() => {
        capture.transcript.push({ call: "runtime.get", args: [id] });
        return Option.some(runtime);
      }),
    );
    yield* Effect.gen(function* () {
      const result = yield* orchestrator
        .dispatch(
          dispatch(fixture, {
            messageId,
            dispatchMode:
              testCase === "wrong-mode"
                ? { type: "start_immediately" }
                : { type: "queue_after_active" },
            createdBy: testCase === "wrong-creator" ? "user" : "agent",
            creationSource: testCase === "wrong-source" ? "provider" : "server",
            delegatedCompletion: {
              parentRunId:
                testCase === "missing-owner" ? RunId.make("missing:owner") : fixture.run.id,
              generation: testCase === "generation-mismatch" ? 2 : 1,
              taskIds:
                testCase === "command-task-ids-do-not-replace-cohort"
                  ? [NodeId.make("untrusted:command-task")]
                  : cohortTaskIds,
            },
          }),
        )
        .pipe(Effect.result);
      capture.transcript.push(
        Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
      );
      expect(capture.snapshot()).toMatchSnapshot();
    }).pipe(
      Effect.ensuring(Effect.sync(() => getSpy.mockRestore())),
      Effect.ensuring(capture.stop),
    );
  }).pipe(Effect.provide(makeOracleLayer())),
);

const usageCases = [
  "valid",
  "missing-source",
  "no-error",
  "not-shell-error",
  "session-error-hides-usage-limit",
  "recovery-absent",
  "recovery-disabled",
  "request-mismatch",
  "run-mismatch",
  "reset-mismatch",
  "future-reset",
  "archived",
  "deleted",
  "settled",
  "provider-mismatch",
  "pending-request",
  "snoozed",
  "cancelled-unstarted-higher-ordinal",
] as const;

it.effect.each(usageCases)("fallback matrix 5: usage-limit continuation %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const resetAt = DateTime.formatIso(
      testCase === "future-reset"
        ? DateTime.add(fixture.now, { minutes: 1 })
        : DateTime.subtract(fixture.now, { minutes: 1 }),
    );
    const requestId = CommandId.make("oracle:fallback:recovery-request");
    yield* updateRun(fixture, { status: "failed", completedAt: fixture.now });
    if (testCase !== "no-error") yield* saveUsageFailure(fixture, resetAt);
    yield* updateThread(fixture, {
      limitRecovery:
        testCase === "recovery-absent"
          ? null
          : {
              requestId,
              runId: testCase === "run-mismatch" ? RunId.make("different:run") : fixture.run.id,
              resetAt:
                testCase === "reset-mismatch"
                  ? DateTime.formatIso(DateTime.subtract(fixture.now, { hours: 1 }))
                  : resetAt,
              autoResume: testCase !== "recovery-disabled",
              snooze: false,
            },
      ...(testCase === "archived" ? { archivedAt: fixture.now } : {}),
      ...(testCase === "deleted" ? { deletedAt: fixture.now } : {}),
      ...(testCase === "settled" ? { settledOverride: "settled" as const } : {}),
      ...(testCase === "provider-mismatch"
        ? { providerInstanceId: ProviderInstanceId.make("different-provider") }
        : {}),
      ...(testCase === "snoozed"
        ? { snoozedUntil: DateTime.add(fixture.now, { hours: 1 }), snoozedAt: fixture.now }
        : {}),
    });
    if (testCase === "pending-request") yield* pendingRequest(fixture);
    if (testCase === "session-error-hides-usage-limit")
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:session-error"),
          type: "provider-session.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: { ...fixture.session, lastError: "A later session error hides the old limit." },
        },
      ]);
    if (testCase === "not-shell-error") {
      const failure = usageFailure(fixture, resetAt);
      assert(failure.type === "error");
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:newer-error"),
          type: "turn-item.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...failure,
            id: TurnItemId.make("oracle:fallback:newer-error"),
            ordinal: 902,
            updatedAt: DateTime.add(fixture.now, { seconds: 1 }),
            failure: { ...failure.failure, class: "unknown", message: "A newer different error." },
          },
        },
      ]);
    }
    if (testCase === "cancelled-unstarted-higher-ordinal") {
      const newer = makeRunRecords(
        fixture.threadId,
        99,
        "cancelled",
        DateTime.add(fixture.now, { seconds: 1 }),
        fixture.providerThread.id,
      );
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:cancelled-unstarted"),
          type: "run.created",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...newer.run,
            startedAt: null,
            completedAt: DateTime.add(fixture.now, { seconds: 1 }),
          },
        },
      ]);
    }
    expect(
      yield* recordOracle(
        orchestrator.dispatch(
          dispatch(fixture, {
            usageLimitContinuationOfRunId:
              testCase === "missing-source" ? RunId.make("missing:source") : fixture.run.id,
            usageLimitRecoveryRequestId:
              testCase === "request-mismatch" ? CommandId.make("different:request") : requestId,
          }),
        ),
      ),
    ).toMatchSnapshot();
  }).pipe(Effect.provide(makeOracleLayer())),
);

const restartCases = [
  "cancelled",
  "completed-background",
  "waiting-background",
  "failed-before-prompt",
  "missing-source",
  "completed-without-background",
  "archived",
  "deleted",
  "provider-mismatch",
  "newer-run",
  "cancelled-unstarted-does-not-supersede",
  "queued-does-not-supersede",
  "restart-chain",
  "stopped",
] as const;

it.effect.each(restartCases)("fallback matrix 5: restart continuation %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* updateRun(fixture, {
      status:
        testCase === "completed-background" || testCase === "completed-without-background"
          ? "completed"
          : testCase === "waiting-background"
            ? "waiting"
            : testCase === "failed-before-prompt"
              ? "failed"
              : "cancelled",
      completedAt: fixture.now,
      ...(testCase.includes("background") && testCase !== "completed-without-background"
        ? {
            restartCancelledBackgroundWork: [
              { kind: "shell" as const, label: "awaited saved build", awaited: true },
            ],
          }
        : {}),
      ...(testCase === "failed-before-prompt" || testCase === "restart-chain"
        ? {
            restartContinuationOfRunId: fixture.history.at(-1)!.run.id,
            activeAttemptId: null,
            restartCancelledBackgroundWork: [
              { kind: "shell" as const, label: "saved chain command", awaited: true },
            ],
          }
        : {}),
    });
    if (testCase === "archived") yield* updateThread(fixture, { archivedAt: fixture.now });
    if (testCase === "deleted") yield* updateThread(fixture, { deletedAt: fixture.now });
    if (testCase === "provider-mismatch")
      yield* updateThread(fixture, {
        providerInstanceId: ProviderInstanceId.make("different-provider"),
      });
    if (testCase === "stopped")
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:stop-item"),
          type: "turn-item.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.item,
            id: TurnItemId.make("oracle:fallback:stop-item"),
            ordinal: 903,
            type: "run_interrupt_request",
            message: "The owner stopped this continuation.",
          },
        },
      ]);
    if (
      testCase === "newer-run" ||
      testCase === "cancelled-unstarted-does-not-supersede" ||
      testCase === "queued-does-not-supersede"
    ) {
      const newer = makeRunRecords(
        fixture.threadId,
        99,
        testCase === "newer-run"
          ? "completed"
          : testCase === "queued-does-not-supersede"
            ? "queued"
            : "cancelled",
        DateTime.add(fixture.now, { seconds: 1 }),
        fixture.providerThread.id,
      );
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:fallback:newer-run"),
          type: "run.created",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...newer.run,
            ...(testCase === "cancelled-unstarted-does-not-supersede" ? { startedAt: null } : {}),
          },
        },
      ]);
    }
    expect(
      yield* recordOracle(
        orchestrator.dispatch(
          dispatch(fixture, {
            restartContinuationOfRunId:
              testCase === "missing-source" ? RunId.make("missing:source") : fixture.run.id,
          }),
        ),
      ),
    ).toMatchSnapshot();
  }).pipe(Effect.provide(makeOracleLayer())),
);

const planCases = [
  "active",
  "absent",
  "wrong-kind",
  "completed",
  "foreign-project",
  "same-thread-pending-metadata",
] as const;

it.effect.each(planCases)("fallback matrix 6: source plan %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle();
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sourceThreadId =
      testCase === "same-thread-pending-metadata"
        ? fixture.threadId
        : ThreadId.make("oracle:plan-source");
    const planId = PlanId.make("oracle:source-plan");
    if (sourceThreadId !== fixture.threadId) {
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("oracle:create-plan-source"),
        threadId: sourceThreadId,
        projectId:
          testCase === "foreign-project" ? ProjectId.make("other:project") : fixture.projectId,
        title: "Source plan thread",
        modelSelection: fixture.thread.modelSelection,
        runtimeMode: "full-access",
        interactionMode: "plan",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
    } else {
      yield* updateThread(fixture, {
        lastVisitedAt: null,
        settledOverride: "settled",
        settledAt: fixture.now,
      });
    }
    if (testCase !== "absent")
      yield* persistOracleEvents([
        {
          id: EventId.make("oracle:source-plan"),
          type: "plan.updated",
          threadId: sourceThreadId,
          occurredAt: fixture.now,
          payload: {
            id: planId,
            threadId: sourceThreadId,
            runId: null,
            nodeId: NodeId.make("oracle:plan-node"),
            status: testCase === "completed" ? "completed" : "active",
            ...(testCase === "wrong-kind"
              ? { kind: "todo_list", steps: [] }
              : {
                  kind: "proposed_plan",
                  markdown: "# Concrete source plan\n\n1. Preserve every historical field.",
                }),
          },
        },
      ]);
    expect(
      yield* recordOracle(
        orchestrator.dispatch(
          dispatch(fixture, {
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "defer_start" },
            sourcePlanRef: { threadId: sourceThreadId, planId },
          }),
        ),
      ),
    ).toMatchSnapshot();
    if (testCase === "active" || testCase === "same-thread-pending-metadata") {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal((yield* store.getPlan(sourceThreadId, planId))?.status, "completed");
    }
  }).pipe(Effect.provide(makeOracleLayer())),
);

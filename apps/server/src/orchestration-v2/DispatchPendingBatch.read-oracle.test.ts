import { assert, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  ContextTransferId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2DomainEvent,
  RuntimeRequestId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";

import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import {
  makeOracleLayer,
  persistOracleEvents,
  seedOracle,
  startOracleCapture,
} from "./testkit/DispatchReadOracle.ts";

vi.mock("effect/Ref", async (importOriginal) => {
  const original = await importOriginal<typeof import("effect/Ref")>();
  return { ...original, update: vi.fn(original.update) };
});

// The private command event Ref has no public arbitrary-batch entry; append at its exact response marker.
const injectAtResolvedRequest = (
  threadId: ThreadId,
  requestId: RuntimeRequestId,
  batch: readonly OrchestrationV2DomainEvent[],
  record: () => void,
) => {
  const original = vi.mocked(Ref.update).getMockImplementation()!;
  let injected = 0;
  const update = (self: Ref.Ref<unknown>, modify: (value: unknown) => unknown) =>
    original(self, (value) => {
      const next = modify(value);
      const marker =
        Array.isArray(next) &&
        next.some((candidate: unknown) => {
          if (typeof candidate !== "object" || candidate === null) return false;
          const event = candidate as Partial<OrchestrationV2DomainEvent>;
          return (
            event.type === "runtime-request.updated" &&
            event.threadId === threadId &&
            event.payload?.id === requestId &&
            event.payload.status === "resolved"
          );
        });
      if (injected !== 0 || !marker || !Array.isArray(next)) return next;
      injected++;
      record();
      return [...next, ...batch];
    });
  const replacement = ((...args: unknown[]) =>
    args.length === 1
      ? (self: Ref.Ref<unknown>) => update(self, args[0] as (value: unknown) => unknown)
      : update(
          args[0] as Ref.Ref<unknown>,
          args[1] as (value: unknown) => unknown,
        )) as typeof Ref.update;
  vi.mocked(Ref.update).mockImplementation(replacement);
  return {
    count: () => injected,
    restore: () => vi.mocked(Ref.update).mockImplementation(original),
  };
};

const cases = [
  "full-steer",
  "copied-restart",
  "completed-turn",
  "assistant-maintenance",
  "target-merge",
  "other-thread",
] as const;

it.effect.each(cases)(
  "pending matrix 10: original reducer over injected event collection: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ active: "running" });
      const target =
        testCase === "other-thread"
          ? yield* seedOracle({
              threadId: ThreadId.make("pending-batch-other-thread"),
              active: "running",
            })
          : fixture;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const adapters = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const requestId = RuntimeRequestId.make("pending-batch-request");
      const requestNodeId = NodeId.make("pending-batch-request-node");
      yield* persistOracleEvents([
        {
          id: EventId.make("batch:request"),
          type: "runtime-request.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            id: requestId,
            nodeId: requestNodeId,
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: fixture.now,
            resolvedAt: null,
          },
        },
        {
          id: EventId.make("batch:request-node"),
          type: "node.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.root,
            id: requestNodeId,
            parentNodeId: fixture.root.id,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            runtimeRequestId: requestId,
            completedAt: null,
          },
        },
        {
          id: EventId.make("batch:request-item"),
          type: "turn-item.updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.item,
            id: TurnItemId.make("pending-batch-request-item"),
            nodeId: requestNodeId,
            ordinal: 55,
            type: "user_input_request",
            status: "waiting",
            completedAt: null,
            requestId,
            questions: [
              { id: "choice", header: "Choice", question: "Which implementation?", options: [] },
            ],
          },
        },
      ]);
      const rootId = NodeId.make(`pending-root:${target.threadId}`);
      const attemptId = RunAttemptId.make(`pending-attempt:${target.threadId}`);
      const messageId = MessageId.make(`pending-selected-message:${target.threadId}`);
      const selected = {
        ...target.run,
        rootNodeId: rootId,
        activeAttemptId: attemptId,
        userMessageId: messageId,
      };
      const changedSelection = { ...target.thread.modelSelection, model: "pending-picker-model" };
      const thread = {
        ...target.thread,
        title: "Pending full metadata replacement",
        lastVisitedAt: DateTime.subtract(target.now, { hours: 2 }),
        modelSelection:
          testCase === "copied-restart" ? changedSelection : target.thread.modelSelection,
      };
      const base = { threadId: target.threadId, occurredAt: target.now };
      const batch: OrchestrationV2DomainEvent[] = [
        {
          ...base,
          id: EventId.make("batch:created"),
          type: "thread.created",
          payload: { ...thread, title: "Pending created replacement" },
        },
        {
          ...base,
          id: EventId.make("batch:metadata"),
          type: "thread.metadata-updated",
          payload: thread,
        },
        { ...base, id: EventId.make("batch:selected-run"), type: "run.updated", payload: selected },
        {
          ...base,
          id: EventId.make("batch:historical-run"),
          type: "run.updated",
          payload: { ...target.history[0]!.run, status: "cancelled", startedAt: null },
        },
        {
          ...base,
          id: EventId.make("batch:new-run"),
          type: "run.created",
          payload: {
            ...target.history[1]!.run,
            id: RunId.make(`pending-queued:${target.threadId}`),
            ordinal: 80,
            status: "queued",
            queuePosition: 97,
            queueHeld: true,
            startedAt: null,
            completedAt: null,
            rootNodeId: null,
            activeAttemptId: null,
          },
        },
        {
          ...base,
          id: EventId.make("batch:attempt"),
          type: "run-attempt.updated",
          payload: {
            ...target.attempt,
            id: attemptId,
            runId: target.history[0]!.run.id,
            rootNodeId: rootId,
            attemptOrdinal: 7,
            nativeThreadId: "pending-native-attempt",
            reason: "provider_recovery",
          },
        },
        {
          ...base,
          id: EventId.make("batch:older-attempt"),
          type: "run-attempt.created",
          payload: {
            ...target.attempt,
            id: RunAttemptId.make(`pending-older-attempt:${target.threadId}`),
            attemptOrdinal: 23,
            status: "failed",
            completedAt: DateTime.subtract(target.now, { hours: 1 }),
            nativeThreadId: "older-max-attempt",
          },
        },
        {
          ...base,
          id: EventId.make("batch:root"),
          type: "node.updated",
          payload: {
            ...target.root,
            id: rootId,
            rootNodeId: rootId,
            nativeItemRef: {
              driver: target.providerThread.driver,
              nativeId: "pending-copied-native-item",
              strength: "strong",
            },
            checkpointScopeId: target.history[0]!.scope.id,
          },
        },
        {
          ...base,
          id: EventId.make("batch:turn"),
          type: "provider-turn.updated",
          payload: {
            ...target.turn,
            runAttemptId: attemptId,
            nodeId: rootId,
            nativeTurnRef: {
              driver: target.providerThread.driver,
              nativeId: "pending-native-turn",
              strength: "strong",
            },
            ...(testCase === "completed-turn"
              ? { status: "completed", completedAt: target.now }
              : {}),
          },
        },
        {
          ...base,
          id: EventId.make("batch:provider-thread"),
          type: "provider-thread.updated",
          payload: {
            ...target.providerThread,
            nativeConversationHeadRef: {
              driver: target.providerThread.driver,
              nativeId: "pending-native-head",
              strength: "strong",
            },
          },
        },
        {
          ...base,
          id: EventId.make("batch:old-message-role"),
          type: "message.updated",
          payload: {
            ...target.message,
            role: "assistant",
            text: "Former selected user message is now assistant-only.",
          },
        },
        {
          ...base,
          id: EventId.make("batch:selected-message"),
          type: "message.updated",
          payload: {
            ...target.message,
            id: messageId,
            nodeId: rootId,
            role: testCase === "assistant-maintenance" ? "assistant" : "user",
            text: testCase === "assistant-maintenance" ? "/compact" : "Pending selected request",
          },
        },
        {
          ...base,
          id: EventId.make("batch:visible-item-to-error"),
          type: "turn-item.updated",
          payload: {
            id: target.item.id,
            threadId: target.threadId,
            runId: target.run.id,
            nodeId: rootId,
            providerThreadId: target.providerThread.id,
            providerTurnId: target.turn.id,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 991,
            type: "error",
            status: "failed",
            title: "Pending replacement error",
            startedAt: target.now,
            completedAt: target.now,
            updatedAt: target.now,
            failure: {
              class: "provider_error",
              message: "Retain pending failure payload",
              code: "pending_error",
              retryable: true,
            },
          },
        },
        {
          ...base,
          id: EventId.make("batch:new-selected-item"),
          type: "turn-item.updated",
          payload: {
            ...target.item,
            id: TurnItemId.make(`pending-new-item:${target.threadId}`),
            nodeId: rootId,
            messageId,
            text: "Pending selected request",
            ordinal: 900,
          },
        },
        {
          ...base,
          id: EventId.make("batch:queued-item"),
          type: "turn-item.updated",
          payload: {
            ...target.item,
            id: TurnItemId.make(`pending-queued-item:${target.threadId}`),
            nodeId: rootId,
            messageId: MessageId.make(`async-answer:${requestId}`),
            text: "Before the answer",
            ordinal: 950,
            inputIntent: "queued_turn",
            status: "pending",
          },
        },
        {
          ...base,
          id: EventId.make("batch:subagent"),
          type: "subagent.updated",
          payload: {
            id: NodeId.make(`pending-subagent:${target.threadId}`),
            threadId: target.threadId,
            runId: target.run.id,
            parentNodeId: rootId,
            origin: "provider_native",
            createdBy: "agent",
            driver: target.providerThread.driver,
            providerInstanceId: target.run.providerInstanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: "Pending full sibling prompt",
            title: "Pending sibling",
            model: "pending-child-model",
            completionWake: "settled_only",
            status: "running",
            result: null,
            startedAt: target.now,
            completedAt: null,
            updatedAt: target.now,
          },
        },
        {
          ...base,
          id: EventId.make("batch:transfer"),
          type: "context-transfer.created",
          payload: {
            id: ContextTransferId.make(`pending-transfer:${target.threadId}`),
            type: testCase === "target-merge" ? "merge_back" : "fork",
            sourceThreadId:
              testCase === "target-merge" ? ThreadId.make("pending-source") : target.threadId,
            targetThreadId:
              testCase === "target-merge" ? target.threadId : ThreadId.make("pending-destination"),
            sourcePoint: {
              threadId:
                testCase === "target-merge" ? ThreadId.make("pending-source") : target.threadId,
            },
            basePoint: null,
            sourceProviderInstanceId: target.run.providerInstanceId,
            targetProviderInstanceId: target.run.providerInstanceId,
            targetRunId: null,
            status: "pending",
            resolution: null,
            createdBy: "agent",
            error: null,
            createdAt: target.now,
            updatedAt: target.now,
            consumedAt: null,
          },
        },
      ];
      const runtime = {
        providerSession: fixture.session,
      } as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const runtimeSpy = vi
        .spyOn(sessions, "get")
        .mockImplementation(() =>
          testCase === "target-merge" ? Effect.succeedNone : Effect.succeedSome(runtime),
        );
      const adapter = yield* adapters.get(fixture.run.providerInstanceId);
      const transitionSpy = vi
        .spyOn(adapter, "planSelectionTransition")
        .mockImplementation(() => Effect.succeed({ type: "restart_session" }));
      const readSpy = vi.spyOn(store, "getThreadRecords");
      const capture = yield* startOracleCapture();
      const injected = injectAtResolvedRequest(fixture.threadId, requestId, batch, () =>
        capture.transcript.push({ injectedPendingEvents: batch }),
      );
      yield* Effect.gen(function* () {
        const result = yield* orchestrator
          .dispatch({
            type: "runtime-request.respond",
            commandId: CommandId.make("pending-batch-answer"),
            threadId: fixture.threadId,
            requestId,
            answers: { choice: "The complete saved implementation." },
          })
          .pipe(Effect.result);
        capture.transcript.push(
          Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
        );
        assert.equal(injected.count(), 1);
        assert.isTrue(
          readSpy.mock.calls.some(
            ([id, fields]) =>
              id === fixture.threadId &&
              fields.includes("contextTransfers") &&
              fields.includes("attempts") &&
              fields.includes("messages"),
          ),
        );
        const snapshot = capture.snapshot();
        const serialized = JSON.stringify(snapshot);
        if (testCase === "target-merge") {
          assert.isTrue(Result.isFailure(result));
          assert.include(serialized, "merge");
        } else {
          assert.isTrue(Result.isSuccess(result));
          if (testCase === "completed-turn" || testCase === "assistant-maintenance") {
            const final = yield* store.getThreadProjection(fixture.threadId);
            assert.equal(
              final.runs.find((run) => run.userMessageId === `async-answer:${requestId}`)?.status,
              "queued",
            );
            assert.notInclude(serialized, '"type":"provider-turn.steer"');
          } else if (testCase === "copied-restart") {
            assert.include(serialized, '"type":"provider-turn.restart"');
            assert.include(serialized, '"attemptOrdinal":24');
            assert.include(serialized, '"nativeThreadId":"pending-native-attempt"');
            assert.include(serialized, '"nativeId":"pending-copied-native-item"');
          } else {
            assert.include(serialized, '"type":"provider-turn.steer"');
            assert.include(
              serialized,
              testCase === "other-thread" ? fixture.turn.id : '"ordinal":992',
            );
          }
        }
        expect(snapshot).toMatchSnapshot();
      }).pipe(
        Effect.ensuring(
          Effect.all([
            capture.stop,
            Effect.sync(() => {
              injected.restore();
              readSpy.mockRestore();
              transitionSpy.mockRestore();
              runtimeSpy.mockRestore();
            }),
          ]),
        ),
      );
    }).pipe(Effect.provide(makeOracleLayer())),
);

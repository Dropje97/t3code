import { expect, it, vi, assert } from "@effect/vitest";
import {
  ChatAttachmentId,
  CommandId,
  ContextTransferId,
  EventId,
  MessageId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ScheduledTaskId,
  TurnItemId,
  type OrchestrationV2Command,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderSession,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import {
  makeOracleLayer,
  seedOracle,
  recordOracle,
  startOracleCapture,
  makeRunRecords,
  recordEvents,
  persistOracleEvents,
  otherInstanceId,
} from "./testkit/DispatchReadOracle.ts";

type Dispatch = Extract<OrchestrationV2Command, { type: "message.dispatch" }>;
const attachment = {
  type: "image" as const,
  id: ChatAttachmentId.make("oracle-image"),
  name: "design.png",
  mimeType: "image/png",
  sizeBytes: 64,
};
const cases = [
  "empty-immediate",
  "empty-deferred",
  "immediate-history",
  "deferred-history",
  "fresh-context",
  "imported-runless-immediate",
  "imported-history-fallback",
  "fresh-context-probe-clear",
  "fresh-context-probe-busy",
  "fresh-context-probe-failure",
  "shown-model-previous-selection",
  "literal-uuid-preserved",
  "source-agent-mcp",
  "source-user-mobile",
  "source-system-server",
  "source-agent-provider",
  "title-seed-empty",
  "title-seed-history",
  "title-maintenance-only",
  "title-nonmaintenance",
  "title-maintenance-attachment",
  "title-maintenance-command",
  "unsettle-and-unsnooze",
  "notification-preserves-snooze",
  "auto-running",
  "auto-preparing",
  "auto-waiting",
  "auto-completed-turn",
  "auto-null-attempt",
  "auto-null-owned-turn",
  "intent-steer",
  "intent-restart",
  "multiple-live-auto-last",
  "multiple-live-queue-first",
  "explicit-missing",
  "explicit-foreign",
  "explicit-terminal",
  "explicit-preparing",
  "explicit-completed-turn",
  "steer-different-root-turn",
  "steer-null-attempt",
  "steer-null-owned-turn",
  "steer-native-tie-order",
  "restart-native-tie-order",
  "steer-stop-reached",
  "steer-unsupported",
  "steer-maintenance",
  "steer-maintenance-target",
  "steer-goal",
  "restart-same-selection",
  "restart-attachment",
  "restart-missing-attempt",
  "restart-missing-root",
  "restart-null-root",
  "restart-cross-run-attempt",
  "restart-higher-older-attempt",
  "restart-copied-fields",
  "steer-model-change-apply-next",
  "restart-model-change-handoff",
  "restart-provider-change-handoff",
  "queue-running",
  "queue-preparing",
  "queue-failed-worktree",
  "queue-old-failed-worktree",
  "queue-release-user",
  "queue-preserve-server",
  "queue-preserve-scheduled",
  "queue-position-gaps",
  "queue-duplicate-linked-item",
  "queue-buffered-provider-wake",
  "queue-buffered-provider-stopped",
  "queue-pending-merge-rejected",
] as const;

it.effect.each(cases)("preserves complete message dispatch transcript: %s", (testCase) =>
  Effect.gen(function* () {
    const active =
      testCase.startsWith("auto-") ||
      testCase.startsWith("intent-") ||
      testCase.startsWith("steer-") ||
      testCase.startsWith("restart-") ||
      testCase.startsWith("queue-") ||
      testCase.startsWith("multiple-") ||
      testCase.startsWith("explicit-");
    const empty = testCase.includes("empty") || testCase === "imported-runless-immediate";
    const fixture = yield* seedOracle({
      history: empty ? 0 : 4,
      active: empty ? null : active ? "running" : "completed",
      provider: !empty,
    });
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    let command: Dispatch = {
      type: "message.dispatch",
      commandId: CommandId.make("oracle-send"),
      threadId: fixture.threadId,
      messageId: MessageId.make("oracle-message"),
      text: "Continue the work",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    };
    let run = fixture.run;
    let root = fixture.root;
    let turn = fixture.turn;
    let session: OrchestrationV2ProviderSession = fixture.session;
    const events: OrchestrationV2DomainEvent[] = [];
    const base = { threadId: fixture.threadId, occurredAt: fixture.now };
    const add = (event: Omit<OrchestrationV2DomainEvent, "id">) =>
      events.push({
        ...event,
        id: EventId.make(`mutate:${events.length}`),
      } as OrchestrationV2DomainEvent);
    if (testCase.includes("deferred"))
      command = { ...command, dispatchMode: { type: "defer_start" } };
    if (testCase.startsWith("fresh-context")) command = { ...command, freshContext: true };
    if (testCase.startsWith("imported-")) {
      add({
        ...base,
        type: "thread.metadata-updated",
        payload: { ...fixture.thread, historyOrigin: "v1_import" },
      });
      add({
        ...base,
        type: "message.updated",
        payload: {
          ...fixture.message,
          id: MessageId.make("imported-user"),
          runId: null,
          nodeId: null,
          text: "IMPORTED_USER_HISTORY_MARKER",
        },
      });
      add({
        ...base,
        type: "turn-item.updated",
        payload: {
          ...fixture.item,
          id: TurnItemId.make("imported-user-item"),
          messageId: MessageId.make("imported-user"),
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          ordinal: 17,
          text: "IMPORTED_USER_HISTORY_MARKER",
        },
      });
      add({
        ...base,
        type: "turn-item.updated",
        payload: {
          ...fixture.item,
          type: "assistant_message",
          id: TurnItemId.make("imported-assistant-item"),
          messageId: MessageId.make("imported-assistant"),
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          ordinal: 18,
          text: "IMPORTED_ASSISTANT_HISTORY_MARKER",
          streaming: false,
        },
      });
    }
    if (testCase === "source-agent-mcp")
      command = { ...command, createdBy: "agent", creationSource: "mcp" };
    if (testCase === "source-user-mobile") command = { ...command, creationSource: "mobile" };
    if (testCase === "source-system-server")
      command = { ...command, createdBy: "system", creationSource: "server" };
    if (testCase === "source-agent-provider")
      command = { ...command, createdBy: "agent", creationSource: "provider" };
    if (testCase.startsWith("title-seed")) command = { ...command, titleSeed: "A concrete title" };
    if (testCase.startsWith("title-maintenance") || testCase === "title-nonmaintenance") {
      for (const [index, bundle] of [...fixture.history, fixture].entries())
        add({
          ...base,
          type: "message.updated",
          payload: {
            ...bundle.message,
            text:
              testCase === "title-nonmaintenance" && index === 0
                ? "Real prior message"
                : " \u2003/CoMpAcT\u00a0",
            attachments:
              testCase === "title-maintenance-attachment" && index === 0 ? [attachment] : [],
          },
        });
      if (testCase === "title-maintenance-command") command = { ...command, text: "/logout" };
    }
    if (["unsettle-and-unsnooze", "notification-preserves-snooze"].includes(testCase)) {
      add({
        ...base,
        type: "thread.metadata-updated",
        payload: {
          ...fixture.thread,
          settledOverride: "settled",
          settledAt: fixture.now,
          unsettledAt: null,
          snoozedUntil: DateTime.add(fixture.now, { hours: 1 }),
          snoozedAt: fixture.now,
        },
      });
      if (testCase === "notification-preserves-snooze")
        command = {
          ...command,
          createdBy: "agent",
          creationSource: "server",
          dispatchMode: { type: "queue_after_active" },
          notification: {
            source: { kind: "command" },
            outcome: "completed",
            summary: "Background command finished",
            detail: "Exit 0",
          },
        };
    }
    if (testCase.startsWith("auto-") || testCase.startsWith("multiple-live-auto"))
      command = { ...command, deliveryIntent: "auto" };
    if (testCase === "intent-steer") command = { ...command, deliveryIntent: "steer" };
    if (testCase === "intent-restart") command = { ...command, deliveryIntent: "restart" };
    if (
      testCase.startsWith("steer-") ||
      testCase.startsWith("explicit-") ||
      testCase.startsWith("restart-")
    )
      command = {
        ...command,
        dispatchMode: {
          type: testCase.startsWith("restart-") ? "restart_active" : "steer_active",
          targetRunId: run.id,
        },
      };
    if (testCase === "restart-attachment")
      command = {
        ...command,
        attachments: [attachment],
        dispatchMode: { type: "steer_active", targetRunId: run.id },
      };
    if (testCase === "explicit-missing")
      command = {
        ...command,
        dispatchMode: { type: "steer_active", targetRunId: RunId.make("missing-run") },
      };
    if (testCase === "explicit-foreign") {
      yield* seedOracle({ threadId: ThreadIdForForeign, history: 0, active: "running" });
      const foreign = makeRunRecords(ThreadIdForForeign, 11, "running", fixture.now);
      yield* persistOracleEvents(recordEvents(foreign, fixture.now));
      command = { ...command, dispatchMode: { type: "steer_active", targetRunId: foreign.run.id } };
    }
    if (testCase === "explicit-terminal")
      run = { ...run, status: "completed", completedAt: fixture.now };
    if (
      testCase === "auto-preparing" ||
      testCase === "explicit-preparing" ||
      testCase === "queue-preparing"
    )
      run = { ...run, status: "preparing", startedAt: null };
    if (testCase === "auto-waiting") run = { ...run, status: "waiting" };
    if (testCase.includes("completed-turn"))
      turn = { ...turn, status: "completed", completedAt: fixture.now };
    if (testCase.includes("null-attempt")) run = { ...run, activeAttemptId: null };
    if (testCase.includes("null-owned-turn")) {
      run = { ...run, activeAttemptId: null };
      turn = { ...turn, runAttemptId: null };
    }
    if (testCase === "steer-different-root-turn")
      turn = { ...turn, nodeId: fixture.history[0]!.root.id };
    if (testCase.endsWith("native-tie-order")) {
      for (const suffix of ["a", "z"])
        add({
          ...base,
          type: "provider-turn.updated",
          payload: {
            ...turn,
            id: ProviderTurnId.make(`turn-tie-${suffix}`),
            providerThreadId: ProviderThreadId.make(`${suffix}-provider`),
            ordinal: turn.ordinal,
            nativeTurnRef: { ...turn.nativeTurnRef!, nativeId: `native-tie-${suffix}` },
          },
        });
    }
    if (testCase === "steer-stop-reached" || testCase === "queue-buffered-provider-stopped")
      add({
        ...base,
        type: "turn-item.updated",
        payload: {
          ...fixture.item,
          id: TurnItemId.make("stop-marker"),
          type: "run_interrupt_request",
          message: "Stopped by user",
          ordinal: 90,
        },
      });
    if (testCase === "steer-unsupported")
      session = {
        ...session,
        capabilities: {
          ...session.capabilities,
          turns: {
            ...session.capabilities.turns,
            supportsActiveSteering: false,
            supportsSteeringByInterruptRestart: false,
            supportsQueuedMessages: false,
          },
        },
      };
    if (testCase === "restart-attachment")
      session = {
        ...session,
        capabilities: {
          ...session.capabilities,
          turns: { ...session.capabilities.turns, activeSteeringAcceptsAttachments: false },
        },
      };
    if (testCase === "steer-maintenance") command = { ...command, text: "/compact" };
    if (testCase === "steer-goal") command = { ...command, text: "/goal Fix all the regressions" };
    if (testCase === "steer-maintenance-target")
      add({ ...base, type: "message.updated", payload: { ...fixture.message, text: "/compact" } });
    if (testCase === "restart-missing-attempt")
      run = { ...run, activeAttemptId: RunAttemptId.make("missing-attempt") };
    if (testCase === "restart-missing-root")
      run = { ...run, rootNodeId: NodeId.make("missing-root") };
    if (testCase === "restart-null-root") run = { ...run, rootNodeId: null };
    if (testCase === "restart-cross-run-attempt")
      run = { ...run, activeAttemptId: fixture.history[0]!.attempt.id };
    if (["restart-missing-attempt", "restart-cross-run-attempt"].includes(testCase))
      turn = { ...turn, runAttemptId: run.activeAttemptId };
    if (testCase === "restart-higher-older-attempt")
      add({
        ...base,
        type: "run-attempt.created",
        payload: {
          ...fixture.attempt,
          id: RunAttemptId.make("older-high-attempt"),
          attemptOrdinal: 12,
          status: "failed",
          completedAt: DateTime.subtract(fixture.now, { hours: 1 }),
        },
      });
    if (testCase === "restart-copied-fields") {
      add({
        ...base,
        type: "run-attempt.updated",
        payload: {
          ...fixture.attempt,
          nativeThreadId: "original-attempt-native",
          reason: "provider_recovery",
        },
      });
      root = {
        ...root,
        runtimeRequestId: null,
        nativeItemRef: {
          driver: fixture.providerThread.driver,
          nativeId: "copy-native-item",
          strength: "strong",
        },
        checkpointScopeId: fixture.history[0]!.scope.id,
      };
    }
    if (testCase === "multiple-live-queue-first")
      run = { ...run, status: "preparing", startedAt: null };
    if (testCase.startsWith("multiple-live")) {
      const newer = makeRunRecords(
        fixture.threadId,
        11,
        "running",
        fixture.now,
        fixture.providerThread.id,
      );
      yield* persistOracleEvents(recordEvents(newer, fixture.now));
      if (testCase.endsWith("queue-first"))
        command = { ...command, dispatchMode: { type: "queue_after_active" } };
    }
    if (testCase.startsWith("queue-"))
      command = { ...command, dispatchMode: { type: "queue_after_active" } };
    if (["queue-failed-worktree", "queue-old-failed-worktree"].includes(testCase)) {
      const failed = testCase === "queue-failed-worktree" ? run : fixture.history[0]!.run;
      if (testCase === "queue-failed-worktree")
        run = {
          ...failed,
          status: "failed",
          completedAt: fixture.now,
          workspacePreparation: { type: "worktree", baseRef: "main", branch: "oracle-worktree" },
        };
      else
        add({
          ...base,
          type: "run.updated",
          payload: {
            ...failed,
            status: "failed",
            completedAt: DateTime.subtract(fixture.now, { hours: 1 }),
            workspacePreparation: { type: "worktree", baseRef: "main", branch: "old-worktree" },
          },
        });
      add({
        ...base,
        type: "thread.metadata-updated",
        payload: { ...fixture.thread, worktreePath: null },
      });
    }
    if (
      [
        "queue-release-user",
        "queue-preserve-server",
        "queue-preserve-scheduled",
        "queue-position-gaps",
        "queue-old-failed-worktree",
        "queue-buffered-provider-wake",
        "queue-buffered-provider-stopped",
      ].includes(testCase)
    ) {
      for (const [index, ordinal] of [17, 43].entries()) {
        const queued = makeRunRecords(
          fixture.threadId,
          ordinal,
          "queued",
          fixture.now,
          fixture.providerThread.id,
        );
        queued.run = { ...queued.run, queueHeld: true, queuePosition: index ? 120 : 70 };
        yield* persistOracleEvents(recordEvents(queued, fixture.now));
      }
      if (testCase.startsWith("queue-buffered-provider"))
        command = { ...command, createdBy: "agent", creationSource: "provider" };
      if (testCase === "queue-preserve-server")
        command = { ...command, createdBy: "system", creationSource: "server" };
      if (testCase === "queue-preserve-scheduled")
        command = { ...command, scheduledTaskId: ScheduledTaskId.make("scheduled-send") };
    }
    if (testCase === "queue-duplicate-linked-item") {
      command = { ...command, messageId: fixture.history[0]!.message.id };
      add({
        ...base,
        type: "turn-item.updated",
        payload: {
          ...fixture.history[0]!.item,
          id: TurnItemId.make("duplicate-link-later"),
          ordinal: 89,
          title: "Preserve first item metadata",
        },
      });
    }
    if (testCase === "shown-model-previous-selection")
      add({
        ...base,
        type: "run.updated",
        payload: {
          ...run,
          modelSelection: {
            ...run.modelSelection,
            model: "previous-model",
            options: [{ id: "reasoningEffort", value: "low" }],
          },
        },
      });
    if (testCase === "literal-uuid-preserved")
      command = { ...command, text: "Keep identity 12345678-1234-4234-a234-123456789abc exactly" };
    if (testCase.startsWith("fresh-context-probe")) {
      session = {
        ...session,
        capabilities: {
          ...session.capabilities,
          sessions: {
            ...session.capabilities.sessions,
            supportsMultipleProviderThreadsPerSession: false,
          },
        },
      };
      add({ ...base, type: "provider-session.attached", payload: session });
    }
    if (testCase === "steer-model-change-apply-next" || testCase === "restart-model-change-handoff")
      command = {
        ...command,
        modelSelection: {
          ...fixture.thread.modelSelection,
          model: "new-model",
          options: [{ id: "reasoningEffort", value: "high" }],
        },
      };
    if (testCase === "restart-provider-change-handoff" || testCase === "restart-native-tie-order")
      command = {
        ...command,
        modelSelection: { ...fixture.thread.modelSelection, instanceId: otherInstanceId },
      };
    if (testCase === "queue-pending-merge-rejected")
      add({
        ...base,
        type: "context-transfer.created",
        payload: {
          id: ContextTransferId.make("pending-target-merge"),
          type: "merge_back",
          sourceThreadId: ThreadIdForForeign,
          targetThreadId: fixture.threadId,
          sourcePoint: {
            threadId: ThreadIdForForeign,
            runId: fixture.history[0]!.run.id,
            providerThreadRef: fixture.providerThread.nativeThreadRef,
            providerTurnRef: fixture.history[0]!.turn.nativeTurnRef!,
          },
          basePoint: null,
          sourceProviderInstanceId: run.providerInstanceId,
          targetProviderInstanceId: null,
          targetRunId: null,
          status: "pending",
          resolution: null,
          createdBy: "user",
          error: null,
          createdAt: fixture.now,
          updatedAt: fixture.now,
          consumedAt: null,
        },
      });
    if (active) {
      add({ ...base, type: "run.updated", payload: run });
      add({ ...base, type: "node.updated", payload: root });
      add({ ...base, type: "provider-turn.updated", payload: turn });
      add({ ...base, type: "provider-session.attached", payload: session });
    }
    yield* persistOracleEvents(events);
    const runtime = {
      providerSession: session,
      ...(testCase.startsWith("fresh-context-probe")
        ? {
            hasPendingBackgroundWork: testCase.endsWith("failure")
              ? Effect.die("probe failed")
              : Effect.succeed(testCase.endsWith("busy")),
          }
        : {}),
    } as ProviderAdapter.ProviderAdapterV2SessionRuntime;
    const runtimeSpy = vi
      .spyOn(sessions, "get")
      .mockImplementation(() => Effect.succeedSome(runtime));
    const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
    const adapter = yield* registry.get(fixture.run.providerInstanceId);
    const transitionSpy =
      testCase === "restart-model-change-handoff"
        ? vi
            .spyOn(adapter, "planSelectionTransition")
            .mockImplementation(() => Effect.succeed({ type: "create_with_handoff" }))
        : undefined;
    yield* Effect.gen(function* () {
      const transcript = yield* recordOracle(orchestrator.dispatch(command));
      const expectedError = [
        "fresh-context-probe-busy",
        "fresh-context-probe-failure",
        "queue-pending-merge-rejected",
        "auto-waiting",
        "explicit-foreign",
        "explicit-missing",
        "explicit-preparing",
        "restart-null-root",
        "steer-maintenance",
        "steer-null-attempt",
        "steer-stop-reached",
        "steer-unsupported",
      ].includes(testCase);
      assert.equal(Object.hasOwn(transcript.at(-1), "error"), expectedError);
      if (
        [
          "auto-running",
          "intent-steer",
          "steer-null-owned-turn",
          "steer-different-root-turn",
          "steer-native-tie-order",
        ].includes(testCase)
      )
        assert.include(JSON.stringify(transcript), '"type":"provider-turn.steer"');
      if (testCase === "imported-runless-immediate")
        assert.include(JSON.stringify(transcript), '"call":"handoff.prepareLegacyImport"');
      if (testCase === "multiple-live-queue-first")
        assert.notInclude(JSON.stringify(transcript), '"call":"checkpoint.prepareRootRunScope"');
      if (testCase.startsWith("fresh-context-probe"))
        assert.include(JSON.stringify(transcript), '"call":"runtime.hasPendingBackgroundWork"');
      expect(transcript).toMatchSnapshot();
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          runtimeSpy.mockRestore();
          transitionSpy?.mockRestore();
        }),
      ),
    );
  }).pipe(Effect.provide(makeOracleLayer())),
);

const ThreadIdForForeign = ThreadId.make("foreign-thread");

it.effect.each(["lower-user", "move-error", "irrelevant-assistant-maximum"] as const)(
  "retains initial user/error ordinal floor across provider update: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle();
      if (testCase === "move-error")
        yield* seedOracle({ threadId: ThreadIdForForeign, history: 1, active: null });
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const highest = { ...fixture.item, id: TurnItemId.make("initial-highest"), ordinal: 900 };
      const event: OrchestrationV2DomainEvent =
        testCase === "move-error"
          ? {
              id: EventId.make("floor-error"),
              type: "turn-item.updated",
              threadId: fixture.threadId,
              occurredAt: fixture.now,
              payload: {
                ...highest,
                type: "error",
                failure: {
                  class: "provider_error",
                  message: "Old provider error",
                  code: null,
                  retryable: null,
                },
              },
            }
          : {
              id: EventId.make("floor-user"),
              type: "turn-item.updated",
              threadId: fixture.threadId,
              occurredAt: fixture.now,
              payload: highest,
            };
      yield* persistOracleEvents([event]);
      if (testCase === "irrelevant-assistant-maximum")
        yield* persistOracleEvents([
          {
            id: EventId.make("other-maximum"),
            type: "turn-item.updated",
            threadId: fixture.threadId,
            occurredAt: fixture.now,
            payload: {
              ...highest,
              id: TurnItemId.make("assistant-maximum"),
              ordinal: 9000,
              type: "assistant_message",
              text: "An unrelated assistant body",
              streaming: false,
            },
          },
        ]);
      const reachedLaterRead = yield* Deferred.make<void>();
      const providerUpdated = yield* Deferred.make<void>();
      const originalNext = store.getNextTurnItemOrdinal;
      const capture = yield* startOracleCapture();
      const nextSpy = vi.spyOn(store, "getNextTurnItemOrdinal").mockImplementation((threadId) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(reachedLaterRead, undefined);
          yield* Deferred.await(providerUpdated);
          const next = yield* originalNext(threadId);
          capture.transcript.push({ laterStoreNextOrdinal: next });
          return next;
        }),
      );
      yield* Effect.gen(function* () {
        const dispatch = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("race-send"),
            threadId: fixture.threadId,
            messageId: MessageId.make("race-message"),
            text: "Input after control read",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(reachedLaterRead);
        yield* store.apply({
          ...event,
          id: EventId.make("provider-update"),
          payload: {
            ...event.payload,
            ordinal: 2,
            ...(testCase === "move-error" ? { threadId: ThreadIdForForeign } : {}),
          },
          ...(testCase === "move-error" ? { threadId: ThreadIdForForeign } : {}),
        } as OrchestrationV2DomainEvent);
        if (testCase === "irrelevant-assistant-maximum") {
          const projection = yield* store.getThreadProjection(fixture.threadId);
          const assistant = projection.turnItems.find((item) => item.id === "assistant-maximum")!;
          yield* store.apply({
            id: EventId.make("provider-assistant-update"),
            type: "turn-item.updated",
            threadId: fixture.threadId,
            occurredAt: fixture.now,
            payload: { ...assistant, ordinal: 3 },
          });
        }
        capture.transcript.push({ providerUpdate: { itemId: highest.id, ordinal: 2 } });
        yield* Deferred.succeed(providerUpdated, undefined);
        const result = yield* Fiber.join(dispatch);
        capture.transcript.push({ result });
        const projection = yield* store.getThreadProjection(fixture.threadId);
        const newItem = projection.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === "race-message",
        );
        assert.ok(newItem);
        // Stored sink allocation may rebase ordinals; inspect the pre-commit event as well.
        assert.include(JSON.stringify(capture.snapshot()), '"ordinal":901');
        expect(capture.snapshot()).toMatchSnapshot();
      }).pipe(
        Effect.ensuring(Effect.all([capture.stop, Effect.sync(() => nextSpy.mockRestore())])),
      );
    }).pipe(Effect.provide(makeOracleLayer())),
);

it.effect.each(["idle-answer", "active-answer", "item-outside-initial-cohort"] as const)(
  "preserves pending request, node and item updates before nested dispatch: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({
        active: testCase === "active-answer" ? "running" : "completed",
      });
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const readSpy = vi.spyOn(store, "getThreadRecords");
      const requestId = RuntimeRequestId.make("pending-answer");
      const nodeId = NodeId.make("pending-answer-node");
      const now = fixture.now;
      yield* persistOracleEvents([
        {
          id: EventId.make("pending-request"),
          type: "runtime-request.updated",
          threadId: fixture.threadId,
          occurredAt: now,
          payload: {
            id: requestId,
            nodeId,
            providerTurnId: null,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "message" },
            createdAt: now,
            resolvedAt: null,
          },
        },
        {
          id: EventId.make("pending-request-node"),
          type: "node.updated",
          threadId: fixture.threadId,
          occurredAt: now,
          payload: {
            ...fixture.root,
            id: nodeId,
            parentNodeId: fixture.root.id,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            runtimeRequestId: requestId,
            completedAt: null,
          },
        },
        {
          id: EventId.make("pending-request-item"),
          type: "turn-item.updated",
          threadId: fixture.threadId,
          occurredAt: now,
          payload: {
            ...fixture.item,
            id: TurnItemId.make("pending-answer-item"),
            nodeId,
            ordinal: testCase === "item-outside-initial-cohort" ? 777 : 55,
            type: "user_input_request",
            status: "waiting",
            completedAt: null,
            requestId,
            questions: [{ id: "color", header: "Color", question: "Which color?", options: [] }],
          },
        },
      ]);
      const runtime = {
        providerSession: fixture.session,
      } as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const runtimeSpy = vi
        .spyOn(sessions, "get")
        .mockImplementation(() => Effect.succeedSome(runtime));
      yield* Effect.gen(function* () {
        const transcript = yield* recordOracle(
          orchestrator.dispatch({
            type: "runtime-request.respond",
            commandId: CommandId.make("answer-command"),
            threadId: fixture.threadId,
            requestId,
            answers: { color: "Blue" },
          }),
        );
        assert.isTrue(
          readSpy.mock.calls.some(
            ([threadId, fields]) =>
              threadId === fixture.threadId &&
              fields.includes("contextTransfers") &&
              fields.includes("attempts") &&
              fields.includes("messages"),
          ),
        );
        assert.isFalse(Object.hasOwn(transcript.at(-1), "error"));
        assert.include(JSON.stringify(transcript), "async-answer:pending-answer");
        if (testCase === "active-answer")
          assert.include(JSON.stringify(transcript), '"type":"provider-turn.steer"');
        if (testCase === "item-outside-initial-cohort")
          assert.include(JSON.stringify(transcript), '"ordinal":778');
        expect(transcript).toMatchSnapshot();
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            runtimeSpy.mockRestore();
            readSpy.mockRestore();
          }),
        ),
      );
    }).pipe(Effect.provide(makeOracleLayer())),
);

import { assert, expect, it, vi } from "@effect/vitest";
import {
  CheckpointScopeId,
  ContextHandoffId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProjectId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

import {
  makeOracleLayer,
  makeRunRecords,
  persistOracleEvents,
  startOracleCapture,
  seedOracle,
} from "./testkit/DispatchReadOracle.ts";

const cases = [
  "empty",
  "all-held",
  "archived",
  "deleted",
  "live-preparing",
  "live-starting",
  "live-running",
  "live-waiting",
  "archived-after-preflight",
  "deleted-after-preflight",
  "live-after-preflight",
  "default-order-gaps",
  "explicit-choice",
  "explicit-held-choice",
  "explicit-held-no-admitting-run",
  "explicit-missing",
  "explicit-terminal",
  "delegated-priority",
  "delegated-held-skipped",
  "missing-message",
  "assistant-message",
  "null-root",
  "null-attempt",
  "null-provider-thread",
  "missing-root",
  "missing-attempt",
  "missing-provider-thread",
  "missing-scope",
  "scope-null-prepares",
  "attempt-owned-by-other-run",
  "duplicate-linked-item-first-match",
  "usage-limit-before-missing-state",
  "session-error-invalidates-usage-limit",
  "disabled-provider-before-missing-state",
  "runtime-ceiling-before-missing-state",
  "restart-background-probes-clear",
  "restart-background-second-busy",
  "restart-background-probe-failure",
  "rebase-newer-user",
  "rebase-newer-notification",
  "no-rebase-newer-assistant",
  "no-rebase-newer-cancelled-without-prompt",
  "picker-changed-ordinary-retains-model",
  "picker-changed-monitor-follows-model",
  "picker-changed-buffered-monitor-retains-model",
  "fresh-context-reserved-native",
  "reserved-before-fresh-boundary",
  "cancelled-unstarted-restart-is-boundary",
  "cancelled-unstarted-ordinary-is-not-boundary",
  "handoff-latest-instance-differs-from-picker",
  "last-delivered-failed-with-membership",
  "last-delivered-failed-without-membership",
  "last-delivered-interrupted-with-membership",
  "last-delivered-null-attempt-membership",
  "runless-legacy-import",
  "shown-model-completion-time-order",
  "shown-model-completion-tie-ordinal",
  "shown-model-cancelled-unstarted-infinity",
  "shown-model-shared-message-foreign-run-link",
  "shown-model-handoff-only",
] as const;

type QueueCase = (typeof cases)[number];
type RunRecords = Omit<ReturnType<typeof makeRunRecords>, "item"> & {
  item: OrchestrationV2TurnItem;
};

const makeQueuedRecords = (
  ...args: Parameters<typeof makeRunRecords>
): ReturnType<typeof makeRunRecords> => {
  const records = makeRunRecords(...args);
  return {
    ...records,
    root: { ...records.root, status: "pending", providerTurnId: null, startedAt: null },
    attempt: {
      ...records.attempt,
      status: "pending",
      providerTurnId: null,
      startedAt: null,
      nativeThreadId: "reserved-queued-native",
    },
    item: {
      ...records.item,
      status: "pending",
      providerTurnId: null,
      startedAt: null,
      completedAt: null,
    },
  };
};

const persistRun = (records: RunRecords, now: DateTime.Utc) => {
  const { run, root, attempt, message, item, scope, turn } = records;
  const common = { threadId: run.threadId, occurredAt: now };
  const events: OrchestrationV2DomainEvent[] = [
    { ...common, id: EventId.make(`queue:run:${run.id}`), type: "run.created", payload: run },
    { ...common, id: EventId.make(`queue:root:${root.id}`), type: "node.updated", payload: root },
    {
      ...common,
      id: EventId.make(`queue:attempt:${attempt.id}`),
      type: "run-attempt.created",
      payload: attempt,
    },
    {
      ...common,
      id: EventId.make(`queue:message:${message.id}`),
      type: "message.updated",
      payload: message,
    },
    {
      ...common,
      id: EventId.make(`queue:item:${item.id}`),
      type: "turn-item.updated",
      payload: item,
    },
    {
      ...common,
      id: EventId.make(`queue:scope:${scope.id}`),
      type: "checkpoint-scope.created",
      payload: scope,
    },
    {
      ...common,
      id: EventId.make(`queue:turn:${turn.id}`),
      type: "provider-turn.updated",
      payload: turn,
    },
  ];
  return persistOracleEvents(
    run.status === "queued"
      ? events.filter((event) => event.type !== "provider-turn.updated")
      : events,
  );
};

const seedQueuedCase = (testCase: QueueCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedOracle({
      history: testCase === "runless-legacy-import" ? 0 : 6,
      active: testCase === "runless-legacy-import" ? null : "completed",
      runtimeMode:
        testCase === "runtime-ceiling-before-missing-state" ? "approval-required" : "full-access",
    });
    const { threadId, now } = fixture;
    const common = { threadId, occurredAt: now };
    const queued = makeQueuedRecords(threadId, 20, "queued", now, fixture.providerThread.id);
    const later = makeQueuedRecords(threadId, 30, "queued", now, fixture.providerThread.id);
    let chosen: RunRecords = queued;
    let explicit = false;
    let explicitRunId: RunId | undefined;
    let replay = false;
    let disableProvider = false;
    let rows: RunRecords[] = testCase === "empty" ? [] : [queued, later];
    const extraEvents: OrchestrationV2DomainEvent[] = [];
    const updateThread = (payload: typeof fixture.thread) =>
      extraEvents.push({
        ...common,
        id: EventId.make("queue:thread"),
        type: "thread.metadata-updated",
        payload,
      });
    const updateRun = (payload: OrchestrationV2Run) =>
      extraEvents.push({
        ...common,
        id: EventId.make(`queue:update:${payload.id}`),
        type: "run.updated",
        payload,
      });
    const updateItem = (payload: OrchestrationV2TurnItem) =>
      extraEvents.push({
        ...common,
        id: EventId.make(`queue:update:${payload.id}`),
        type: "turn-item.updated",
        payload,
      });
    if (testCase === "all-held")
      rows = rows.map((row) => ({ ...row, run: { ...row.run, queueHeld: true } }));
    if (testCase === "archived") updateThread({ ...fixture.thread, archivedAt: now });
    if (testCase === "deleted") updateThread({ ...fixture.thread, deletedAt: now });
    if (testCase.startsWith("live-") && !testCase.endsWith("after-preflight")) {
      updateRun({
        ...fixture.run,
        status: testCase.slice(5) as OrchestrationV2Run["status"],
        completedAt: null,
      });
    }
    if (testCase === "default-order-gaps") {
      rows = [
        { ...queued, run: { ...queued.run, queuePosition: 900 } },
        { ...later, run: { ...later.run, queuePosition: 7 } },
      ];
    }
    if (testCase.startsWith("explicit-")) {
      explicit = true;
      chosen = later;
      explicitRunId = later.run.id;
      if (testCase !== "explicit-choice") replay = true;
      if (testCase === "explicit-held-choice" || testCase === "explicit-held-no-admitting-run") {
        rows = rows.map((row) => ({
          ...row,
          run: {
            ...row.run,
            queueHeld: row.run.id === later.run.id || testCase === "explicit-held-no-admitting-run",
          },
        }));
      }
      if (testCase === "explicit-missing") explicitRunId = RunId.make("run:missing-explicit");
      if (testCase === "explicit-terminal")
        rows = rows.map((row) =>
          row.run.id === later.run.id
            ? { ...row, run: { ...row.run, status: "completed", startedAt: now, completedAt: now } }
            : row,
        );
    }
    if (testCase === "delegated-priority" || testCase === "delegated-held-skipped") {
      rows = [
        queued,
        {
          ...later,
          run: {
            ...later.run,
            queuePosition: 999,
            queueHeld: testCase === "delegated-held-skipped",
          },
          message: {
            ...later.message,
            createdBy: "agent",
            creationSource: "server",
            delegatedCompletion: {
              parentRunId: fixture.run.id,
              taskIds: [NodeId.make("task:queue-delegated")],
              generation: 3,
            },
            notification: {
              source: { kind: "delegated_task", taskIds: [NodeId.make("task:queue-delegated")] },
              outcome: "completed",
              summary: "Delegated work finished",
            },
          },
        },
      ];
    }
    const missing = new Set([
      "missing-message",
      "assistant-message",
      "null-root",
      "null-attempt",
      "null-provider-thread",
      "missing-root",
      "missing-attempt",
      "missing-provider-thread",
      "missing-scope",
      "scope-null-prepares",
      "attempt-owned-by-other-run",
      "usage-limit-before-missing-state",
      "disabled-provider-before-missing-state",
      "runtime-ceiling-before-missing-state",
    ]);
    if (missing.has(testCase)) {
      chosen = { ...queued };
      if (
        [
          "missing-message",
          "usage-limit-before-missing-state",
          "disabled-provider-before-missing-state",
          "runtime-ceiling-before-missing-state",
        ].includes(testCase)
      )
        chosen = {
          ...chosen,
          run: { ...chosen.run, userMessageId: MessageId.make("message:missing-queue") },
        };
      if (testCase === "assistant-message")
        chosen = { ...chosen, message: { ...chosen.message, role: "assistant" } };
      if (testCase === "null-root")
        chosen = { ...chosen, run: { ...chosen.run, rootNodeId: null } };
      if (testCase === "null-attempt")
        chosen = { ...chosen, run: { ...chosen.run, activeAttemptId: null } };
      if (testCase === "null-provider-thread")
        chosen = { ...chosen, run: { ...chosen.run, providerThreadId: null } };
      if (testCase === "missing-root")
        chosen = {
          ...chosen,
          run: { ...chosen.run, rootNodeId: NodeId.make("node:missing-queue") },
        };
      if (testCase === "missing-attempt")
        chosen = {
          ...chosen,
          run: { ...chosen.run, activeAttemptId: RunAttemptId.make("attempt:missing-queue") },
        };
      if (testCase === "missing-provider-thread")
        chosen = {
          ...chosen,
          run: {
            ...chosen.run,
            providerThreadId: ProviderThreadId.make("provider-thread:missing-queue"),
          },
        };
      if (testCase === "missing-scope")
        chosen = {
          ...chosen,
          root: {
            ...chosen.root,
            checkpointScopeId: CheckpointScopeId.make("scope:missing-queue"),
          },
        };
      if (testCase === "scope-null-prepares")
        chosen = { ...chosen, root: { ...chosen.root, checkpointScopeId: null } };
      if (testCase === "attempt-owned-by-other-run")
        chosen = {
          ...chosen,
          attempt: {
            ...chosen.attempt,
            runId: fixture.history[0]!.run.id,
            rootNodeId: fixture.history[0]!.root.id,
            attemptOrdinal: 17,
            reason: "provider_recovery",
          },
        };
      rows = [chosen, later];
    }
    if (testCase === "duplicate-linked-item-first-match") {
      updateItem({
        ...queued.item,
        id: TurnItemId.make("item:queue-duplicate-a"),
        ordinal: 51,
        text: "First legacy text",
        title: "Legacy first",
        nativeItemRef: {
          driver: fixture.providerThread.driver,
          nativeId: "legacy-first",
          strength: "strong",
        },
      });
      updateItem({
        ...queued.item,
        id: TurnItemId.make("item:queue-duplicate-b"),
        ordinal: 52,
        text: "Second legacy text",
        title: "Legacy second",
      });
    }
    if (
      testCase === "usage-limit-before-missing-state" ||
      testCase === "session-error-invalidates-usage-limit"
    ) {
      updateRun({
        ...fixture.run,
        status: "failed",
        completedAt: DateTime.add(now, { seconds: 10 }),
      });
      const failure = {
        class: "usage_limit" as const,
        code: "queue-limit",
        message: "Provider usage exhausted",
        retryable: true,
        resetAt: "2040-01-01T00:00:00.000Z",
      };
      updateItem({
        ...fixture.item,
        id: TurnItemId.make("item:queue-limit"),
        type: "error",
        status: "failed",
        title: "Usage limit",
        failure,
      } as Extract<OrchestrationV2TurnItem, { type: "error" }>);
      extraEvents.push({
        ...common,
        id: EventId.make("queue:session-error"),
        type: "provider-session.attached",
        payload: {
          ...fixture.session,
          lastError:
            testCase === "session-error-invalidates-usage-limit"
              ? "A different session error"
              : failure.message,
        },
      });
    }
    if (testCase === "disabled-provider-before-missing-state") disableProvider = true;
    if (testCase === "runtime-ceiling-before-missing-state")
      updateThread({ ...fixture.thread, runtimeMode: "full-access" });
    if (testCase.startsWith("restart-background-")) {
      updateThread({
        ...fixture.thread,
        modelSelection: { ...fixture.thread.modelSelection, model: "new-picker-before-old-queue" },
      });
      extraEvents.push({
        ...common,
        id: EventId.make("queue:shared-background-provider"),
        type: "provider-thread.updated",
        payload: {
          ...fixture.providerThread,
          id: ProviderThreadId.make("provider:queue-shared-child"),
          ownerNodeId: fixture.history[0]!.root.id,
          nativeThreadRef: {
            driver: fixture.providerThread.driver,
            nativeId: "shared-child-native",
            strength: "strong",
          },
        },
      });
    }
    if (testCase.startsWith("rebase-") || testCase.startsWith("no-rebase-")) {
      const ahead = makeRunRecords(threadId, 90, "completed", now, fixture.providerThread.id);
      if (testCase === "no-rebase-newer-cancelled-without-prompt") {
        yield* persistRun(
          {
            ...ahead,
            run: { ...ahead.run, status: "cancelled", startedAt: null },
            item: {
              ...ahead.item,
              type: "assistant_message",
              streaming: false,
              text: "No prompt delivered",
            } as OrchestrationV2TurnItem,
          },
          now,
        );
      } else {
        yield* persistRun(
          {
            ...ahead,
            item:
              testCase === "no-rebase-newer-assistant"
                ? ({
                    ...ahead.item,
                    type: "assistant_message",
                    streaming: false,
                    text: "Only assistant content",
                  } as OrchestrationV2TurnItem)
                : testCase === "rebase-newer-notification"
                  ? ({
                      ...ahead.item,
                      type: "notification",
                      source: { kind: "monitor" },
                      outcome: "updated",
                      summary: "Started ahead",
                      detail: "Newer notification",
                    } as OrchestrationV2TurnItem)
                  : ahead.item,
          },
          now,
        );
      }
    }
    if (testCase.startsWith("picker-changed-")) {
      updateThread({
        ...fixture.thread,
        modelSelection: { ...fixture.thread.modelSelection, model: "picker-new-model" },
      });
      if (testCase !== "picker-changed-ordinary-retains-model")
        rows = rows.map((row) =>
          row.run.id === queued.run.id
            ? {
                ...row,
                message: {
                  ...row.message,
                  createdBy: "agent",
                  creationSource: testCase.includes("buffered") ? "provider" : "server",
                  notification: {
                    source: { kind: "monitor" },
                    outcome: "updated",
                    summary: "Monitor has new output",
                  },
                },
              }
            : row,
        );
    }
    if (testCase === "fresh-context-reserved-native")
      rows = rows.map((row) =>
        row.run.id === queued.run.id ? { ...row, run: { ...row.run, freshContext: true } } : row,
      );
    if (
      [
        "reserved-before-fresh-boundary",
        "cancelled-unstarted-restart-is-boundary",
        "cancelled-unstarted-ordinary-is-not-boundary",
      ].includes(testCase)
    ) {
      const boundary = makeRunRecords(threadId, 80, "completed", now, fixture.providerThread.id);
      yield* persistRun(
        {
          ...boundary,
          run: {
            ...boundary.run,
            freshContext: true,
            status: testCase === "reserved-before-fresh-boundary" ? "completed" : "cancelled",
            startedAt: testCase === "reserved-before-fresh-boundary" ? now : null,
            ...(testCase === "cancelled-unstarted-restart-is-boundary"
              ? { restartContinuationOfRunId: fixture.run.id }
              : {}),
          },
          item: {
            ...boundary.item,
            type: "assistant_message",
            streaming: false,
            text: "Boundary has no user prompt",
          } as OrchestrationV2TurnItem,
        },
        now,
      );
      if (testCase === "reserved-before-fresh-boundary") {
        const activeId = ProviderThreadId.make("provider:queue-active-after-boundary");
        updateThread({ ...fixture.thread, activeProviderThreadId: activeId });
        extraEvents.push({
          ...common,
          id: EventId.make("queue:active-after-boundary"),
          type: "provider-thread.updated",
          payload: {
            ...fixture.providerThread,
            id: activeId,
            firstRunOrdinal: 80,
            lastRunOrdinal: 80,
            nativeThreadRef: {
              driver: fixture.providerThread.driver,
              nativeId: "new-context-after-fresh",
              strength: "strong",
            },
          },
        });
      }
    }
    if (
      testCase === "handoff-latest-instance-differs-from-picker" ||
      testCase.startsWith("last-delivered-")
    ) {
      const sourceProviderId = ProviderThreadId.make("provider:queue-other-instance");
      const otherInstanceId = ProviderInstanceId.make("codex-other");
      const sourceModel = { instanceId: otherInstanceId, model: "source-other-model" };
      const source = makeRunRecords(threadId, 12, "completed", now, sourceProviderId);
      extraEvents.push({
        ...common,
        id: EventId.make("queue:other-provider"),
        type: "provider-thread.updated",
        payload: {
          ...fixture.providerThread,
          id: sourceProviderId,
          providerInstanceId: otherInstanceId,
          providerSessionId: null,
          nativeThreadRef: {
            driver: fixture.providerThread.driver,
            nativeId: "queue-other-native",
            strength: "strong",
          },
          firstRunOrdinal: 12,
          lastRunOrdinal: 12,
        },
      });
      yield* persistRun(
        {
          ...source,
          run: { ...source.run, modelSelection: sourceModel, providerInstanceId: otherInstanceId },
          attempt: { ...source.attempt, providerInstanceId: otherInstanceId },
          message: { ...source.message, text: "Source provider changed the architecture" },
          item: { ...source.item, text: "Source provider changed the architecture" },
        },
        now,
      );
      if (testCase.startsWith("last-delivered-")) {
        const status = testCase.includes("interrupted") ? "interrupted" : "failed";
        const delivered = makeRunRecords(threadId, 10, status, now, fixture.providerThread.id);
        const nullAttempt = testCase === "last-delivered-null-attempt-membership";
        yield* persistRun(
          {
            ...delivered,
            run: {
              ...delivered.run,
              activeAttemptId: nullAttempt ? null : delivered.run.activeAttemptId,
            },
            turn: {
              ...delivered.turn,
              runAttemptId: nullAttempt
                ? null
                : testCase === "last-delivered-failed-without-membership"
                  ? RunAttemptId.make("attempt:not-delivered")
                  : delivered.run.activeAttemptId,
            },
            item: { ...delivered.item, text: "Target provider already saw this delta" },
          },
          now,
        );
      }
    }
    if (testCase === "runless-legacy-import") {
      updateThread({ ...fixture.thread, historyOrigin: "v1_import" });
      updateItem({
        ...fixture.item,
        id: TurnItemId.make("item:runless-import-user"),
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        ordinal: 3,
        text: "Imported user requirement",
      });
      updateItem({
        ...fixture.item,
        id: TurnItemId.make("item:runless-import-assistant"),
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        ordinal: 4,
        type: "assistant_message",
        streaming: false,
        text: "Imported assistant reasoning",
      });
      extraEvents.push({
        ...common,
        id: EventId.make("queue:import-unloaded-provider"),
        type: "provider-thread.updated",
        payload: {
          ...fixture.providerThread,
          nativeThreadRef: null,
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          providerSessionId: null,
        },
      });
    }
    if (testCase.startsWith("shown-model-")) {
      const previous = fixture.history[0]!;
      const candidate: OrchestrationV2Run = {
        ...previous.run,
        modelSelection: { ...previous.run.modelSelection, model: "previous-shown-model" },
        completedAt: DateTime.add(now, { seconds: 30 }),
      };
      if (testCase === "shown-model-completion-tie-ordinal")
        updateRun({ ...candidate, completedAt: fixture.run.completedAt });
      else if (testCase === "shown-model-cancelled-unstarted-infinity")
        updateRun({ ...candidate, status: "cancelled", startedAt: null, completedAt: null });
      else if (testCase === "shown-model-shared-message-foreign-run-link") {
        updateRun({ ...candidate, userMessageId: fixture.history[1]!.message.id });
        updateItem({ ...previous.item, text: "/compact" });
        updateItem({
          ...fixture.history[1]!.item,
          runId: fixture.run.id,
          text: "Visible link belongs to a different run",
        });
      } else if (testCase === "shown-model-handoff-only") {
        updateRun({
          ...candidate,
          contextHandoffId: ContextHandoffId.make("handoff:old-shown-model"),
        });
        updateItem({ ...previous.item, text: "/compact" });
      } else updateRun(candidate);
    }
    for (const row of rows) yield* persistRun(row, now);
    yield* persistOracleEvents(extraEvents);
    const commandId = CommandId.make(`command:queue-oracle:${testCase}`);
    if (explicit && replay) {
      const sink = yield* EventSink.EventSinkV2;
      yield* sink.commitCommand({
        commandId,
        threadId,
        commandType: "queued-run.start",
        acceptedAt: now,
        events: [
          {
            ...common,
            id: EventId.make("queue:replayed-receipt"),
            type: "thread.metadata-updated",
            payload: fixture.thread,
          },
        ],
        effects: [],
      });
    }
    return { ...fixture, commandId, explicit, explicitRunId, disableProvider };
  });

// Original-flow behavior oracle from 4c085208fe: these snapshots stay fixed after staged reads land.
it.effect.each(cases)("queued-start preserves the complete transcript: %s", (testCase) =>
  Effect.gen(function* () {
    const fixture = yield* seedQueuedCase(testCase);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const restores: Array<{ mockRestore: () => void }> = [];
    if (testCase.endsWith("after-preflight")) {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const original = store.canStartQueuedRun;
      let preflights = 0;
      restores.push(
        vi.spyOn(store, "canStartQueuedRun").mockImplementation((threadId) =>
          original(threadId).pipe(
            Effect.tap((ready) => {
              if (!ready || ++preflights !== 2) return Effect.void;
              const common = {
                threadId: fixture.threadId,
                occurredAt: fixture.now,
                id: EventId.make("queue:after-preflight"),
              };
              return persistOracleEvents(
                testCase === "live-after-preflight"
                  ? [
                      {
                        ...common,
                        type: "run.updated",
                        payload: { ...fixture.run, status: "waiting", completedAt: null },
                      },
                    ]
                  : [
                      {
                        ...common,
                        type: "thread.metadata-updated",
                        payload: {
                          ...fixture.thread,
                          ...(testCase === "archived-after-preflight"
                            ? { archivedAt: fixture.now }
                            : { deletedAt: fixture.now }),
                        },
                      },
                    ],
              ).pipe(Effect.provideService(ProjectionStore.ProjectionStoreV2, store));
            }),
          ),
        ),
      );
    }
    if (testCase.startsWith("restart-background-")) {
      const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const adapter = yield* registry.get(fixture.thread.providerInstanceId);
      restores.push(
        vi
          .spyOn(adapter, "planSelectionTransition")
          .mockImplementation(() => Effect.succeed({ type: "restart_session" })),
      );
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const runtime = {
        instanceId: fixture.thread.providerInstanceId,
        driver: fixture.providerThread.driver,
        providerSessionId: fixture.session.id,
        providerSession: fixture.session,
        events: Stream.empty,
        hasPendingBackgroundWorkForThread: (providerThread: typeof fixture.providerThread) =>
          testCase === "restart-background-probe-failure"
            ? Effect.fail("Background probe unavailable")
            : Effect.succeed(
                testCase === "restart-background-second-busy" &&
                  providerThread.id === "provider:queue-shared-child",
              ),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      restores.push(
        vi.spyOn(sessions, "get").mockImplementation(() => Effect.succeedSome(runtime)),
      );
    }
    const capture = yield* startOracleCapture();
    yield* Effect.gen(function* () {
      const effect: Effect.Effect<unknown, Orchestrator.OrchestratorV2Error> = fixture.explicit
        ? orchestrator.dispatch({
            type: "queued-run.start",
            commandId: fixture.commandId,
            threadId: fixture.threadId,
            runId: fixture.explicitRunId!,
          })
        : orchestrator.resumeQueuedRuns;
      const result = yield* effect.pipe(Effect.result);
      capture.transcript.push(
        Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
      );
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const after = yield* store.getThreadProjection(fixture.threadId);
      const started = after.runs.filter((run) => run.status === "starting" && run.ordinal >= 20);
      const mustWait =
        [
          "empty",
          "all-held",
          "archived",
          "deleted",
          "explicit-held-no-admitting-run",
          "explicit-missing",
          "explicit-terminal",
          "assistant-message",
          "usage-limit-before-missing-state",
          "disabled-provider-before-missing-state",
          "runtime-ceiling-before-missing-state",
          "restart-background-second-busy",
          "restart-background-probe-failure",
        ].includes(testCase) ||
        testCase.startsWith("live-") ||
        testCase.endsWith("after-preflight") ||
        testCase.startsWith("missing-") ||
        testCase.startsWith("null-");
      assert.lengthOf(started, mustWait ? 0 : 1);
      if (!mustWait) {
        assert.equal(
          started[0]!.id,
          RunId.make(
            `run:${fixture.threadId}:${["default-order-gaps", "explicit-choice", "explicit-held-choice", "delegated-priority"].includes(testCase) ? 30 : 20}`,
          ),
        );
      }
      if (testCase.startsWith("rebase-")) assert.equal(started[0]!.ordinal, 91);
      if (testCase.startsWith("no-rebase-")) assert.equal(started[0]!.ordinal, 20);
      if (testCase.startsWith("last-delivered-")) {
        assert.lengthOf(after.contextTransfers, 1);
        assert.equal(
          after.contextTransfers[0]!.basePoint?.runId,
          RunId.make(
            `run:${fixture.threadId}:${testCase === "last-delivered-failed-without-membership" ? 7 : 10}`,
          ),
        );
      }
      if (
        testCase === "handoff-latest-instance-differs-from-picker" ||
        testCase === "runless-legacy-import"
      )
        assert.lengthOf(after.contextHandoffs, 1);
      if (
        testCase.startsWith("restart-background-") &&
        testCase !== "restart-background-probes-clear"
      )
        assert.isTrue(
          after.runs.filter((run) => run.status === "queued").every((run) => run.queueHeld),
        );
      expect(capture.snapshot()).toMatchSnapshot();
    }).pipe(
      Effect.ensuring(capture.stop),
      Effect.ensuring(Effect.sync(() => restores.toReversed().forEach((spy) => spy.mockRestore()))),
    );
  }).pipe(
    Effect.provide(
      makeOracleLayer(
        testCase === "disabled-provider-before-missing-state"
          ? {
              projectSettingsOverrides: {
                [ProjectId.make("oracle-project")]: {
                  providerInstances: { [ProviderInstanceId.make("codex")]: { enabled: false } },
                },
              },
            }
          : undefined,
        testCase === "runtime-ceiling-before-missing-state"
          ? {
              organization: "Oracle",
              lockedSettings: [],
              limits: { maxRuntimeMode: "approval-required" },
            }
          : undefined,
      ),
    ),
  ),
);

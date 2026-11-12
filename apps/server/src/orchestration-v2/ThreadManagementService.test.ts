import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderThread,
  ProviderThreadId,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadProjection,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { AgentScope } from "@t3tools/shared/AgentScope";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ReadFixture from "./SendContinuationReads.testkit.ts";

it("stamps authoritative provenance on commands that create threads or messages", () => {
  const command: OrchestrationV2Command = {
    type: "thread.create",
    createdBy: "agent",
    creationSource: "mcp",
    commandId: CommandId.make("command:thread-management:create"),
    threadId: ThreadId.make("thread:thread-management:create"),
    projectId: ProjectId.make("project:thread-management"),
    title: "Thread management",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    // A client cannot claim a linked environment's origin, or a parent there.
    linkOrigin: { sessionId: "forged", label: "Somewhere else" },
    delegatedFrom: {
      environmentId: EnvironmentId.make("environment:elsewhere"),
      threadId: ThreadId.make("thread:elsewhere"),
      title: "Somewhere else",
    },
  };

  const stamped = ThreadManagementService.withCreationProvenance(command, {
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).toMatchObject({
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).not.toHaveProperty("linkOrigin");
  expect(stamped).not.toHaveProperty("delegatedFrom");
});

const layerReceiptsTest = CommandReceiptStore.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);

const layerDispatchChokepointTest =
  (settingsLayer: ReturnType<typeof ServerSettings.layerTest>) => (dispatched: Array<string>) =>
    ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: ThreadId.make("thread:thread-management:chokepoint"),
                projectId: ProjectId.make("project:thread-management:chokepoint"),
                deletedAt: null,
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex_work"),
                  model: "test",
                },
              },
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(settingsLayer),
      Layer.provideMerge(layerReceiptsTest),
    );

it.effect(
  "the dispatch chokepoint refuses a continuation message.dispatch on a project-disabled instance before reaching the orchestrator",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-disabled");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const error = yield* Effect.flip(
        threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("message:thread-management:chokepoint-disabled"),
          text: "Background task completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        }),
      );

      expect(error).toBeInstanceOf(
        ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
      );
      expect(error).toMatchObject({ projectId, instanceId });
      expect(dispatched).toEqual([]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect(
  "a replay of an already-accepted command id dispatches despite the instance now being disabled",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-replay");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: yield* DateTime.now,
        resultSequence: 1,
        status: "accepted",
        error: null,
      });

      const threads = yield* ThreadManagementService.ThreadManagementService;
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId,
        threadId,
        messageId: MessageId.make("message:thread-management:chokepoint-replay"),
        text: "Background task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      });

      expect(dispatched).toEqual(["message.dispatch"]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect("waitForThread follows a restart continuation instead of the cut run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-restart");
    const threadId = ThreadId.make("thread:thread-management:wait-restart");
    const runId = RunId.make("run:thread-management:wait-restart");
    const continuationId = RunId.make("run:thread-management:wait-restart:2");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      restartContinuationOfRunId?: RunId;
    }> = [{ id: runId, status: "cancelled" }];
    let pending = true;
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: (run) => Effect.succeed(pending && run.id === runId),
          awaitRestartContinuation: () => Effect.never,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    const continuation = { id: continuationId, restartContinuationOfRunId: runId };
    runs = [...runs, { ...continuation, status: "running" }];
    pending = false;
    yield* Queue.offer(
      events,
      stored(1, { type: "run.updated", payload: { ...continuation, status: "running" } }),
    );
    runs = [runs[0]!, { ...continuation, status: "completed" }];
    yield* Queue.offer(
      events,
      stored(2, { type: "run.updated", payload: { ...continuation, status: "completed" } }),
    );
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: continuationId, status: "completed" },
    });
  }),
);

it.effect("waitForThread can wait past an ended run for the thread's delegated work", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-delegated");
    const threadId = ThreadId.make("thread:thread-management:wait-delegated");
    const runId = RunId.make("run:thread-management:wait-delegated");
    const wakeRunId = RunId.make("run:thread-management:wait-delegated:wake");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const at = (seconds: number) => DateTime.makeUnsafe(seconds * 1_000);
    const run = (id: RunId, ordinal: number, status: OrchestrationV2Run["status"], end: number) =>
      ({
        id,
        ordinal,
        status,
        startedAt: at(end - 1),
        completedAt: status === "running" ? null : at(end),
      }) as unknown as OrchestrationV2Run;
    // The run ended, but a task it delegated still runs and will wake the thread.
    let runs = [run(runId, 1, "completed", 1)];
    let subagents = [{ status: "running" }];
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "subagent.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [],
                  subagents,
                  providerThreads: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    // Without the option, the ended run is the answer, as before.
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).toMatchObject({ timedOut: false, run: { id: runId } });
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).not.toHaveProperty("resultRun");

    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60 * 60 * 1_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // A wait that returns early fails below instead of hanging here.
    yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
    // The task finishes; its delivery wakes the thread, whose next run answers.
    subagents = [{ status: "completed" }];
    runs = [...runs, run(wakeRunId, 2, "completed", 3)];
    yield* Queue.offer(events, stored(1, { type: "subagent.updated", payload: {} }));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "completed" },
      resultRun: { id: wakeRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread settles delegated work on a thread rolled back past its only run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-rolled-back");
    const threadId = ThreadId.make("thread:thread-management:wait-rolled-back");
    const runId = RunId.make("run:thread-management:wait-rolled-back");
    const rolledBack = {
      id: runId,
      ordinal: 1,
      status: "rolled_back",
      startedAt: DateTime.makeUnsafe(0),
      completedAt: DateTime.makeUnsafe(1_000),
    } as unknown as OrchestrationV2Run;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [rolledBack],
              messages: [],
              subagents: [],
              providerThreads: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // Nothing will ever run there again, so a wait still open runs out here.
    yield* TestClock.adjust(Duration.millis(60_000));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "rolled_back" },
      resultRun: { id: runId, status: "rolled_back" },
    });
  }),
);

it.effect("waitForThread returns the cut run once its continuation is declined", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-declined");
    const threadId = ThreadId.make("thread:thread-management:wait-declined");
    const runId = RunId.make("run:thread-management:wait-declined");
    const subscribed = yield* Deferred.make<void>();
    const declined = yield* Deferred.make<void>();
    let pending = true;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(pending),
          awaitRestartContinuation: () => Deferred.await(declined),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.never),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [{ id: runId, status: "cancelled" }],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    pending = false;
    yield* Deferred.succeed(declined, undefined);
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({ timedOut: false, run: { id: runId, status: "cancelled" } });
  }),
);

it.effect.each([
  "live",
  "no-match",
  "no-match-clear",
  "no-session",
  "unsupported-runtime",
] as const)(
  "marks background work awaited on the thread's active provider thread with state %s",
  (state) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:background-wait");
      const providerThreadId = ProviderThreadId.make("provider-thread:background-wait");
      const providerSessionId = ProviderSessionId.make("provider-session:background-wait");
      const driver = ProviderDriverKind.make("claudeAgent");
      const now = yield* DateTime.now;
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [{ taskId: "tests", kind: "command" }],
        contextUsage: null,
        nativeMetadata: null,
        createdAt: now,
        updatedAt: now,
      };
      const calls: Array<{ providerThreadId: ProviderThreadId; awaited: boolean }> = [];
      const runtime = {
        driver,
        providerSessionId,
        ...(state === "unsupported-runtime"
          ? {}
          : {
              awaitBackgroundTasks: (input: {
                readonly providerThread: OrchestrationV2ProviderThread;
                readonly taskIds: ReadonlyArray<string>;
                readonly awaited: boolean;
              }) =>
                Effect.sync(() => {
                  calls.push({
                    providerThreadId: input.providerThread.id,
                    awaited: input.awaited,
                  });
                  return { matched: input.taskIds.filter((taskId) => taskId === "tests") };
                }),
            }),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              getThreadRecords: () =>
                Effect.succeed({
                  thread: {
                    id: threadId,
                    deletedAt: null,
                    activeProviderThreadId: providerThreadId,
                  },
                  providerThreads: [providerThread],
                } as unknown as OrchestrationV2ThreadProjection),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(state === "no-session" ? Option.none() : Option.some(runtime)),
            }),
          ),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const result = yield* Effect.gen(function* () {
        const service = yield* ThreadManagementService.ThreadManagementService;
        return yield* service
          .awaitBackgroundTasks({
            threadId,
            taskIds: state.startsWith("no-match") ? ["finished"] : ["tests", "finished"],
            awaited: state !== "no-match-clear",
          })
          .pipe(Effect.result);
      }).pipe(Effect.provide(Layer.merge(layerTest, ThreadCommandExecutor.layer)));

      if (state === "live") {
        expect(result).toMatchObject({
          success: { matched: ["tests"], unmatched: ["finished"] },
        });
        expect(calls).toEqual([{ providerThreadId, awaited: true }]);
      } else if (state === "no-match") {
        expect(result).toMatchObject({
          failure: { _tag: "ThreadManagementBackgroundWaitError", reason: "unmatched" },
        });
        expect(calls).toEqual([{ providerThreadId, awaited: true }]);
      } else if (state === "no-match-clear") {
        expect(result).toMatchObject({ success: { matched: [], unmatched: ["finished"] } });
        expect(calls).toEqual([{ providerThreadId, awaited: false }]);
      } else {
        expect(result).toMatchObject({
          failure: {
            _tag: "ThreadManagementBackgroundWaitError",
            reason: state === "no-session" ? "unavailable" : "unsupported",
          },
        });
        expect(calls).toEqual([]);
      }
    }),
);

it.effect.each([
  { status: "completed" as const, followUp: "none", settles: true },
  { status: "failed" as const, followUp: "none", settles: false },
  { status: "interrupted" as const, followUp: "none", settles: false },
  { status: "completed" as const, followUp: "notification", settles: true },
  { status: "completed" as const, followUp: "replayed-notification", settles: true },
  { status: "completed" as const, followUp: "delegated", settles: true },
  { status: "completed" as const, followUp: "failed-notification", settles: false },
  { status: "completed" as const, followUp: "held-notification", settles: true },
  { status: "completed" as const, followUp: "user", settles: false },
  { status: "completed" as const, followUp: "steered-notification", settles: false },
])("settleAfterRun handles $status with $followUp follow-up", ({ status, followUp, settles }) =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:settle-after-run");
    const threadId = ThreadId.make("thread:thread-management:settle-after-run");
    const runId = RunId.make("run:thread-management:settle-after-run");
    const followUpId = RunId.make(`${runId}:follow-up`);
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const replayRejected = yield* Deferred.make<void>();
    let sequence = 0;
    let sequenceReads = 0;
    let rejectedCommandId: CommandId | undefined;
    const runs = [{ id: runId, ordinal: 1, status, userMessageId: MessageId.make("original") }];
    const nextRun = {
      id: followUpId,
      ordinal: 2,
      status: "running" as OrchestrationV2Run["status"],
      userMessageId: MessageId.make("follow-up"),
    };
    const dispatched: Array<string> = [];
    // The notification starts after the completed run was observed, before settlement commits.
    let raced = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () =>
            Effect.sync(() => {
              if (followUp === "replayed-notification" && sequenceReads++ === 2) sequence = 1;
              return sequence;
            }),
          restartContinuationPending: () => Effect.succeed(false),
          streamStoredEventsFrom: () =>
            Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.fromQueue(events)),
            ),
          getThreadRecords: (_threadId, _fields, filter) =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs: raced ? [...runs, nextRun] : runs,
                  messages: [
                    { id: MessageId.make("original"), runId, role: "user" as const },
                    {
                      id: nextRun.userMessageId,
                      runId: followUpId,
                      role: "user" as const,
                      ...(followUp.endsWith("notification") ? { notification: {} } : {}),
                      ...(followUp === "delegated" ? { delegatedCompletion: {} } : {}),
                    },
                    { id: MessageId.make("reply"), runId: followUpId, role: "assistant" as const },
                    ...(followUp === "steered-notification"
                      ? [
                          {
                            id: MessageId.make("user-steer"),
                            runId: followUpId,
                            role: "user" as const,
                          },
                        ]
                      : []),
                  ].filter(
                    (message) =>
                      (filter?.messageIds === undefined ||
                        filter.messageIds.includes(message.id)) &&
                      (filter?.messageRunIds === undefined ||
                        filter.messageRunIds.includes(message.runId)) &&
                      (filter?.messageRoles === undefined ||
                        filter.messageRoles.includes(message.role)),
                  ),
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
          dispatch: (command) =>
            Effect.gen(function* () {
              if (followUp !== "none" && !raced) {
                raced = true;
                rejectedCommandId = command.commandId;
                return yield* new Orchestrator.OrchestratorDispatchError({
                  commandId: command.commandId,
                  commandType: command.type,
                  cause: "Thread is still running",
                });
              }
              if (followUp === "replayed-notification" && command.commandId === rejectedCommandId) {
                yield* Deferred.succeed(replayRejected, undefined);
                return yield* new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                  commandId: command.commandId,
                  commandType: command.type,
                  detail: "Thread is still running",
                });
              }
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .settleAfterRun({ projectId, threadId, runId })
      .pipe(Effect.forkChild);
    if (followUp !== "none") {
      yield* Deferred.await(subscribed);
      expect(dispatched).toEqual([]);
      nextRun.status =
        followUp === "failed-notification"
          ? "failed"
          : followUp === "held-notification"
            ? "queued"
            : "completed";
      yield* Queue.offer(events, {
        sequence: 1,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    if (followUp === "replayed-notification") {
      yield* Deferred.await(replayRejected);
      expect(dispatched).toEqual([]);
      sequence = 2;
      yield* Queue.offer(events, {
        sequence,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    yield* Fiber.join(fiber);
    expect(dispatched).toEqual(settles ? ["thread.settle"] : []);
  }),
);

it.effect.each([
  {
    name: "kills the thread's own agent and ends its turn",
    others: [] as ReadonlyArray<string>,
    killed: true,
    expected: { outcome: "killed", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
  {
    name: "keeps a process another working thread shares and only ends this turn",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "still settles a finished run's background work in a shared process",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    settledRun: true,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "detaches when a sibling starts a turn just before the session is released",
    others: [] as ReadonlyArray<string>,
    killed: false,
    siblingBusy: true,
    expected: { outcome: "shared", sharedWithThreadIds: [] },
    teardown: ["release", "detach"],
  },
  {
    name: "reports a teardown that has not finished in time as still releasing",
    others: [] as ReadonlyArray<string>,
    killed: false,
    releaseHangs: true,
    expected: { outcome: "releasing", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
])("Force stop $name", ({ others, killed, expected, teardown, ...row }) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop");
    const providerSessionId = ProviderSessionId.make("provider-session:force-stop");
    const providerThreadId = ProviderThreadId.make("provider-thread:force-stop");
    const runId = RunId.make("run:thread-management:force-stop");
    const dispatched: Array<OrchestrationV2Command | { readonly type: string }> = [];
    const teardowns: Array<string> = [];
    const tornDown = yield* Deferred.make<void>();
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          isLive: () => Effect.succeed(true),
          otherWorkingThreads: () => Effect.succeed(others.map((id) => ThreadId.make(id))),
          releaseUnlessBusy: () =>
            Effect.sync(() => teardowns.push("release")).pipe(
              Effect.andThen(
                "releaseHangs" in row ? Effect.never : Effect.succeed(!("siblingBusy" in row)),
              ),
            ),
          detach: () =>
            Effect.sync(() => teardowns.push("detach")).pipe(
              Effect.andThen(Deferred.succeed(tornDown, undefined)),
              Effect.asVoid,
            ),
        }),
      ),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                {
                  id: runId,
                  ordinal: 1,
                  status: "settledRun" in row ? "completed" : "running",
                  activeAttemptId: "attempt",
                },
                // A queued follow-up neither supersedes the request nor becomes the target.
                { id: RunId.make("run:queued-follow-up"), ordinal: 2, status: "queued" },
              ],
              providerThreads: [{ id: providerThreadId, appThreadId: threadId, providerSessionId }],
              providerTurns: [
                {
                  id: "provider-turn:force-stop",
                  providerThreadId,
                  runAttemptId: "attempt",
                  status: "settledRun" in row ? "completed" : "running",
                },
              ],
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.succeed(killed),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const stopping = yield* service.forceStopThread({ threadId, runId }).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      expect(yield* Fiber.join(stopping)).toEqual(expected);
      expect(dispatched).toMatchObject([
        {
          type: "thread.background-work.settle",
          providerThreadId,
          reason: "Force stopped by user",
        },
      ]);
      if (teardown.at(-1) === "detach") yield* Deferred.await(tornDown);
      expect(teardowns).toEqual(teardown);
    }).pipe(Effect.provide(layerTest));
  }),
);

it.effect("Force stop refuses once a newer run has started, before killing anything", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop-superseded");
    let killed = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                { id: RunId.make("run:stopped"), ordinal: 1, status: "running" },
                { id: RunId.make("run:newer"), ordinal: 2, status: "running" },
              ],
              providerThreads: [],
              providerTurns: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.sync(() => (killed = true)),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const error = yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      return yield* Effect.flip(
        service.forceStopThread({ threadId, runId: RunId.make("run:stopped") }),
      );
    }).pipe(Effect.provide(layerTest));
    expect(error.reason).toBe("superseded");
    expect(killed).toBe(false);
  }),
);

it.effect(
  "admits two transcript exports at a time and frees a slot on failure or interruption",
  () =>
    Effect.gen(function* () {
      const started = yield* Queue.unbounded<string>();
      const gates = new Map(
        ["a", "b", "c", "d", "e", "f"].map((id) => [
          id,
          Deferred.makeUnsafe<void, Orchestrator.OrchestratorV2Error>(),
        ]),
      );
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getTranscriptByteFloor: () => Effect.succeed(0),
            getThreadSnapshot: (threadId) =>
              Queue.offer(started, threadId).pipe(
                Effect.andThen(Deferred.await(gates.get(threadId)!)),
                Effect.as({
                  schemaVersion: 1,
                  snapshotSequence: 1,
                  projection: {
                    thread: { id: threadId, title: threadId },
                    updatedAt: DateTime.makeUnsafe(0),
                    visibleTurnItems: [],
                  } as unknown as OrchestrationV2ThreadProjection,
                }),
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );

      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const exportThread = (id: string) =>
          threads.getThreadTranscript(ThreadId.make(id)).pipe(
            Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, {
              getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:export")),
            }),
            Effect.forkChild({ startImmediately: true }),
          );
        const a = yield* exportThread("a");
        const b = yield* exportThread("b");
        const c = yield* exportThread("c");
        const d = yield* exportThread("d");
        const e = yield* exportThread("e");
        const f = yield* exportThread("f");
        expect(yield* Queue.takeAll(started)).toEqual(["a", "b"]);

        yield* Fiber.interrupt(c);
        yield* Deferred.fail(
          gates.get("a")!,
          new Orchestrator.OrchestratorProjectionError({ threadId: ThreadId.make("a"), cause: "" }),
        );
        expect(yield* Queue.take(started)).toBe("d");
        yield* Fiber.interrupt(b);
        expect(yield* Queue.take(started)).toBe("e");
        yield* Deferred.succeed(gates.get("d")!, undefined);
        expect(yield* Queue.take(started)).toBe("f");
        yield* Deferred.succeed(gates.get("e")!, undefined);
        yield* Deferred.succeed(gates.get("f")!, undefined);

        expect((yield* Fiber.await(a))._tag).toBe("Failure");
        expect((yield* Fiber.join(d)).threadId).toBe("d");
        expect((yield* Fiber.join(e)).threadId).toBe("e");
        expect((yield* Fiber.join(f)).threadId).toBe("f");
        expect(yield* Queue.size(started)).toBe(0);
      }).pipe(Effect.provide(layerTest));
    }),
);

const sendReadCases = [
  ...(["auto", "queue", "steer", "restart"] as const).flatMap((mode) =>
    [false, true].flatMap((freshContext) =>
      ["running", "idle"].map((state) => ({
        name: `${mode}/${freshContext ? "fresh" : "current"}/${state}`,
        mode,
        freshContext,
        state,
      })),
    ),
  ),
  ...[
    "queued-without-item",
    "queued-item",
    "started-item",
    "duplicate-items",
    "equal-ordinal-items",
    "binary-id-tie",
    "missing-message",
    "runless-message",
    "missing-run",
    "missing-item",
    "preflight-race",
    "concurrent-send",
    "replayed-send",
    "archived",
    "deleted",
    "wrong-project",
    "null-attempt",
    "completed-turn",
  ].map((state) => ({ name: state, mode: "auto" as const, freshContext: false, state })),
];

it.effect.each(sendReadCases)(
  "send reads retain main's result for $name",
  ({ mode, freshContext, state }) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const { store, threadId, projectId } = fixture;
      const selected = fixture.run(21, {
        status: "running",
        completedAt: null,
        ...(state === "null-attempt" ? { activeAttemptId: null } : {}),
      });
      if (state !== "idle") {
        const older = fixture.run(20, { status: "running", completedAt: null });
        yield* fixture.putRun(older);
        yield* fixture.putTurn(fixture.turn(older, 7, "running"));
        yield* fixture.putRun(selected);
        yield* fixture.putTurn(
          fixture.turn(selected, 8, state === "completed-turn" ? "completed" : "running"),
        );
        const newer = fixture.run(22, { status: "running", completedAt: null });
        yield* fixture.putRun(newer);
        yield* fixture.putTurn(fixture.turn(newer, 9, "completed"));
      }
      if (state === "archived" || state === "deleted") {
        yield* fixture.putThread({
          ...fixture.thread,
          ...(state === "archived" ? { archivedAt: fixture.now } : { deletedAt: fixture.now }),
        });
      }
      const broadPreflight = yield* store.getThreadRecords(threadId, ["runs", "providerTurns"]);
      const steerable = broadPreflight.runs
        .filter(
          (run) =>
            run.status === "running" &&
            run.activeAttemptId !== null &&
            broadPreflight.providerTurns.some(
              (turn) => turn.runAttemptId === run.activeAttemptId && turn.status === "running",
            ),
        )
        .toSorted((left, right) => right.ordinal - left.ordinal)[0];
      const effectiveMode =
        freshContext && (mode === "steer" || mode === "restart") ? "queue" : mode;
      const messageId = MessageId.make("sent-message");
      const commands: Array<OrchestrationV2ServerCommand> = [];
      let dispatched = false;
      const sentSql = ReadFixture.traceSql();
      const serviceLayer = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadRecords: (id, fields, filter) =>
              store.getThreadRecords(id, fields, filter).pipe(Effect.orDie),
            getSentMessageState: (id, sentMessageId) =>
              store
                .getSentMessageState(id, sentMessageId)
                .pipe(Effect.orDie, Effect.withTracer(sentSql.tracer)),
            dispatch: (command) =>
              Effect.gen(function* () {
                commands.push(command);
                if (!dispatched) {
                  dispatched = true;
                  if (state === "preflight-race") {
                    yield* fixture.putRun({
                      ...selected,
                      status: "completed",
                      completedAt: fixture.now,
                    });
                  }
                  const delivered = fixture.run(30, {
                    status: state === "queued-without-item" ? "queued" : "running",
                    completedAt: null,
                  });
                  if (state !== "missing-run") yield* fixture.putRun(delivered);
                  if (state !== "missing-message") {
                    yield* fixture.putMessage(
                      fixture.message(messageId, state === "runless-message" ? null : delivered.id),
                    );
                  }
                  if (state !== "missing-item" && state !== "queued-without-item") {
                    yield* fixture.putItem(
                      fixture.item(
                        "sent-item",
                        40,
                        messageId,
                        delivered.id,
                        state === "queued-item"
                          ? "queued_turn"
                          : state === "started-item"
                            ? "turn_start"
                            : "steer",
                      ),
                    );
                  }
                  if (
                    state === "duplicate-items" ||
                    state === "equal-ordinal-items" ||
                    state === "binary-id-tie"
                  ) {
                    // Main selects the first matching item across all runs, including legacy runless items.
                    yield* fixture.putItem(
                      fixture.item("z-legacy-item", 0, messageId, fixture.run(2).id, "queued_turn"),
                    );
                    yield* fixture.putItem(
                      fixture.item(
                        state === "binary-id-tie" ? "ä-legacy-item" : "a-legacy-item",
                        state === "duplicate-items" ? 1 : 0,
                        messageId,
                        null,
                        "turn_start",
                      ),
                    );
                  }
                  if (state === "concurrent-send") {
                    const concurrent = fixture.run(31, { status: "running", completedAt: null });
                    yield* fixture.putRun(concurrent);
                    yield* fixture.putMessage(
                      fixture.message(concurrent.userMessageId, concurrent.id),
                    );
                    yield* fixture.putItem(
                      fixture.item(
                        "concurrent-item",
                        41,
                        concurrent.userMessageId,
                        concurrent.id,
                        "turn_start",
                      ),
                    );
                  }
                }
                return { sequence: 123, storedEvents: [] };
              }).pipe(Effect.orDie),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(
          Layer.mock(CommandReceiptStore.CommandReceiptStoreV2)({
            getByCommandId: () => Effect.succeedNone,
          }),
        ),
      );
      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const input = {
          projectId: state === "wrong-project" ? ProjectId.make("other-project") : projectId,
          threadId,
          commandId: CommandId.make("send-read-command"),
          messageId,
          text: "Send payload",
          attachments: [],
          mode,
          freshContext,
          createdBy: "agent" as const,
          creationSource: "mcp" as const,
        };
        const outcome = yield* threads.sendToThread(input).pipe(Effect.result);
        const expectedError =
          state === "wrong-project" || state === "deleted"
            ? "ThreadManagementThreadNotFoundError"
            : state === "archived"
              ? "ThreadManagementThreadArchivedError"
              : (effectiveMode === "steer" || effectiveMode === "restart") &&
                  steerable === undefined
                ? "ThreadManagementNoSteerableRunError"
                : ["missing-message", "runless-message", "missing-run", "missing-item"].includes(
                      state,
                    )
                  ? "ThreadManagementDurableRunProjectionError"
                  : undefined;
        if (expectedError !== undefined) {
          expect(outcome._tag).toBe("Failure");
          if (outcome._tag === "Failure") expect(outcome.failure._tag).toBe(expectedError);
          return;
        }
        expect(outcome._tag).toBe("Success");
        if (outcome._tag !== "Success") return;
        const result = outcome.success;
        const selects = sentSql.queries.filter(({ query }) => /^\s*SELECT\b/i.test(query));
        expect(selects.length).toBeGreaterThan(0);
        expect(selects.every(({ transaction }) => transaction !== null)).toBe(true);
        expect(new Set(selects.map(({ transaction }) => transaction)).size).toBe(1);
        // The original main read is the result oracle; no optimized-code snapshots are recorded.
        const broad = yield* store.getThreadRecords(threadId, ["runs", "messages", "turnItems"], {
          messageIds: [messageId],
          turnItemTypes: ["user_message"],
        });
        const message = broad.messages.find((candidate) => candidate.id === messageId)!;
        const run = broad.runs.find((candidate) => candidate.id === message.runId)!;
        const item =
          broad.turnItems.find(
            (candidate) => candidate.type === "user_message" && candidate.messageId === messageId,
          ) ?? null;
        expect(result.message).toEqual(message);
        expect(result.run).toEqual(run);
        expect(result.turnItem).toEqual(item);
        expect(result.projection.runs).toEqual([run]);
        expect(result.projection.messages).toEqual([message]);
        expect(
          result.projection.turnItems.every(
            (candidate) => candidate.type === "user_message" && candidate.messageId === messageId,
          ),
        ).toBe(true);
        const expectedDelivery =
          item === null || (item.type === "user_message" && item.inputIntent === "queued_turn")
            ? "queued"
            : item.type === "user_message" && item.inputIntent === "turn_start"
              ? "started"
              : effectiveMode === "restart"
                ? "restarted"
                : "steered";
        expect(result.delivery).toBe(expectedDelivery);
        expect(commands[0]).toMatchObject({
          type: "message.dispatch",
          messageId,
          dispatchMode:
            effectiveMode === "steer" || effectiveMode === "restart"
              ? {
                  type: effectiveMode === "steer" ? "steer_active" : "restart_active",
                  targetRunId: steerable!.id,
                }
              : effectiveMode === "auto" && steerable !== undefined
                ? { type: "steer_active", targetRunId: steerable.id }
                : { type: effectiveMode === "queue" ? "queue_after_active" : "start_immediately" },
          ...(effectiveMode === "auto" ? { deliveryIntent: "auto" } : {}),
          ...(freshContext ? { freshContext: true } : {}),
        });
        if (state === "replayed-send") {
          const replay = yield* threads.sendToThread(input);
          expect({
            message: replay.message,
            run: replay.run,
            turnItem: replay.turnItem,
            delivery: replay.delivery,
          }).toEqual({
            message: result.message,
            run: result.run,
            turnItem: result.turnItem,
            delivery: result.delivery,
          });
        }
      }).pipe(Effect.provide(serviceLayer));
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

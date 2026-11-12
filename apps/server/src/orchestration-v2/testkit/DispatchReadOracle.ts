import { vi } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointScopeId,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type RuntimeMode,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2TurnItem,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Option from "effect/Option";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "../Adapters/CodexAdapterV2.ts";
import * as CheckpointService from "../CheckpointService.ts";
import * as CommandPolicy from "../CommandPolicy.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as ContextHandoffService from "../ContextHandoffService.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "../ProviderSessionManager.ts";
import * as ProviderSwitchService from "../ProviderSwitchService.ts";
import * as RuntimePolicy from "../RuntimePolicy.ts";
import * as ThreadCommandExecutor from "../ThreadCommandExecutor.ts";
import * as ThreadForkService from "../ThreadForkService.ts";
import * as TurnItemPositionStore from "../TurnItemPositionStore.ts";

const instanceId = ProviderInstanceId.make("codex");
export const otherInstanceId = ProviderInstanceId.make("codex-other");
const modelSelection = { instanceId, model: "gpt-5.4" };
const driver = ProviderDriverKind.make("codex");

class AllocatedUuids extends Context.Service<AllocatedUuids, Set<string>>()(
  "t3/orchestration-v2/testkit/DispatchReadOracle/AllocatedUuids",
) {}

export const makeOracleLayer = (
  settings?: Parameters<typeof ServerSettings.layerTest>[0],
  managedPolicy?: Parameters<typeof ServerSettings.layerTest>[1],
) => {
  const database = SqlitePersistence.layerMemory;
  const stores = Layer.mergeAll(
    ProjectionStore.layer,
    ProjectStore.layer,
    EventStore.layer,
    CommandReceiptStore.layer,
    EffectOutbox.layer,
    TurnItemPositionStore.layer,
  ).pipe(Layer.provide(database));
  const persistence = Layer.mergeAll(
    stores,
    EventSink.layerFromStores.pipe(Layer.provide(stores), Layer.provide(database)),
  );
  const adapters = Layer.sync(ProviderAdapterRegistry.ProviderAdapterRegistryV2, () => {
    const values = [instanceId, otherInstanceId].map(
      (id) =>
        ({
          instanceId: id,
          driver,
          getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
          planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
          openSession: () => Effect.die("Oracle stops at the persisted provider effect"),
        }) as ProviderAdapter.ProviderAdapterV2["Service"],
    );
    return {
      get: (id) => {
        const value = values.find((adapter) => adapter.instanceId === id);
        return value
          ? Effect.succeed(value)
          : Effect.fail(
              new ProviderAdapterRegistry.ProviderAdapterRegistryLookupError({ instanceId: id }),
            );
      },
      list: () => Effect.succeed(values.map((adapter) => adapter.instanceId)),
    };
  });
  const externalRuntime = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
    get: () => Effect.succeedNone,
    isLive: () => Effect.succeed(false),
    otherWorkingThreads: () => Effect.succeed([]),
    detach: () => Effect.void,
    release: () => Effect.void,
    releaseUnlessBusy: () => Effect.succeed(true),
  });
  const allocations = Layer.sync(AllocatedUuids, () => new Set<string>());
  const ids = Layer.effect(
    IdAllocator.IdAllocatorV2,
    Effect.gen(function* () {
      const original = yield* IdAllocator.IdAllocatorV2;
      const allocated = yield* AllocatedUuids;
      const observe =
        <I, A extends string>(
          method: (input: I) => Effect.Effect<A, IdAllocator.IdAllocatorV2Error>,
        ) =>
        (input: I) =>
          method(input).pipe(
            Effect.tap((id) =>
              Effect.sync(() => {
                const uuid = id.match(
                  /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
                )?.[0];
                if (uuid) allocated.add(uuid);
              }),
            ),
          );
      const allocate = {
        command: observe(original.allocate.command),
        event: observe(original.allocate.event),
        rawEvent: observe(original.allocate.rawEvent),
        project: observe(original.allocate.project),
        thread: observe(original.allocate.thread),
        message: observe(original.allocate.message),
        providerSession: observe(original.allocate.providerSession),
        runtimeRequest: observe(original.allocate.runtimeRequest),
        checkpointScope: observe(original.allocate.checkpointScope),
        checkpoint: observe(original.allocate.checkpoint),
        contextHandoff: observe(original.allocate.contextHandoff),
        contextTransfer: observe(original.allocate.contextTransfer),
        plan: observe(original.allocate.plan),
      };
      return { ...original, allocate };
    }),
  ).pipe(Layer.provide(IdAllocator.layer), Layer.provide(allocations));
  const checkpoint = CheckpointService.layer.pipe(
    Layer.provide(ids),
    Layer.provide(Layer.mock(CheckpointStore.CheckpointStore)({})),
    Layer.provide(NodeCrypto.layer),
  );
  const services = Layer.mergeAll(
    allocations,
    persistence,
    adapters,
    externalRuntime,
    checkpoint,
    ProviderSwitchService.layer.pipe(Layer.provide(adapters)),
    ContextHandoffService.layer.pipe(Layer.provide(ids)),
    ids,
    RuntimePolicy.layer,
    ServerSettings.layerTest(settings, managedPolicy),
    CommandPolicy.layer,
    ProviderContinuationRequests.layer,
    ThreadForkService.layer,
    ThreadCommandExecutor.layer,
  );
  return Layer.mergeAll(
    services,
    database,
    Orchestrator.layer.pipe(Layer.provide(services), Layer.provide(NodeServices.layer)),
  );
};

export const makeRunRecords = (
  threadId: ThreadId,
  ordinal: number,
  status: OrchestrationV2Run["status"],
  now: DateTime.Utc,
  providerThreadId = ProviderThreadId.make(`provider:${threadId}`),
) => {
  const runId = RunId.make(`run:${threadId}:${ordinal}`);
  const nodeId = NodeId.make(`root:${threadId}:${ordinal}`);
  const attemptId = RunAttemptId.make(`attempt:${threadId}:${ordinal}`);
  const turnId = ProviderTurnId.make(`turn:${threadId}:${ordinal}`);
  const messageId = MessageId.make(`message:${threadId}:${ordinal}`);
  const scopeId = CheckpointScopeId.make(`scope:${threadId}:${ordinal}`);
  const terminal = ["completed", "failed", "interrupted", "cancelled", "rolled_back"].includes(
    status,
  );
  const startedAt =
    status === "queued" || status === "preparing"
      ? null
      : DateTime.subtract(now, { seconds: 100 - ordinal });
  const completedAt = terminal ? DateTime.subtract(now, { seconds: 50 - ordinal }) : null;
  const run: OrchestrationV2Run = {
    id: runId,
    threadId,
    ordinal,
    providerInstanceId: instanceId,
    modelSelection,
    providerThreadId,
    userMessageId: messageId,
    rootNodeId: nodeId,
    activeAttemptId: attemptId,
    status,
    requestedAt: DateTime.subtract(now, { seconds: 150 - ordinal }),
    startedAt,
    completedAt,
    checkpointId: null,
    contextHandoffId: null,
  };
  const attempt: OrchestrationV2RunAttempt = {
    id: attemptId,
    runId,
    attemptOrdinal: 1,
    rootNodeId: nodeId,
    providerInstanceId: instanceId,
    providerThreadId,
    providerTurnId: turnId,
    reason: "initial",
    status: terminal ? "completed" : "running",
    startedAt: startedAt ?? now,
    completedAt,
  };
  const root: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId,
    runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: "root_turn",
    status: terminal ? "completed" : status === "queued" ? "pending" : "running",
    countsForRun: true,
    providerThreadId,
    providerTurnId: turnId,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: scopeId,
    startedAt,
    completedAt,
  };
  const message: OrchestrationV2ConversationMessage = {
    id: messageId,
    threadId,
    runId,
    nodeId,
    role: "user",
    text: `Historical input ${ordinal}`,
    attachments: [],
    streaming: false,
    createdBy: "user",
    creationSource: "web",
    createdAt: run.requestedAt,
    updatedAt: run.requestedAt,
  };
  const item: Extract<OrchestrationV2TurnItem, { type: "user_message" }> = {
    id: TurnItemId.make(`item:${threadId}:${ordinal}`),
    threadId,
    runId,
    nodeId,
    providerThreadId,
    providerTurnId: turnId,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: ordinal * 10,
    status: "completed",
    title: `Historical input ${ordinal}`,
    startedAt: run.requestedAt,
    completedAt: run.requestedAt,
    updatedAt: run.requestedAt,
    type: "user_message",
    messageId,
    inputIntent: status === "queued" ? "queued_turn" : "turn_start",
    text: message.text,
    attachments: [],
    createdBy: "user",
    creationSource: "web",
  };
  const scope: OrchestrationV2CheckpointScope = {
    id: scopeId,
    threadId,
    runId,
    nodeId,
    parentScopeId: null,
    providerThreadId,
    kind: "root_run",
    ordinalWithinParent: 0,
    advancesAppRunCount: true,
    cwd: "/oracle/workspace",
    createdAt: run.requestedAt,
  };
  const turn: OrchestrationV2ProviderTurn = {
    id: turnId,
    providerThreadId,
    nodeId,
    runAttemptId: attemptId,
    nativeTurnRef: { driver, nativeId: `native-turn:${ordinal}`, strength: "strong" },
    ordinal,
    status: terminal ? "completed" : "running",
    startedAt: startedAt ?? now,
    completedAt,
  };
  return { run, root, attempt, message, item, scope, turn };
};

export const recordEvents = (
  records: ReturnType<typeof makeRunRecords>,
  now: DateTime.Utc,
): OrchestrationV2DomainEvent[] => {
  const { run, root, attempt, message, item, scope, turn } = records;
  const base = { threadId: run.threadId, occurredAt: now };
  return [
    { ...base, id: EventId.make(`seed:${run.id}`), type: "run.created", payload: run },
    { ...base, id: EventId.make(`seed:${root.id}`), type: "node.updated", payload: root },
    {
      ...base,
      id: EventId.make(`seed:${attempt.id}`),
      type: "run-attempt.created",
      payload: attempt,
    },
    { ...base, id: EventId.make(`seed:${message.id}`), type: "message.updated", payload: message },
    { ...base, id: EventId.make(`seed:${item.id}`), type: "turn-item.updated", payload: item },
    {
      ...base,
      id: EventId.make(`seed:${scope.id}`),
      type: "checkpoint-scope.created",
      payload: scope,
    },
    { ...base, id: EventId.make(`seed:${turn.id}`), type: "provider-turn.updated", payload: turn },
  ];
};

export const persistOracleEvents = Effect.fn("persistOracleEvents")(function* (
  events: readonly OrchestrationV2DomainEvent[],
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  for (const event of events) yield* store.apply(event);
});

export const seedOracle = Effect.fn("seedOracle")(function* (
  options: {
    history?: number;
    active?: OrchestrationV2Run["status"] | null;
    threadId?: ThreadId;
    runtimeMode?: RuntimeMode;
    provider?: boolean;
  } = {},
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const threadId = options.threadId ?? ThreadId.make("oracle-thread");
  const projectId = ProjectId.make("oracle-project");
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${threadId}`),
    threadId,
    projectId,
    title: "Oracle",
    modelSelection,
    runtimeMode: options.runtimeMode ?? "full-access",
    interactionMode: "default",
    branch: "main",
    worktreePath: "/oracle/workspace",
    createdBy: "user",
    creationSource: "web",
  });
  const providerThreadId = ProviderThreadId.make(`provider:${threadId}`);
  const session = {
    id: ProviderSessionId.make(`session:${threadId}`),
    driver,
    providerInstanceId: instanceId,
    status: "ready" as const,
    cwd: "/oracle/workspace",
    model: modelSelection.model,
    capabilities: CodexProviderCapabilitiesV2,
    createdAt: now,
    updatedAt: now,
    lastError: null,
  };
  const providerThread = {
    id: providerThreadId,
    driver,
    providerInstanceId: instanceId,
    providerSessionId: session.id,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: `native:${threadId}`, strength: "strong" as const },
    nativeConversationHeadRef: null,
    status: "active" as const,
    firstRunOrdinal: 1,
    lastRunOrdinal: (options.history ?? 4) + 1,
    handoffIds: [],
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
  };
  const thread = {
    ...(yield* store.getThread(threadId)),
    activeProviderThreadId: options.provider === false ? null : providerThreadId,
  };
  yield* persistOracleEvents([
    {
      id: EventId.make(`seed:thread:${threadId}`),
      type: "thread.metadata-updated",
      threadId,
      occurredAt: now,
      payload: thread,
    },
    ...(options.provider === false
      ? []
      : [
          {
            id: EventId.make(`seed:${session.id}`),
            type: "provider-session.attached" as const,
            threadId,
            occurredAt: now,
            payload: session,
          },
          {
            id: EventId.make(`seed:${providerThread.id}`),
            type: "provider-thread.updated" as const,
            threadId,
            occurredAt: now,
            payload: providerThread,
          },
        ]),
  ]);
  const history = Array.from({ length: options.history ?? 4 }, (_, i) =>
    makeRunRecords(threadId, i + 1, "completed", now, providerThreadId),
  );
  const active = makeRunRecords(
    threadId,
    history.length + 1,
    options.active ?? "completed",
    now,
    providerThreadId,
  );
  for (const bundle of [...history, ...(options.active === null ? [] : [active])])
    yield* persistOracleEvents(recordEvents(bundle, now));
  return { now, threadId, projectId, thread, providerThread, session, ...active, history };
});

// Error.toJSON drops non-enumerable message/cause; visit errors before JSON serialization.
const snapshotValue = (value: unknown): unknown => {
  if (value instanceof Error)
    return {
      ...Object.fromEntries(
        Object.entries(value).map(([key, field]) => [key, snapshotValue(field)]),
      ),
      name: value.name,
      message: value.message,
      ...(value.cause === undefined ? {} : { cause: snapshotValue(value.cause) }),
    };
  if (Array.isArray(value)) return value.map(snapshotValue);
  if (value !== null && typeof value === "object") {
    if ("toJSON" in value && typeof value.toJSON === "function")
      return snapshotValue(value.toJSON());
    return Object.fromEntries(
      Object.entries(value).map(([key, field]) => [key, snapshotValue(field)]),
    );
  }
  return value;
};

// Canonicalize only issued allocator UUIDs, once across all payloads and references.
const canonicalOracle = (value: unknown, allocated: ReadonlySet<string>) => {
  const uuids = new Map<string, number>();
  return JSON.parse(
    JSON.stringify(snapshotValue(value)).replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,
      (uuid) => {
        if (!allocated.has(uuid)) return uuid;
        if (!uuids.has(uuid)) uuids.set(uuid, uuids.size);
        return `UUID-${uuids.get(uuid)}`;
      },
    ),
  );
};

export const startOracleCapture = Effect.fn("startOracleCapture")(function* () {
  const allocated = yield* AllocatedUuids;
  const sink = yield* EventSink.EventSinkV2;
  const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const checkpoints = yield* CheckpointService.CheckpointServiceV2;
  const handoffs = yield* ContextHandoffService.ContextHandoffServiceV2;
  const switches = yield* ProviderSwitchService.ProviderSwitchServiceV2;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const policy = yield* RuntimePolicy.RuntimePolicyV2;
  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
  const transcript: unknown[] = [];
  const spies: Array<{ mockRestore: () => void }> = [];
  const watch = <T extends object, K extends keyof T>(service: T, key: K, label: string) => {
    const method = service[key];
    const original = vi.isMockFunction(method) ? method.getMockImplementation() : method;
    if (typeof original !== "function") throw new Error(`Not a method: ${label}`);
    // All watched service methods return Effects; this generic boundary preserves their signatures.
    spies.push(
      vi.spyOn(service, key as never).mockImplementation(((...args: unknown[]) =>
        Effect.sync(() => {
          const input = args[0] as Parameters<
            ProviderSwitchService.ProviderSwitchServiceV2Shape["plan"]
          >[0];
          const observedArgs =
            label === "switch.plan"
              ? [
                  {
                    targetModelSelection: input.targetModelSelection,
                    projection: {
                      thread: input.projection.thread,
                      providerSessions: input.projection.providerSessions,
                      providerThreads: input.projection.providerThreads,
                    },
                  },
                ]
              : args;
          transcript.push({ call: label, args: observedArgs });
        }).pipe(
          Effect.andThen((original as (...args: unknown[]) => Effect.Effect<unknown>)(...args)),
          Effect.tap((output) =>
            Effect.sync(() => {
              if (label.startsWith("sink.")) transcript.push({ returned: label, output });
            }),
          ),
          Effect.map((output) => {
            if (label !== "runtime.get") return output;
            const found = output as Option.Option<ProviderAdapter.ProviderAdapterV2SessionRuntime>;
            if (Option.isNone(found)) return output;
            const runtime = found.value;
            const probe = <A, E>(name: string, args: unknown[], effect: Effect.Effect<A, E>) =>
              Effect.sync(() => transcript.push({ call: name, args })).pipe(Effect.andThen(effect));
            return Option.some({
              ...runtime,
              ...(runtime.hasPendingBackgroundWork === undefined
                ? {}
                : {
                    hasPendingBackgroundWork: probe(
                      "runtime.hasPendingBackgroundWork",
                      [],
                      runtime.hasPendingBackgroundWork,
                    ),
                  }),
              ...(runtime.hasPendingBackgroundWorkForThread === undefined
                ? {}
                : {
                    hasPendingBackgroundWorkForThread: (
                      providerThread: Parameters<
                        NonNullable<typeof runtime.hasPendingBackgroundWorkForThread>
                      >[0],
                    ) =>
                      probe(
                        "runtime.hasPendingBackgroundWorkForThread",
                        [providerThread],
                        runtime.hasPendingBackgroundWorkForThread!(providerThread),
                      ),
                  }),
            });
          }),
        )) as never),
    );
  };
  for (const key of [
    "commitCommand",
    "commitRejectedCommand",
    "writeWithEffects",
    "write",
    "writeIfRunCurrent",
  ] as const)
    watch(sink, key, `sink.${key}`);
  watch(requests, "offer", "continuation.offer");
  watch(checkpoints, "prepareRootRunScope", "checkpoint.prepareRootRunScope");
  for (const key of [
    "prepareLegacyImport",
    "prepareForkDelta",
    "prepareProviderHandoff",
    "prepareSideChatContext",
  ] as const)
    watch(handoffs, key, `handoff.${key}`);
  watch(switches, "plan", "switch.plan");
  watch(sessions, "get", "runtime.get");
  watch(sessions, "otherWorkingThreads", "runtime.otherWorkingThreads");
  watch(policy, "resolve", "runtimePolicy.resolve");
  for (const id of [instanceId, otherInstanceId]) {
    const adapter = yield* registry.get(id);
    watch(adapter, "getCapabilities", `adapter.${id}.getCapabilities`);
    watch(adapter, "planSelectionTransition", `adapter.${id}.planSelectionTransition`);
  }
  return {
    transcript,
    stop: Effect.sync(() => spies.toReversed().forEach((spy) => spy.mockRestore())),
    snapshot: () => canonicalOracle(transcript, allocated),
  };
});

export const recordOracle = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const capture = yield* startOracleCapture();
    yield* Effect.gen(function* () {
      const result = yield* effect.pipe(Effect.result);
      capture.transcript.push(
        Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
      );
    }).pipe(Effect.ensuring(capture.stop));
    return capture.snapshot();
  });

import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export const layerDatabase = ProjectionStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

export const seed = Effect.fnUntraced(function* () {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const now = DateTime.makeUnsafe("2026-01-02T03:04:05Z");
  const threadId = ThreadId.make("oracle-thread");
  const projectId = ProjectId.make("oracle-project");
  const providerInstanceId = ProviderInstanceId.make("codex");
  const driver = ProviderDriverKind.make("codex");
  const providerThreadId = ProviderThreadId.make("oracle-provider-thread");
  const modelSelection = { instanceId: providerInstanceId, model: "oracle-model" };
  const thread: OrchestrationV2AppThread = {
    id: threadId,
    projectId,
    createdBy: "user",
    creationSource: "web",
    title: "Read-equivalence oracle",
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "oracle-branch",
    worktreePath: "/workspace/oracle",
    activeProviderThreadId: providerThreadId,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  let eventOrdinal = 0;
  const apply = (event: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">) =>
    store.apply({
      ...event,
      id: EventId.make(`oracle-event-${++eventOrdinal}`),
      occurredAt: now,
    } as OrchestrationV2DomainEvent);
  const putThread = (payload: OrchestrationV2AppThread) =>
    apply({ type: "thread.metadata-updated", threadId, payload });
  const run = (ordinal: number, patch: Partial<OrchestrationV2Run> = {}): OrchestrationV2Run => ({
    id: RunId.make(`oracle-run-${ordinal}`),
    threadId,
    ordinal,
    providerInstanceId,
    modelSelection,
    providerThreadId,
    userMessageId: MessageId.make(`oracle-user-${ordinal}`),
    rootNodeId: NodeId.make(`oracle-root-${ordinal}`),
    activeAttemptId: RunAttemptId.make(`oracle-attempt-${ordinal}`),
    status: "completed",
    requestedAt: now,
    startedAt: now,
    completedAt: now,
    checkpointId: null,
    contextHandoffId: null,
    ...patch,
  });
  const putRun = (payload: OrchestrationV2Run) =>
    apply({ type: "run.updated", threadId, runId: payload.id, payload });
  const message = (
    id: MessageId,
    runId: RunId | null,
    patch: Partial<OrchestrationV2ConversationMessage> = {},
  ): OrchestrationV2ConversationMessage => ({
    id,
    threadId,
    runId,
    nodeId: null,
    role: "user",
    text: `History ${id}`,
    attachments: [],
    streaming: false,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    ...patch,
  });
  const putMessage = (payload: OrchestrationV2ConversationMessage) =>
    apply({ type: "message.updated", threadId, payload });
  const turn = (
    run: OrchestrationV2Run,
    ordinal: number,
    status: OrchestrationV2ProviderTurn["status"],
    patch: Partial<OrchestrationV2ProviderTurn> = {},
  ): OrchestrationV2ProviderTurn => ({
    id: ProviderTurnId.make(`oracle-turn-${run.ordinal}-${ordinal}`),
    providerThreadId,
    nodeId: run.rootNodeId!,
    runAttemptId: run.activeAttemptId,
    nativeTurnRef: null,
    ordinal,
    status,
    startedAt: now,
    completedAt: status === "running" ? null : now,
    ...patch,
  });
  const putTurn = (payload: OrchestrationV2ProviderTurn) =>
    apply({ type: "provider-turn.updated", threadId, payload });
  type UserItem = Extract<OrchestrationV2TurnItem, { type: "user_message" }>;
  const item = (
    id: string,
    ordinal: number,
    messageId: MessageId,
    runId: RunId | null,
    inputIntent: UserItem["inputIntent"],
  ): UserItem => ({
    id: TurnItemId.make(id),
    threadId,
    runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed",
    title: `Input ${id}`,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
    type: "user_message",
    messageId,
    inputIntent,
    text: `Input ${id}`,
    attachments: [],
    createdBy: "user",
    creationSource: "web",
  });
  const putItem = (payload: OrchestrationV2TurnItem) =>
    apply({ type: "turn-item.updated", threadId, payload });
  yield* apply({ type: "thread.created", threadId, payload: thread });
  for (let ordinal = 1; ordinal <= 12; ordinal++) {
    const historical = run(ordinal, {
      providerThreadId: ProviderThreadId.make(`history-provider-thread-${ordinal}`),
    });
    yield* putRun(historical);
    yield* putMessage(message(historical.userMessageId, historical.id));
    yield* putItem(
      item(
        `oracle-history-item-${ordinal}`,
        ordinal,
        historical.userMessageId,
        historical.id,
        "turn_start",
      ),
    );
    yield* putTurn(
      turn(historical, ordinal, "completed", { providerThreadId: historical.providerThreadId! }),
    );
  }
  return {
    store,
    now,
    threadId,
    projectId,
    providerInstanceId,
    driver,
    providerThreadId,
    thread,
    run,
    message,
    turn,
    item,
    putThread,
    putRun,
    putMessage,
    putTurn,
    putItem,
  };
});

export function traceSql() {
  const queries: Array<{ query: string; transaction: string | null }> = [];
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (time, exit) => {
        end(time, exit);
        const query = span.attributes.get("db.query.text");
        if (typeof query !== "string") return;
        let parent = Option.getOrUndefined(span.parent);
        while (parent?._tag === "Span" && parent.name !== "sql.transaction")
          parent = Option.getOrUndefined(parent.parent);
        queries.push({
          query: query.replace(/[\t ]+$/gm, ""),
          transaction: parent?._tag === "Span" ? parent.spanId : null,
        });
      };
      return span;
    },
  });
  return { tracer, queries };
}

import { expect, it } from "@effect/vitest";
import { CommandId, EventId, MessageId, TurnItemId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { makeOracleLayer, seedOracle } from "./testkit/DispatchReadOracle.ts";

it.effect.each(["staged", "broad"] as const)(
  "%s keeps SQLite's first duplicate-linked item on equal ordinals",
  (reader) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ history: 0, active: "running" });
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      if (reader === "broad") {
        yield* store.apply({
          type: "thread.metadata-updated",
          id: EventId.make("review:import"),
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: { ...fixture.thread, historyOrigin: "v1_import" },
        });
      }
      const messageId = MessageId.make("review:duplicate-message");
      const expectedId = TurnItemId.make("review:\uE000");
      const otherId = TurnItemId.make("review:\u{1F600}");
      for (const id of [otherId, expectedId]) {
        yield* store.apply({
          type: "turn-item.updated",
          id: EventId.make(`review:seed:${id}`),
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: { ...fixture.item, id, ordinal: 777, messageId },
        });
      }
      const before = yield* store.getThreadRecords(fixture.threadId, ["turnItems"], {
        turnItemTypes: ["user_message", "error"],
      });
      expect(
        before.turnItems
          .filter((item) => item.type === "user_message" && item.messageId === messageId)
          .map((item) => item.id),
      ).toEqual([expectedId, otherId]);
      const result = yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("review:queue"),
        threadId: fixture.threadId,
        messageId,
        text: "Queue this message",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
      });
      const updated = result.storedEvents.flatMap(({ event }) =>
        event.type === "turn-item.updated" &&
        event.payload.type === "user_message" &&
        event.payload.messageId === messageId
          ? [event.payload.id]
          : [],
      );
      expect(updated).toEqual([expectedId]);
    }).pipe(Effect.provide(makeOracleLayer())),
);

import { assert, expect, it, vi } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  EventId,
  MessageId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import {
  makeOracleLayer,
  persistOracleEvents,
  recordOracle,
  seedOracle,
} from "./testkit/DispatchReadOracle.ts";

const pendingTranscript = Effect.fn("pendingOracle.transcript")(function* (
  command: OrchestrationV2ServerCommand,
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const readSpy = vi.spyOn(store, "getThreadRecords");
  return yield* recordOracle(orchestrator.dispatch(command)).pipe(
    Effect.tap(() =>
      Effect.sync(() => {
        assert.isTrue(
          readSpy.mock.calls.some(
            ([, fields, filter]) =>
              fields.includes("contextTransfers") &&
              fields.includes("attempts") &&
              fields.includes("messages") &&
              filter?.messageRoles?.includes("user"),
          ),
        );
      }),
    ),
    Effect.ensuring(Effect.sync(() => readSpy.mockRestore())),
  );
});

it.effect.each(["new-card", "replace-user-item"] as const)(
  "pending matrix 10: linked card unsnoozes before nested dispatch: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ active: "running" });
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const replaced = fixture.history[0]!;
      const messageId =
        testCase === "replace-user-item"
          ? replaced.message.id
          : MessageId.make("pending:linked-card-wake");
      const itemId =
        testCase === "replace-user-item"
          ? replaced.item.id
          : TurnItemId.make("pending:linked-card");
      yield* persistOracleEvents([
        {
          id: EventId.make("pending:linked-card-snooze"),
          type: "thread.metadata-updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.thread,
            snoozedAt: fixture.now,
            snoozedUntil: DateTime.add(fixture.now, { hours: 3 }),
          },
        },
      ]);
      const commandId = CommandId.make("pending:linked-card");
      expect(
        yield* pendingTranscript({
          type: "link_request.record",
          commandId,
          threadId: fixture.threadId,
          runId: fixture.run.id,
          nodeId: fixture.root.id,
          turnItemId: itemId,
          label: "Build worker",
          hint: "Connect to the existing build worker.",
          reason: "Run the remaining verification on the linked worker.",
          linkStatus: "linked",
          environmentId: EnvironmentId.make("requested-worker"),
          linkedEnvironmentId: EnvironmentId.make("linked-worker"),
          linkedLabel: "Build worker",
          wake: {
            messageId,
            text: "The build worker is connected; continue verification.",
            notification: {
              source: { kind: "system" },
              outcome: "completed",
              summary: "Build worker connected",
              detail: "Existing user authorization was used.",
            },
          },
        }),
      ).toMatchSnapshot();
      const final = yield* store.getThreadProjection(fixture.threadId);
      const item = final.turnItems.find((item) => item.id === itemId);
      assert.equal(item?.type, "link_request");
      if (item?.type !== "link_request") return;
      assert.equal(item.linkStatus, "linked");
      assert.equal(item.status, "completed");
      assert.equal(final.thread.snoozedUntil, null);
      const message = final.messages.find((message) => message.id === messageId);
      assert.equal(message?.text, "The build worker is connected; continue verification.");
      const queued = final.runs.find(
        (run) => run.userMessageId === messageId && run.id !== replaced.run.id,
      );
      assert.ok(queued);
      assert.equal(queued.status, "queued");
      assert.equal(message?.runId, queued.id);
      assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
      if (testCase === "replace-user-item") {
        assert.isFalse(
          final.turnItems.some(
            (item) => item.type === "user_message" && item.messageId === messageId,
          ),
        );
      }
    }).pipe(Effect.provide(makeOracleLayer())),
);

it.effect.each(["record-progress", "end-watch"] as const)(
  "pending matrix 10: complete watch metadata replacement precedes nested notification: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ active: "completed" });
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const key = { host: "github.example.test", repository: "oracle/dispatch", number: 17 };
      const startedAt = DateTime.formatIso(DateTime.subtract(fixture.now, { minutes: 10 }));
      const watch = {
        startedAt,
        headSha: "aabbccdd",
        failedChecks: ["test"],
        passed: false,
        passedChecks: ["lint"],
        owedChecks: ["typecheck"],
        remarksThrough: startedAt,
        remarkIds: ["review-1"],
        conflicting: false,
        wakes: 2,
        checksAwaitedSince: null,
      };
      const nextWatch = {
        ...watch,
        failedChecks: [],
        passedChecks: ["lint", "test", "typecheck"],
        owedChecks: [],
        passed: true,
        wakes: 3,
        remarksThrough: DateTime.formatIso(fixture.now),
        remarkIds: ["review-2"],
      };
      const snoozedUntil = DateTime.add(fixture.now, { hours: 2 });
      yield* persistOracleEvents([
        {
          id: EventId.make("pending:watch-metadata"),
          type: "thread.metadata-updated",
          threadId: fixture.threadId,
          occurredAt: fixture.now,
          payload: {
            ...fixture.thread,
            title: "Dispatch and queue oracle",
            snoozedAt: fixture.now,
            snoozedUntil,
            lastVisitedAt: DateTime.subtract(fixture.now, { hours: 1 }),
            pullRequests: [
              {
                ...key,
                url: "https://github.example.test/oracle/dispatch/pull/17",
                source: "manual",
                linkedAt: startedAt,
                snapshot: null,
                stack: null,
                watch,
              },
            ],
          },
        },
      ]);
      const commandId = CommandId.make("pending:watch-wake");
      const messageId = MessageId.make("pending:watch-message");
      expect(
        yield* pendingTranscript({
          type: "thread.pull-request-watch.sync",
          commandId,
          threadId: fixture.threadId,
          ...key,
          startedAt,
          watch: testCase === "end-watch" ? null : nextWatch,
          ...(testCase === "end-watch" ? { endedWatch: nextWatch } : {}),
          wake: {
            messageId,
            text: "All checks passed for the saved change.",
            notification: {
              source: { kind: "monitor" },
              outcome: "updated",
              summary: "All checks passed",
              detail: "The saved head aabbccdd passed lint, test and typecheck.",
            },
          },
        }),
      ).toMatchSnapshot();
      const final = yield* store.getThreadProjection(fixture.threadId);
      assert.equal(final.thread.title, "Dispatch and queue oracle");
      assert.deepEqual(final.thread.snoozedUntil, snoozedUntil);
      const savedLink = final.thread.pullRequests?.[0];
      assert.ok(savedLink);
      if (testCase === "end-watch") {
        assert.isUndefined(savedLink.watch);
        assert.deepEqual(savedLink.endedWatch, nextWatch);
      } else {
        assert.deepEqual(savedLink.watch, nextWatch);
      }
      const message = final.messages.find((message) => message.id === messageId);
      assert.equal(message?.text, "All checks passed for the saved change.");
      const run = final.runs.find((run) => run.userMessageId === messageId);
      assert.ok(run);
      assert.equal(run.status, "starting");
      assert.equal(message?.runId, run.id);
      assert.isTrue(
        final.turnItems.some((item) => item.runId === run.id && item.type === "notification"),
      );
      const effects = yield* outbox.listByCommandId(commandId);
      assert.isTrue(effects.some((effect) => effect.request.type === "provider-turn.start"));
    }).pipe(Effect.provide(makeOracleLayer())),
);

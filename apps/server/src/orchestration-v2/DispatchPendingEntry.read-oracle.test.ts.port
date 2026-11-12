import { assert, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";

import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { makeOracleLayer, seedOracle, startOracleCapture } from "./testkit/DispatchReadOracle.ts";

vi.mock("effect/Ref", async (importOriginal) => {
  const original = await importOriginal<typeof import("effect/Ref")>();
  return { ...original, make: vi.fn(original.make) };
});

// Arm after the layer and command lock exist; dispatchOnce creates its event array before effects.
const initializePendingEvents = (
  batch: readonly OrchestrationV2DomainEvent[],
  record: () => void,
) => {
  const original = vi.mocked(Ref.make).getMockImplementation()!;
  let injected = 0;
  vi.mocked(Ref.make).mockImplementation(((value: unknown) => {
    if (injected !== 0 || !Array.isArray(value) || value.length !== 0) return original(value);
    injected++;
    record();
    return original([...batch]);
  }) as typeof Ref.make);
  return { count: () => injected, restore: () => vi.mocked(Ref.make).mockImplementation(original) };
};

const cases = [
  "target-created",
  "target-metadata-status",
  "other-thread-only",
  "source-created-plan-stored-only",
] as const;

it.effect.each(cases)(
  "pending entry matrix 6/10: public dispatch starts with pending input: %s",
  (testCase) =>
    Effect.gen(function* () {
      const fixture = yield* seedOracle({ active: "completed" });
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const sourceThreadId = ThreadId.make("pending-entry-source");
      const sourcePlanId = PlanId.make("pending-entry-proposed-plan");
      const foreign =
        testCase === "other-thread-only"
          ? yield* seedOracle({
              threadId: ThreadId.make("pending-entry-foreign"),
              active: "completed",
            })
          : undefined;
      const now = fixture.now;
      const replacement = {
        ...fixture.thread,
        title: "Pending complete thread replacement",
        branch: "pending-entry-branch",
        worktreePath: "/pending-entry/workspace",
        interactionMode: "plan" as const,
        lastVisitedAt: null,
        settledOverride: "settled" as const,
        settledAt: now,
        snoozedAt: now,
        snoozedUntil: DateTime.add(now, { hours: 3 }),
      };
      const batch: OrchestrationV2DomainEvent[] =
        testCase === "source-created-plan-stored-only"
          ? [
              {
                id: EventId.make("pending-entry:source-created"),
                type: "thread.created",
                threadId: sourceThreadId,
                occurredAt: now,
                payload: {
                  ...fixture.thread,
                  id: sourceThreadId,
                  title: "Pending source plan thread",
                  activeProviderThreadId: null,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: sourceThreadId,
                  },
                },
              },
              {
                id: EventId.make("pending-entry:source-plan"),
                type: "plan.updated",
                threadId: sourceThreadId,
                occurredAt: now,
                payload: {
                  id: sourcePlanId,
                  threadId: sourceThreadId,
                  runId: null,
                  nodeId: NodeId.make("pending-entry-plan-node"),
                  status: "active",
                  kind: "proposed_plan",
                  markdown:
                    "# Pending plan\n\nThis plan exists only in the pending event collection.",
                },
              },
            ]
          : testCase === "other-thread-only"
            ? [
                {
                  id: EventId.make("pending-entry:foreign-metadata"),
                  type: "thread.metadata-updated",
                  threadId: foreign!.threadId,
                  occurredAt: now,
                  payload: {
                    ...foreign!.thread,
                    title: "Foreign pending metadata",
                    branch: "foreign-pending-branch",
                    settledOverride: "settled",
                    settledAt: now,
                  },
                },
              ]
            : [
                {
                  id: EventId.make(`pending-entry:${testCase}`),
                  type:
                    testCase === "target-created" ? "thread.created" : "thread.metadata-updated",
                  threadId: fixture.threadId,
                  occurredAt: now,
                  payload: replacement,
                },
              ];
      const readSpy = vi.spyOn(store, "getThreadRecords");
      const planSpy = vi.spyOn(store, "getPlan");
      const capture = yield* startOracleCapture();
      const pending = initializePendingEvents(batch, () =>
        capture.transcript.push({ initialPendingEvents: batch }),
      );
      yield* Effect.gen(function* () {
        const messageId = MessageId.make("pending-entry-message");
        const result = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("pending-entry-command"),
            threadId: fixture.threadId,
            messageId,
            text: "Use the entire pending state when deciding this message.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
            ...(testCase === "source-created-plan-stored-only"
              ? { sourcePlanRef: { threadId: sourceThreadId, planId: sourcePlanId } }
              : {}),
          })
          .pipe(Effect.result);
        capture.transcript.push(
          Result.isSuccess(result) ? { result: result.success } : { error: result.failure },
        );
        assert.equal(pending.count(), 1);
        const snapshot = capture.snapshot();
        if (testCase === "source-created-plan-stored-only") {
          assert.isTrue(Result.isFailure(result));
          assert.isTrue(
            planSpy.mock.calls.some(([id, plan]) => id === sourceThreadId && plan === sourcePlanId),
          );
          assert.include(JSON.stringify(snapshot), '"_tag":"ProjectionStoreThreadNotFoundError"');
          assert.include(
            JSON.stringify(snapshot),
            `No orchestration projection exists for thread ${sourceThreadId}.`,
          );
          assert.notInclude(JSON.stringify(snapshot), '"_tag":"OrchestratorProjectionError"');
          assert.equal(
            (yield* store.getPlan(sourceThreadId, sourcePlanId).pipe(Effect.exit))._tag,
            "Failure",
          );
          assert.equal((yield* store.getThread(sourceThreadId).pipe(Effect.exit))._tag, "Failure");
          assert.isEmpty(vi.mocked(sink.commitCommand).mock.calls);
        } else {
          assert.isTrue(Result.isSuccess(result));
          const committed = vi.mocked(sink.commitCommand).mock.calls[0]?.[0];
          assert.ok(committed);
          assert.equal(committed.events[0]?.id, batch[0]!.id);
          assert.isTrue(committed.events.some((event) => event.type === "run.created"));
          assert.isTrue(
            committed.effects.some((effect) => effect.request.type === "provider-turn.start"),
          );
          const final = yield* store.getThreadProjection(fixture.threadId);
          const message = final.messages.find((message) => message.id === messageId);
          assert.ok(message);
          assert.equal(final.runs.find((run) => run.id === message.runId)?.status, "starting");
          if (testCase === "other-thread-only") {
            assert.equal(final.thread.title, fixture.thread.title);
            assert.equal(final.thread.branch, fixture.thread.branch);
            assert.equal(
              (yield* store.getThread(foreign!.threadId)).title,
              "Foreign pending metadata",
            );
          } else {
            assert.isTrue(
              readSpy.mock.calls.some(
                ([id, fields]) =>
                  id === fixture.threadId &&
                  fields.includes("contextTransfers") &&
                  fields.includes("attempts") &&
                  fields.includes("messages"),
              ),
            );
            assert.equal(final.thread.title, replacement.title);
            assert.equal(final.thread.branch, replacement.branch);
            assert.equal(final.thread.worktreePath, replacement.worktreePath);
            assert.equal(final.thread.interactionMode, "plan");
            assert.equal(final.thread.settledOverride, null);
            assert.equal(final.thread.settledAt, null);
            assert.equal(final.thread.snoozedUntil, null);
            assert.deepEqual(final.thread.unsettledAt, now);
            assert.deepEqual(
              final.thread.lastVisitedAt,
              DateTime.subtract(now, { milliseconds: 1 }),
            );
          }
        }
        expect(snapshot).toMatchSnapshot();
      }).pipe(
        Effect.ensuring(
          Effect.all([
            capture.stop,
            Effect.sync(() => {
              pending.restore();
              planSpy.mockRestore();
              readSpy.mockRestore();
            }),
          ]),
        ),
      );
    }).pipe(Effect.provide(makeOracleLayer())),
);

import { assert, describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2Notification,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as TestClock from "effect/testing/TestClock";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ReadFixture from "./SendContinuationReads.testkit.ts";
import { isUndeliveredMailboxSteer } from "./NotificationMailbox.ts";

const threadId = ThreadId.make("thread-provider-continuation");
const providerThreadId = ProviderThreadId.make("provider-thread-continuation");
const driver = ProviderDriverKind.make("continuation-test");
const parentRunId = RunId.make("run-provider-continuation-parent");
const delegatedMessageId = MessageId.make("message-provider-continuation-delegated");
const projection = {
  thread: { archivedAt: null, deletedAt: null },
  messages: [],
} as unknown as OrchestrationV2ThreadProjection;

const request = (
  dispatchIfCurrent?: ProviderContinuationRequests.ProviderContinuationRequest["dispatchIfCurrent"],
  detail: string | null = null,
): ProviderContinuationRequests.ProviderContinuationRequest => ({
  threadId,
  providerThreadId,
  driver,
  detail,
  ...(dispatchIfCurrent === undefined ? {} : { dispatchIfCurrent }),
});

const delegatedProjection = (disposition: "open" | "stopped" | "disposed" = "open") =>
  ({
    ...projection,
    runs: [
      {
        id: parentRunId,
        delegatedCompletion: {
          disposition,
          nextGeneration: 2,
          delivery: {
            generation: 1,
            messageId: delegatedMessageId,
            taskIds: ["node-provider-continuation-first", "node-provider-continuation-second"],
          },
        },
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

const makeGuard = Effect.fnUntraced(function* (completed?: Deferred.Deferred<void>) {
  const generation = yield* Ref.make(0);
  const permit = yield* Semaphore.make(1);
  const capture = Effect.gen(function* () {
    const captured = yield* Ref.updateAndGet(generation, (value) => value + 1);
    return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      permit
        .withPermit(
          Effect.gen(function* () {
            if ((yield* Ref.get(generation)) !== captured) return Option.none();
            return Option.some(yield* effect);
          }),
        )
        .pipe(
          completed === undefined
            ? (effect) => effect
            : Effect.ensuring(Deferred.succeed(completed, undefined)),
        );
  });
  return {
    capture,
    invalidate: permit.withPermit(Ref.update(generation, (value) => value + 1)),
  };
});

function layerTest(input: {
  readonly dispatched: Queue.Queue<unknown>;
  readonly getProviderContinuationState: () => Effect.Effect<OrchestrationV2ThreadProjection>;
}) {
  const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
    getProviderContinuationState: input.getProviderContinuationState,
    dispatch: (command) => Queue.offer(input.dispatched, command).pipe(Effect.as({} as never)),
  });
  const layerWorker = ProviderContinuationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
    ),
  );
  return Layer.merge(ProviderContinuationRequests.layer, layerWorker);
}

describe("ProviderContinuationService", () => {
  it.effect("recovers an unaccepted persisted steer using the same delivery identity", () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const original = delegatedProjection();
      const interruptedRunId = RunId.make("interrupted-mailbox-run");
      const recovered = {
        ...original,
        runs: [
          ...original.runs,
          {
            id: interruptedRunId,
            status: "interrupted",
            userMessageId: MessageId.make("original-user-input"),
          },
        ],
        messages: [
          {
            id: delegatedMessageId,
            runId: interruptedRunId,
            delegatedCompletion: { parentRunId, generation: 1, taskIds: [] },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: { parentRunId, generation: 1, messageId: delegatedMessageId },
        });
        const command = (yield* Queue.take(dispatched)) as {
          messageId: MessageId;
          delegatedCompletion: { generation: number; taskIds: readonly string[] };
        };
        assert.equal(command.messageId, delegatedMessageId);
        assert.equal(command.delegatedCompletion.generation, 1);
        assert.equal(command.delegatedCompletion.taskIds.length, 2);
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getProviderContinuationState: () => Effect.succeed(recovered) }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("delivers a message_text wake as a real prompt, not a buffered wake", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: "Delegated task completed.",
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor updated",
          },
          delivery: "message_text",
        });
        const command = (yield* Queue.take(dispatched)) as {
          readonly creationSource: string;
          readonly text: string;
          readonly notification: OrchestrationV2Notification;
        };
        assert.notEqual(command.creationSource, "provider");
        assert.equal(command.creationSource, "server");
        assert.equal(command.text, "Delegated task completed.");
        assert.deepEqual(command.notification, {
          source: { kind: "monitor" },
          outcome: "updated",
          summary: "Monitor updated",
        });
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getProviderContinuationState: () => Effect.succeed(projection) }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("dispatches a current delegated completion as one server-owned queued message", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: {
            parentRunId,
            generation: 1,
            messageId: delegatedMessageId,
          },
        });
        const command = (yield* Queue.take(dispatched)) as {
          readonly createdBy: string;
          readonly creationSource: string;
          readonly delegatedCompletion: {
            readonly generation: number;
            readonly parentRunId: RunId;
            readonly taskIds: ReadonlyArray<string>;
          };
          readonly dispatchMode: { readonly type: string };
          readonly messageId: MessageId;
          readonly text: string;
        };
        assert.equal(command.createdBy, "agent");
        assert.equal(command.creationSource, "server");
        assert.deepEqual(command.dispatchMode, { type: "queue_after_active" });
        assert.equal(command.messageId, delegatedMessageId);
        assert.equal(command.delegatedCompletion.parentRunId, parentRunId);
        assert.equal(command.delegatedCompletion.generation, 1);
        assert.deepEqual(command.delegatedCompletion.taskIds, [
          "node-provider-continuation-first",
          "node-provider-continuation-second",
        ]);
        assert.include(command.text, "task_status");
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getProviderContinuationState: () => Effect.succeed(delegatedProjection()),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect(
    "a project-disabled instance refuses a delegated-completion wake exactly once, disposes it, and never retries",
    () => {
      return Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const cleared = yield* Ref.make(false);
        const disposed = yield* Queue.unbounded<unknown>();
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getProviderContinuationState: () => Effect.succeed(delegatedProjection()),
          dispatch: (command) => {
            if ((command as { type: string }).type === "message.dispatch") {
              return Ref.update(attempts, (count) => count + 1).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                      projectId: ProjectId.make("project-provider-continuation-disabled"),
                      instanceId: ProviderInstanceId.make(
                        "instance-provider-continuation-disabled",
                      ),
                    }) as never,
                  ),
                ),
              );
            }
            return Queue.offer(disposed, command).pipe(Effect.as({} as never));
          },
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            threadId,
            providerThreadId,
            driver,
            detail: null,
            delivery: "message_text",
            clearIfCurrent: () => Ref.set(cleared, true),
            delegatedCompletion: {
              parentRunId,
              generation: 1,
              messageId: delegatedMessageId,
            },
          });
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(attempts), 1);
          assert.isTrue(yield* Ref.get(cleared));

          const dispositions = (yield* Queue.takeAll(disposed)) as ReadonlyArray<{
            type: string;
            taskId: string;
          }>;
          assert.deepEqual(
            dispositions.map((command) => command.taskId),
            ["node-provider-continuation-first", "node-provider-continuation-second"],
          );
          for (const command of dispositions) {
            assert.equal(command.type, "delegated_task.completion-delivery.dispose");
          }

          yield* TestClock.adjust("1 hour");
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(attempts), 1);
          assert.equal(yield* Queue.size(disposed), 0);
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears the adapter's sticky continuation flag when a non-delegated wake is refused",
    () => {
      return Effect.gen(function* () {
        const requested = yield* Ref.make(true);
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getProviderContinuationState: () => Effect.succeed(projection),
          dispatch: () =>
            Effect.fail(
              new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                projectId: ProjectId.make("project-provider-continuation-disabled"),
                instanceId: ProviderInstanceId.make("instance-provider-continuation-disabled"),
              }) as never,
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        // ACP clears its sticky continuation flag on raw dispatch failure; pre-recovery would prevent that cleanup.
        const dispatchIfCurrent = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const exit = yield* Effect.exit(effect);
            if (Exit.isFailure(exit)) {
              yield* Ref.set(requested, false);
              return yield* Effect.failCause(exit.cause);
            }
            return Option.some(exit.value);
          });

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer(request(dispatchIfCurrent));
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          assert.isFalse(
            yield* Ref.get(requested),
            "a disabled instance must still clear the adapter's sticky offer flag",
          );
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears a sticky continuation offer for an adapter whose dispatchIfCurrent does not self-clear on failure",
    () => {
      return Effect.gen(function* () {
        const wakeHeld = yield* Ref.make(true);
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getProviderContinuationState: () => Effect.succeed(projection),
          dispatch: () =>
            Effect.fail(
              new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                projectId: ProjectId.make("project-provider-continuation-disabled"),
                instanceId: ProviderInstanceId.make("instance-provider-continuation-disabled"),
              }) as never,
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        // Muse forwards dispatch failures unchanged; clearIfCurrent must release its sticky wake flag.
        const dispatchIfCurrent = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.asSome(effect);

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            ...request(dispatchIfCurrent),
            clearIfCurrent: () => Ref.set(wakeHeld, false),
          });
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          assert.isFalse(
            yield* Ref.get(wakeHeld),
            "a disabled instance must clear the offer via the explicit callback " +
              "when the adapter itself does not clear on dispatch failure",
          );
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears a sticky continuation offer when a non-delegated wake dispatch fails generically",
    () => {
      return Effect.gen(function* () {
        const cleared = yield* Deferred.make<void>();
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getProviderContinuationState: () => Effect.succeed(projection),
          dispatch: (command) =>
            Effect.fail(
              new Orchestrator.OrchestratorDispatchError({
                commandId: CommandId.make(String(command.commandId)),
                commandType: command.type,
                cause: new Error("SQLITE_FULL: database or disk is full"),
              }),
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            ...request(),
            clearIfCurrent: () => Deferred.succeed(cleared, undefined),
          });
          yield* Deferred.await(cleared);
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "drops stopped and disposed delegated completions instead of reviving them after recovery",
    () => {
      return Effect.gen(function* () {
        for (const disposition of ["stopped", "disposed"] as const) {
          const dispatched = yield* Queue.unbounded<unknown>();
          yield* Effect.gen(function* () {
            const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
            yield* requests.offer({
              threadId,
              providerThreadId,
              driver,
              detail: null,
              delivery: "message_text",
              delegatedCompletion: {
                parentRunId,
                generation: 1,
                messageId: delegatedMessageId,
              },
            });
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
            assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
          }).pipe(
            Effect.provide(
              layerTest({
                dispatched,
                getProviderContinuationState: () =>
                  Effect.succeed(delegatedProjection(disposition)),
              }),
            ),
            Effect.scoped,
          );
        }
      });
    },
  );

  it.effect("does not redispatch a persisted delegated completion message during recovery", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: {
            parentRunId,
            generation: 1,
            messageId: delegatedMessageId,
          },
        });
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getProviderContinuationState: () =>
              Effect.succeed({
                ...delegatedProjection(),
                messages: [{ id: delegatedMessageId }],
              } as unknown as OrchestrationV2ThreadProjection),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("dispatches a current request exactly once", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer(request());
        yield* Queue.take(dispatched);
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getProviderContinuationState: () => Effect.succeed(projection) }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("drops a request invalidated while projection is blocked", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const projectionEntered = yield* Deferred.make<void>();
      const releaseProjection = yield* Deferred.make<void>();
      const guardCompleted = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const guard = yield* makeGuard(guardCompleted);
        const dispatchIfCurrent = yield* guard.capture;
        yield* requests.offer(request(dispatchIfCurrent));
        yield* Deferred.await(projectionEntered);
        yield* guard.invalidate;
        yield* Deferred.succeed(releaseProjection, undefined);
        yield* Deferred.await(guardCompleted);
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getProviderContinuationState: () =>
              Deferred.succeed(projectionEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseProjection)),
                Effect.as(projection),
              ),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("does not revive an old request when a later generation is current", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const firstProjectionEntered = yield* Deferred.make<void>();
      const releaseFirstProjection = yield* Deferred.make<void>();
      let projectionCalls = 0;
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const guard = yield* makeGuard();
        const first = yield* guard.capture;
        yield* requests.offer(request(first, "A"));
        yield* Deferred.await(firstProjectionEntered);
        const second = yield* guard.capture;
        yield* requests.offer(request(second, "B"));
        yield* Deferred.succeed(releaseFirstProjection, undefined);
        const command = yield* Queue.take(dispatched);
        assert.equal((command as { readonly text?: unknown }).text, "B");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getProviderContinuationState: () => {
              projectionCalls += 1;
              return projectionCalls === 1
                ? Deferred.succeed(firstProjectionEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirstProjection)),
                    Effect.as(projection),
                  )
                : Effect.succeed(projection);
            },
          }),
        ),
        Effect.scoped,
      );
    });
  });
});

const continuationReadCases = [
  "ordinary",
  "no-message",
  "missing-source",
  "stopped-source",
  "disposed-source",
  "missing-cohort",
  "null-delivery",
  "generation-mismatch",
  "message-mismatch",
  "empty-task-list",
  "single-task",
  "missing-delivery-run",
  "runless-message",
  "terminal-completed",
  "terminal-failed",
  "terminal-interrupted",
  "terminal-cancelled",
  "terminal-rolled_back",
  "own-user-run",
  "null-attempt",
  "latest-running",
  "latest-completed",
  "tie-completed-first",
  "tie-running-first",
  "binary-provider-tie",
  "owner-is-delivery-run",
  "wrong-role-message",
  "missing-message-metadata",
  "archived",
  "deleted",
] as const;

it.effect.each(continuationReadCases)(
  "continuation state equals main's relevant records for %s",
  (scenario) =>
    Effect.gen(function* () {
      const fixture = yield* ReadFixture.seed();
      const { store, threadId } = fixture;
      const messageId = MessageId.make("delivery-message");
      const source = fixture.run(21, {
        status: "running",
        completedAt: null,
        delegatedCompletion: {
          disposition:
            scenario === "stopped-source"
              ? "stopped"
              : scenario === "disposed-source"
                ? "disposed"
                : "open",
          nextGeneration: 8,
          delivery: {
            generation: scenario === "generation-mismatch" ? 6 : 7,
            messageId:
              scenario === "message-mismatch" ? MessageId.make("different-message") : messageId,
            taskIds:
              scenario === "empty-task-list"
                ? []
                : scenario === "single-task"
                  ? [NodeId.make("task-a")]
                  : [NodeId.make("task-b"), NodeId.make("task-a"), NodeId.make("task-b")],
          },
        },
      });
      if (scenario === "missing-cohort") {
        const { delegatedCompletion: _completion, ...withoutCompletion } = source;
        yield* fixture.putRun(withoutCompletion);
      } else if (scenario === "null-delivery") {
        yield* fixture.putRun({
          ...source,
          delegatedCompletion: { ...source.delegatedCompletion!, delivery: null },
        });
      } else if (scenario !== "missing-source") yield* fixture.putRun(source);
      const delivery = fixture.run(22, {
        status: scenario.startsWith("terminal-")
          ? (scenario.slice("terminal-".length) as
              | "completed"
              | "failed"
              | "interrupted"
              | "cancelled"
              | "rolled_back")
          : "running",
        completedAt: null,
        ...(scenario === "own-user-run" ? { userMessageId: messageId } : {}),
        ...(scenario === "null-attempt" ? { activeAttemptId: null } : {}),
      });
      if (scenario !== "missing-delivery-run") yield* fixture.putRun(delivery);
      const owner = scenario === "owner-is-delivery-run" ? source : delivery;
      if (scenario !== "no-message") {
        yield* fixture.putMessage(
          fixture.message(messageId, scenario === "runless-message" ? null : owner.id, {
            role: scenario === "wrong-role-message" ? "assistant" : "user",
            ...(scenario === "missing-message-metadata"
              ? {}
              : { delegatedCompletion: { parentRunId: source.id, generation: 7, taskIds: [] } }),
          }),
        );
      }
      yield* fixture.putTurn(
        fixture.turn(owner, 1, scenario === "latest-running" ? "completed" : "running"),
      );
      yield* fixture.putTurn(
        fixture.turn(owner, 2, scenario === "latest-running" ? "running" : "completed"),
      );
      if (scenario.startsWith("tie-") || scenario === "binary-provider-tie") {
        yield* fixture.putTurn(
          fixture.turn(owner, 3, scenario === "tie-running-first" ? "running" : "completed", {
            providerThreadId: ProviderThreadId.make(
              scenario === "binary-provider-tie" ? "z-provider" : "a-provider",
            ),
          }),
        );
        yield* fixture.putTurn(
          fixture.turn(owner, 3, scenario === "tie-running-first" ? "completed" : "running", {
            id: fixture.turn(owner, 4, "running").id,
            providerThreadId: ProviderThreadId.make(
              scenario === "binary-provider-tie" ? "ä-provider" : "z-provider",
            ),
          }),
        );
      }
      if (scenario === "archived" || scenario === "deleted") {
        yield* fixture.putThread({
          ...fixture.thread,
          ...(scenario === "archived" ? { archivedAt: fixture.now } : { deletedAt: fixture.now }),
        });
      }
      const completion =
        scenario === "ordinary" ? undefined : { parentRunId: source.id, generation: 7, messageId };
      const broad = yield* store.getThreadRecords(threadId, ["messages", "runs", "providerTurns"], {
        messageIds: completion === undefined ? [] : [messageId],
      });
      const sql = ReadFixture.traceSql();
      const narrow = yield* store
        .getProviderContinuationState(threadId, completion)
        .pipe(Effect.withTracer(sql.tracer));
      expect(narrow.thread).toEqual(broad.thread);
      if (completion === undefined) {
        expect(narrow.messages).toEqual([]);
        expect(narrow.runs).toEqual([]);
        expect(narrow.providerTurns).toEqual([]);
        expect(
          sql.queries.some(({ query }) =>
            /FROM orchestration_v2_projection_(runs|provider_turns|messages|turn_items|run_attempts|nodes)\b/.test(
              query,
            ),
          ),
        ).toBe(false);
      } else {
        const owningRunId = broad.messages[0]?.runId;
        const runs = broad.runs.filter((run) => run.id === source.id || run.id === owningRunId);
        const attemptId = runs.find((run) => run.id === owningRunId)?.activeAttemptId;
        expect(narrow.messages).toEqual(broad.messages);
        expect(narrow.runs).toEqual(runs);
        expect(narrow.providerTurns).toEqual(
          attemptId == null
            ? []
            : broad.providerTurns.filter((turn) => turn.runAttemptId === attemptId),
        );
        expect(isUndeliveredMailboxSteer(narrow, messageId)).toBe(
          isUndeliveredMailboxSteer(broad, messageId),
        );
      }
      const selects = sql.queries.filter(({ query }) => /^\s*SELECT\b/i.test(query));
      expect(selects.length).toBeGreaterThan(0);
      expect(selects.every(({ transaction }) => transaction !== null)).toBe(true);
      expect(new Set(selects.map(({ transaction }) => transaction)).size).toBe(1);
      if (scenario === "generation-mismatch") {
        // The retry path must observe cohort changes made after the initial dispatch read.
        yield* fixture.putRun({
          ...source,
          delegatedCompletion: {
            ...source.delegatedCompletion!,
            delivery: { ...source.delegatedCompletion!.delivery!, generation: 8 },
          },
        });
        const fresh = yield* store.getProviderContinuationState(threadId, completion);
        expect(
          fresh.runs.find((run) => run.id === source.id)?.delegatedCompletion?.delivery?.generation,
        ).toBe(8);
      }
    }).pipe(Effect.provide(ReadFixture.layerDatabase)),
);

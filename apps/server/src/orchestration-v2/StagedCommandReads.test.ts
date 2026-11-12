// Local t3-code-nightly patch: eligible sends take the staged reads, the rest the broad read.
// The read-oracle tests check that both give the same transcript.
import { assert, it } from "@effect/vitest";
import { CommandId, MessageId, type OrchestrationV2Run } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Orchestrator from "./Orchestrator.ts";
import { traceSql } from "./SendContinuationReads.testkit.ts";
import { makeOracleLayer, seedOracle } from "./testkit/DispatchReadOracle.ts";

const stagedOff = process.env.T3CODE_LOCAL_STAGED_COMMAND_READS === "off";

const cases: ReadonlyArray<{
  readonly name: string;
  readonly active: OrchestrationV2Run["status"] | null;
  readonly model?: string;
  readonly staged: boolean;
}> = [
  { name: "an idle send", active: null, staged: true },
  { name: "a send behind a running run", active: "running", staged: true },
  { name: "a model change", active: null, model: "gpt-5.5", staged: false },
];

it.effect.each(cases)(
  "$name takes the staged or broad read",
  ({ active, model, staged }) =>
    Effect.gen(function* () {
      if (stagedOff) return;
      const fixture = yield* seedOracle({ active });
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const trace = traceSql();
      yield* orchestrator
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("staged-send"),
          threadId: fixture.threadId,
          messageId: MessageId.make("staged-message"),
          text: "Next step",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          dispatchMode: { type: "queue_after_active" },
          ...(model === undefined
            ? {}
            : { modelSelection: { ...fixture.thread.modelSelection, model } }),
        })
        .pipe(Effect.exit, Effect.withTracer(trace.tracer));
      // The staged read alone seeks the latest user item; the broad read alone finds the
      // last compaction.
      const queries = trace.queries.map((entry) => entry.query);
      assert.equal(
        queries.some((query) => query.includes("local_projection_turn_items_type_private_idx")),
        staged,
      );
      assert.equal(
        queries.some((query) => query.includes("WITH compaction AS")),
        !staged,
      );
    }).pipe(Effect.provide(makeOracleLayer())),
);

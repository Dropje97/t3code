// @effect-diagnostics nodeBuiltinImport:off - integration tests exercise real process IO.
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as ChildProcess from "effect/process/ChildProcess";
import * as Fiber from "effect/Fiber";
import * as SpawnHost from "./process/SpawnHost.ts";
import { isWindowsCommandNotFound } from "@t3tools/provider-core/server/snapshotProbe";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";

import * as ProcessRunner from "./processRunner.ts";

type ChildProcessCommand = {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly options: {
    readonly shell?: boolean | string;
  };
};

function asChildProcessCommand(command: unknown): ChildProcessCommand {
  return command as ChildProcessCommand;
}

function makeHandle(input: {
  readonly stdout?: string | Stream.Stream<Uint8Array>;
  readonly stderr?: string | Stream.Stream<Uint8Array>;
  readonly code?: number;
  readonly stdin?: ChildProcessSpawner.ChildProcessHandle["stdin"];
  readonly exitCode?: Effect.Effect<ChildProcessSpawner.ExitCode>;
}) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: input.exitCode ?? Effect.succeed(ChildProcessSpawner.ExitCode(input.code ?? 0)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: input.stdin ?? Sink.drain,
    stdout:
      typeof input.stdout === "string"
        ? Stream.encodeText(Stream.make(input.stdout))
        : (input.stdout ?? Stream.empty),
    stderr:
      typeof input.stderr === "string"
        ? Stream.encodeText(Stream.make(input.stderr))
        : (input.stderr ?? Stream.empty),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function makeSpawner(
  f: (
    command: ChildProcessCommand,
  ) => Effect.Effect<ChildProcessSpawner.ChildProcessHandle, PlatformError.PlatformError>,
) {
  return ChildProcessSpawner.make((command) => f(asChildProcessCommand(command)));
}

const runWith =
  (spawner: ChildProcessSpawner.ChildProcessSpawner["Service"]) =>
  (input: ProcessRunner.ProcessRunInput) =>
    Effect.service(ProcessRunner.ProcessRunner).pipe(
      Effect.flatMap((runner) =>
        runner.run({
          ...input,
        }),
      ),
      Effect.provide(
        ProcessRunner.layer.pipe(
          Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
        ),
      ),
    );

describe("runProcess", () => {
  it.effect("resolves and escapes Windows command shims before spawning", () => {
    const spawner = makeSpawner((command) =>
      Effect.sync(() => {
        expect(command.command).toBe(
          [
            '^"C:\\Users\\tester\\AppData\\Roaming\\npm\\az.cmd^"',
            '^"repos^"',
            '^"pr^"',
            '^"list^"',
            '^"--source-branch^"',
            '^"feature^ ^&^ release^"',
          ].join(" "),
        );
        expect(command.args).toEqual([]);
        expect(command.options.shell).toBe(true);
        return makeHandle({ stdout: "[]" });
      }),
    );

    return runWith(spawner)({
      command: "az",
      args: ["repos", "pr", "list", "--source-branch", "feature & release"],
      env: { AZURE_CONFIG_DIR: "C:\\Users\\tester\\.azure" },
    }).pipe(
      Effect.provideService(HostProcess.Platform, "win32"),
      Effect.provideService(HostProcess.Environment, {
        PATH: "C:\\Users\\tester\\AppData\\Roaming\\npm",
        PATHEXT: ".COM;.EXE;.BAT;.CMD",
      }),
      Effect.provideService(SpawnExecutableResolution, (_command, _platform, env) =>
        env.PATH === "C:\\Users\\tester\\AppData\\Roaming\\npm" &&
        env.AZURE_CONFIG_DIR === "C:\\Users\\tester\\.azure"
          ? "C:\\Users\\tester\\AppData\\Roaming\\npm\\az.cmd"
          : undefined,
      ),
      Effect.map((result) => {
        expect(result.stdout).toBe("[]");
      }),
    );
  });

  it.effect("types spawn failures that the platform spawner throws synchronously", () =>
    Effect.gen(function* () {
      // File-valued PATH entries make spawn throw ENOTDIR synchronously, arriving through Effect.callback as a defect.
      const defect = Object.assign(new Error("spawn jj ENOTDIR"), { code: "ENOTDIR" });
      const spawner = makeSpawner(() =>
        Effect.callback<never, never>(() => {
          throw defect;
        }),
      );

      const error = yield* runWith(spawner)({
        command: "jj",
        args: ["--version"],
        cwd: "/repo",
      }).pipe(Effect.flip);

      expect(error._tag).toBe("ProcessSpawnError");
      if (error._tag !== "ProcessSpawnError") {
        return expect.fail("Expected ProcessSpawnError");
      }
      expect(error).toMatchObject({ command: "jj", argumentCount: 1, cwd: "/repo" });
      expect(error.cause).toBe(defect);
    }),
  );

  it.effect("fails when output exceeds max buffer in default mode", () =>
    Effect.gen(function* () {
      const spawner = makeSpawner(() => Effect.succeed(makeHandle({ stdout: "x".repeat(2048) })));

      const error = yield* runWith(spawner)({
        command: "fake",
        args: ["stdout-bytes", "2048"],
        maxOutputBytes: 128,
      }).pipe(Effect.flip);

      expect(error._tag).toBe("ProcessOutputLimitError");
      if (error._tag !== "ProcessOutputLimitError") {
        return expect.fail("Expected ProcessOutputLimitError");
      }
      expect(error).toMatchObject({
        stream: "stdout",
        maxBytes: 128,
        observedBytes: 2048,
      });
      expect(error.message).toBe(
        "Process 'fake' stdout produced 2048 bytes, exceeding the 128 byte limit",
      );
    }),
  );

  it.effect("accepts output at the byte limit followed by an empty chunk", () =>
    Effect.gen(function* () {
      const output = new TextEncoder().encode("exactly");
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: Stream.make(output, new Uint8Array()),
          }),
        ),
      );

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["exact-limit"],
        maxOutputBytes: output.byteLength,
      });

      expect(result.stdout).toBe("exactly");
    }),
  );

  it.effect("fails fast on output limit before timeout for long-running output", () =>
    Effect.gen(function* () {
      const textChunk = "x".repeat(64);
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: Stream.fromIterable(Array.from({ length: 10 }, () => textChunk)).pipe(
              Stream.encodeText,
            ),
            exitCode: Effect.never,
          }),
        ),
      );

      const error = yield* runWith(spawner)({
        command: "fake",
        args: ["spam-stdout"],
        maxOutputBytes: 128,
        timeout: "2 seconds",
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(ProcessRunner.ProcessOutputLimitError);
    }),
  );

  it.effect("truncates output when outputMode is truncate", () =>
    Effect.gen(function* () {
      const spawner = makeSpawner(() => Effect.succeed(makeHandle({ stdout: "x".repeat(2048) })));

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["stdout-bytes", "2048"],
        maxOutputBytes: 128,
        outputMode: "truncate",
      });

      expect(result.code).toBe(0);
      expect(result.stdout.length).toBeLessThanOrEqual(128);
      expect(result.stdoutTruncated).toBe(true);
      expect(result.stderrTruncated).toBe(false);
    }),
  );

  it.effect("writes stdin before waiting for exit", () =>
    Effect.gen(function* () {
      const stdinWritten = yield* Deferred.make<void>();
      const decoder = new TextDecoder();
      const spawner = makeSpawner(() =>
        Effect.succeed(
          makeHandle({
            stdout: "stdin payload",
            stdin: Sink.forEach((chunk: Uint8Array) => {
              const text = decoder.decode(chunk, { stream: true });
              return text.includes("stdin payload")
                ? Deferred.succeed(stdinWritten, undefined)
                : Effect.void;
            }),
            exitCode: Deferred.await(stdinWritten).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
          }),
        ),
      );

      const result = yield* runWith(spawner)({
        command: "fake",
        args: ["stdin-echo"],
        stdin: "stdin payload",
      });

      expect(result.stdout).toBe("stdin payload");
      expect(result.code).toBe(0);
    }),
  );
});

describe("isWindowsCommandNotFound", () => {
  it.effect("matches the localized German cmd.exe error text", () =>
    Effect.gen(function* () {
      const isCommandNotFound = yield* isWindowsCommandNotFound(
        1,
        "wird nicht als interner oder externer Befehl, betriebsfahiges Programm oder Batch-Datei erkannt",
      ).pipe(Effect.provideService(HostProcess.Platform, "win32"));
      expect(isCommandNotFound).toBe(true);
    }),
  );
});

describe.skipIf(HostProcess.Platform.defaultValue() !== "linux")("real process IO", () => {
  describe.each([false, true])("hosted=%s", (hosted) => {
    const layer = ProcessRunner.layer.pipe(
      Layer.provideMerge(
        hosted
          ? SpawnHost.layerHost(
              process.execPath,
              [NodeURL.fileURLToPath(new URL("./bin.ts", import.meta.url)), "__spawn-host"],
              (failure) => {
                throw failure.cause;
              },
            )
          : NodeChildProcessSpawner.layer,
      ),
      Layer.provide(NodeServices.layer),
    );
    const run = (input: ProcessRunner.ProcessRunInput) =>
      Effect.flatMap(ProcessRunner.ProcessRunner, (runner) => runner.run(input));
    it.live("preserves large stdin, EOF, output order, stderr and exit code", () =>
      Effect.gen(function* () {
        const input = "0123456789abcdef\n".repeat(120_000);
        const output = yield* run({
          command: "sh",
          args: ["-c", "cat; printf done >&2; exit 3"],
          stdin: input,
          maxOutputBytes: 4_000_000,
        });
        expect(output).toMatchObject({ stdout: input, stderr: "done", code: 3, timedOut: false });
      }).pipe(Effect.provide(layer)),
    );
    it.live("retains immediate binary output and output produced before the first read", () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        for (let i = 0; i < 30; i++) {
          const handle = yield* spawner.spawn(
            ChildProcess.make("sh", ["-c", "printf '\\000\\377hello'; printf error >&2"], {
              stdin: "ignore",
            }),
          );
          yield* handle.exitCode;
          const bytes = yield* Stream.runCollect(handle.stdout);
          expect(Buffer.concat([...bytes])).toEqual(Buffer.from([0, 255, ...Buffer.from("hello")]));
          expect(yield* Stream.runCollect(Stream.decodeText(handle.stderr))).toEqual(["error"]);
        }
      }).pipe(Effect.scoped, Effect.provide(layer)),
    );
    it.live("preserves ENOENT classification and bounds output", () =>
      Effect.gen(function* () {
        const error = yield* run({
          command: "/definitely-missing-t3-spawn-test",
          args: ["arg"],
        }).pipe(Effect.flip);
        expect(error._tag).toBe("ProcessSpawnError");
        const limit = yield* run({ command: "yes", args: [], maxOutputBytes: 1000 }).pipe(
          Effect.flip,
        );
        expect(limit._tag).toBe("ProcessOutputLimitError");
        expect((yield* run({ command: "echo", args: ["after"] })).stdout).toBe("after\n");
      }).pipe(Effect.provide(layer)),
    );
    it.live("kills the detached process group on timeout", () =>
      Effect.gen(function* () {
        const output = yield* run({
          command: "sh",
          args: ["-c", "sleep 30 & wait"],
          timeout: "100 millis",
          timeoutBehavior: "timedOutResult",
        });
        expect(output.timedOut).toBe(true);
      }).pipe(Effect.provide(layer)),
    );
    it.live("supports extra duplex file descriptors and pipelines", () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const handle = yield* spawner.spawn(
          ChildProcess.make("sh", ["-c", "cat <&3 >&3"], {
            additionalFds: { fd3: { type: "duplex" } },
            stdin: "ignore",
          }),
        );
        const writer = yield* Stream.run(
          Stream.encodeText(Stream.make("fd payload")),
          handle.getInputFd(3),
        ).pipe(Effect.forkScoped);
        const output = yield* Stream.runCollect(Stream.decodeText(handle.getOutputFd(3)));
        yield* Fiber.join(writer);
        expect(output.join("")).toBe("fd payload");
        expect(yield* handle.exitCode).toBe(0);
        const piped = yield* spawner.spawn(
          ChildProcess.make("printf", ["hello"]).pipe(
            ChildProcess.pipeTo(ChildProcess.make("cat")),
          ),
        );
        expect((yield* Stream.runCollect(Stream.decodeText(piped.stdout))).join("")).toBe("hello");
      }).pipe(Effect.scoped, Effect.provide(layer)),
    );
  });
});

// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - IPC callbacks run outside the Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import type * as NodeNet from "node:net";
import type * as NodeStream from "node:stream";

import * as NodeChildProcessSpawner from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSelfInvocation, selfInvocationArgs } from "@t3tools/shared/nodeRuntime";

import {
  processIdentity,
  ownsProcessGroup,
  type SpawnHostError,
  type SpawnHostEvent,
  type SpawnHostRequest,
} from "./spawnHostProcess.ts";

const MAX_HOST_FAILURES = 3;
const HOST_READY_TIMEOUT_MS = 10_000;
const HOSTED_OPTION_KEYS = new Set(["cwd", "env", "stdio", "detached", "shell", "windowsHide"]);
// Inherited descriptors belong to the server, not the host's startup snapshot.
const HOSTED_STDIO = new Set<unknown>([undefined, null, "pipe", "ignore"]);
const canHostEnvironment = (env: NodeJS.ProcessEnv | undefined) => {
  if (env === undefined) return true;
  if (NodeUtil.types.isProxy(env)) return false;
  for (const key in env) {
    let owner = env;
    while (!Object.hasOwn(owner, key)) owner = Object.getPrototypeOf(owner);
    const descriptor = Object.getOwnPropertyDescriptor(owner, key)!;
    if (!("value" in descriptor)) return false;
    const value = descriptor.value;
    if (
      key.includes("\0") ||
      (value !== undefined && (typeof value !== "string" || value.includes("\0")))
    )
      return false;
  }
  return true;
};
const canHost = (options: NodeChildProcess.SpawnOptions) =>
  !NodeUtil.types.isProxy(options) &&
  (Object.getPrototypeOf(options) === Object.prototype ||
    Object.getPrototypeOf(options) === null) &&
  Object.values(Object.getOwnPropertyDescriptors(options)).every(
    (descriptor) => "value" in descriptor,
  ) &&
  Object.keys(options).every((key) => HOSTED_OPTION_KEYS.has(key)) &&
  (options.cwd === undefined || (typeof options.cwd === "string" && !options.cwd.includes("\0"))) &&
  canHostEnvironment(options.env) &&
  (options.detached === undefined || typeof options.detached === "boolean") &&
  (options.windowsHide === undefined || typeof options.windowsHide === "boolean") &&
  (options.shell === undefined ||
    typeof options.shell === "boolean" ||
    (typeof options.shell === "string" && !options.shell.includes("\0"))) &&
  Array.isArray(options.stdio) &&
  options.stdio.every((fd) => HOSTED_STDIO.has(fd));

const snapshotEnvironment = (env: NodeJS.ProcessEnv) => {
  const snapshot: NodeJS.ProcessEnv = Object.create(null);
  // Node includes inherited enumerable environment entries.
  for (const key in env) snapshot[key] = env[key];
  return snapshot;
};

const toSpawnError = (error: SpawnHostError) => Object.assign(new Error(error.message), error);
const killChild = (pid: number, detached: boolean, signal: NodeJS.Signals | number) => {
  try {
    process.kill(detached ? -pid : pid, signal);
  } catch {
    /* Already gone. */
  }
};

export interface SpawnHostFailure {
  readonly failures: number;
  readonly disabled: boolean;
  readonly cause: unknown;
}

class HostedChildProcess
  extends NodeEvents.EventEmitter
  implements NodeChildProcessSpawner.SpawnedChildProcess
{
  pid: number | undefined;
  exitCode: number | null = null;
  signalCode: NodeJS.Signals | null = null;
  stdin: NodeStream.Writable | null = null;
  stdout: NodeStream.Readable | null = null;
  stderr: NodeStream.Readable | null = null;
  stdio: Array<NodeStream.Readable | NodeStream.Writable | null | undefined> = [];
  referenced = true;
  exited = false;
  spawned = false;
  birth: string | undefined;
  readonly id: number;
  readonly detached: boolean;
  readonly #client: SpawnHostClient;

  constructor(client: SpawnHostClient, id: number, detached: boolean) {
    super();
    this.#client = client;
    this.id = id;
    this.detached = detached;
  }

  kill(signal: NodeJS.Signals | number = "SIGTERM"): boolean {
    // Reuse Node's own signal validation and error fields without a native process handle.
    NodeChildProcess.ChildProcess.prototype.kill.call({} as NodeChildProcess.ChildProcess, signal);
    signal = typeof signal === "string" ? (signal.toUpperCase() as NodeJS.Signals) : signal;
    if (this.exited) return false;
    if (!this.spawned) {
      // IPC preserves request order: cancellation also works before the pid arrives.
      this.#client.send({ type: "kill", id: this.id, signal });
      return true;
    }
    try {
      process.kill(this.pid!, signal);
      return true;
    } catch {
      return false;
    }
  }
  ref(): void {
    this.#client.setReferenced(this, true);
  }
  unref(): void {
    this.#client.setReferenced(this, false);
  }

  attachStdio(fd: number, socket: NodeNet.Socket): void {
    this.stdio[fd] = socket;
    // Input writes may fail after Effect has interrupted its sink.
    if (fd === 0) {
      socket.on("error", () => {});
      this.stdin = socket;
    } else if (fd === 1) this.stdout = socket;
    else if (fd === 2) this.stderr = socket;
  }
  markSpawned(pid: number): void {
    this.pid = pid;
    this.spawned = true;
    this.emit("spawn");
  }
  markExited(code: number | null, signal: NodeJS.Signals | null): void {
    this.exitCode = code;
    this.signalCode = signal;
    this.#client.settle(this);
    this.stdin?.destroy();
    this.emit("exit", code, signal);
    // Match Node's flushStdio once the spawner has installed its output pipes.
    process.nextTick(() => {
      for (const stream of this.stdio) {
        if (
          stream &&
          "resume" in stream &&
          stream.readable &&
          stream.listenerCount("data") === 0 &&
          stream.listenerCount("readable") === 0
        )
          stream.resume();
      }
    });
  }
  markFailed(error: Error): void {
    this.#client.settle(this);
    for (const stream of this.stdio) stream?.destroy();
    this.emit("error", error);
  }
}

/** Owns a small builtin-only host; command output travels through transferred sockets. */
export class SpawnHostClient {
  readonly #command: string;
  readonly #args: ReadonlyArray<string>;
  readonly #onHostFailure: (failure: SpawnHostFailure) => void;
  readonly #children = new Map<number, HostedChildProcess>();
  readonly #survivors = new Map<number, { birth: string; detached: boolean }>();
  readonly #onExit = () => {
    this.#stopSurvivors();
    for (const child of this.#children.values()) {
      if (child.pid !== undefined) killChild(child.pid, child.detached, "SIGKILL");
    }
  };
  #host: NodeChildProcess.ChildProcess | undefined;
  readonly #hosts = new Map<NodeChildProcess.ChildProcess, Promise<void>>();
  #ready = false;
  #closed = false;
  #failures = 0;
  #nextId = 0;
  #referencedChildren = 0;
  #readyTimer: NodeJS.Timeout | undefined;
  ready: Promise<void> = Promise.resolve();
  #resolveReady: () => void = () => {};

  constructor(
    command: string,
    args: ReadonlyArray<string>,
    onHostFailure: (failure: SpawnHostFailure) => void,
  ) {
    this.#command = command;
    this.#args = args;
    this.#onHostFailure = onHostFailure;
    process.once("exit", this.#onExit);
    this.#start();
  }

  readonly spawn = (
    command: string,
    args: ReadonlyArray<string>,
    options: NodeChildProcess.SpawnOptions,
  ): NodeChildProcessSpawner.SpawnedChildProcess => {
    if (
      this.#closed ||
      !this.#ready ||
      !this.#host?.connected ||
      !canHost(options) ||
      typeof command !== "string" ||
      !command ||
      command.includes("\0") ||
      args.some((arg) => typeof arg !== "string" || arg.includes("\0"))
    ) {
      if (!this.#closed && this.#failures < MAX_HOST_FAILURES) this.#start();
      return NodeChildProcess.spawn(command, [...args], options);
    }
    const child = new HostedChildProcess(this, ++this.#nextId, options.detached === true);
    this.#children.set(child.id, child);
    this.#referencedChildren++;
    this.#syncChannelRef();
    this.send({
      type: "spawn",
      id: child.id,
      command,
      args,
      // Host cwd/env are a startup snapshot; resolve defaults in the caller.
      options: {
        ...options,
        cwd: options.cwd ? NodePath.resolve(options.cwd as string) : process.cwd(),
        env: snapshotEnvironment(options.env ?? process.env),
      },
    });
    return child;
  };

  send(request: SpawnHostRequest): void {
    const host = this.#host;
    if (!host?.connected) return;
    try {
      host.send(request, (error) => {
        if (error) this.#lose(host, error);
      });
    } catch (cause) {
      // Keep errors asynchronous, as with Node's spawn event, so callers can subscribe.
      process.nextTick(() => this.#lose(host, cause));
    }
  }
  settle(child: HostedChildProcess): void {
    if (child.exited) return;
    child.exited = true;
    this.#children.delete(child.id);
    if (child.referenced) this.#referencedChildren--;
    this.#syncChannelRef();
  }
  setReferenced(child: HostedChildProcess, referenced: boolean): void {
    if (child.referenced === referenced) return;
    child.referenced = referenced;
    if (!referenced && child.pid !== undefined) child.birth = processIdentity(child.pid)?.birth;
    if (!child.exited) this.#referencedChildren += referenced ? 1 : -1;
    this.#syncChannelRef();
  }
  async close(): Promise<void> {
    if (!this.#closed) {
      this.#closed = true;
      this.#stopSurvivors();
      process.removeListener("exit", this.#onExit);
      const host = this.#host;
      if (host) this.#lose(host, new Error("The spawn host scope closed."));
    }
    await Promise.all(
      [...this.#hosts].map(async ([host, closed]) => {
        host.ref();
        const timer = setTimeout(() => host.kill("SIGKILL"), 1500);
        timer.unref();
        try {
          await closed;
        } finally {
          clearTimeout(timer);
        }
      }),
    );
  }
  #stopSurvivors(): void {
    for (const [pid, { birth, detached }] of this.#survivors) {
      if (processIdentity(pid)?.birth === birth || (detached && ownsProcessGroup(pid, birth))) {
        killChild(pid, detached, "SIGKILL");
      }
    }
    this.#survivors.clear();
  }
  #syncChannelRef(): void {
    const channel = this.#host?.channel;
    if (!channel) return;
    if (!this.#ready || this.#referencedChildren > 0) channel.ref();
    else channel.unref();
  }
  #start(): void {
    if (this.#host || this.#closed || this.#failures >= MAX_HOST_FAILURES) return;
    this.ready = new Promise((resolve) => {
      this.#resolveReady = resolve;
    });
    const env = { ...process.env };
    delete env.NODE_OPTIONS;
    let host: NodeChildProcess.ChildProcess;
    try {
      host = NodeChildProcess.spawn(this.#command, [...this.#args], {
        env,
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      });
    } catch (cause) {
      this.#failed(cause);
      this.#resolveReady();
      return;
    }
    this.#host = host;
    this.#hosts.set(
      host,
      new Promise((resolve) => {
        const finished = () => {
          this.#hosts.delete(host);
          resolve();
        };
        host.once("exit", finished);
        host.once("error", () => {
          if (host.pid === undefined) finished();
        });
      }),
    );
    host.unref();
    this.#syncChannelRef();
    this.#readyTimer = setTimeout(() => {
      host.kill("SIGKILL");
      this.#lose(host, new Error("The spawn host did not start in time."));
    }, HOST_READY_TIMEOUT_MS);
    this.#readyTimer.unref();
    host.on("message", (event, socket) => {
      if (host === this.#host)
        this.#onEvent(event as SpawnHostEvent, socket as NodeNet.Socket | undefined);
      else (socket as NodeNet.Socket | undefined)?.destroy();
    });
    host.on("error", (cause) => this.#lose(host, cause));
    // Exit can precede buffered IPC messages; close follows the final message.
    host.once("close", (code, signal) =>
      this.#lose(host, new Error(`The spawn host exited (code ${code}, signal ${signal}).`)),
    );
    host.once("disconnect", () => {
      this.#ready = false;
    });
  }
  #onEvent(event: SpawnHostEvent, socket: NodeNet.Socket | undefined): void {
    if (event.type === "ready") {
      clearTimeout(this.#readyTimer);
      this.#ready = true;
      this.#syncChannelRef();
      this.#resolveReady();
      return;
    }
    const child = this.#children.get(event.id);
    if (!child) {
      socket?.destroy();
      return;
    }
    switch (event.type) {
      case "stdio":
        if (socket) child.attachStdio(event.fd, socket);
        return;
      case "started":
        child.pid = event.pid;
        if (!child.referenced) child.birth = processIdentity(event.pid)?.birth;
        this.send({ type: "handoff", id: event.id });
        return;
      case "spawn":
        child.markSpawned(event.pid);
        this.send({ type: "ack", id: event.id });
        return;
      case "exit":
        child.markExited(event.code, event.signal);
        return;
      case "error":
        child.markFailed(toSpawnError(event.error));
        return;
    }
  }
  #failed(cause: unknown): void {
    this.#failures++;
    this.#onHostFailure({
      failures: this.#failures,
      disabled: this.#failures >= MAX_HOST_FAILURES,
      cause,
    });
  }
  #lose(host: NodeChildProcess.ChildProcess, cause: unknown): void {
    if (host !== this.#host) return;
    clearTimeout(this.#readyTimer);
    this.#host = undefined;
    this.#ready = false;
    if (host.connected) host.disconnect();
    this.#resolveReady();
    if (!this.#closed) this.#failed(cause);
    for (const child of this.#children.values()) {
      if (
        !this.#closed &&
        child.spawned &&
        !child.referenced &&
        child.pid !== undefined &&
        child.birth !== undefined
      ) {
        // Detached launchers survive host loss, but still belong to the server's lifetime.
        this.#survivors.set(child.pid, { birth: child.birth, detached: child.detached });
        child.markExited(null, null);
        continue;
      }
      if (child.pid !== undefined) killChild(child.pid, child.detached, "SIGKILL");
      if (child.spawned) {
        child.markExited(null, "SIGKILL");
      } else {
        // Retrying a sent request could execute a side effect twice.
        child.markFailed(new Error("The spawn host exited before reporting the spawn."));
      }
    }
  }
}

export const layerHost = (
  command: string,
  args: ReadonlyArray<string>,
  onHostFailure: (failure: SpawnHostFailure) => void,
) =>
  Layer.unwrap(
    Effect.acquireRelease(
      Effect.sync(() => new SpawnHostClient(command, args, onHostFailure)),
      (client) => Effect.promise(() => client.close()),
    ).pipe(
      Effect.flatMap((client) =>
        Effect.promise(() => client.ready).pipe(
          Effect.as(NodeChildProcessSpawner.layerWithSpawn(client.spawn)),
        ),
      ),
    ),
  );

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    if ((yield* HostProcess.Platform) !== "linux") return NodeChildProcessSpawner.layer;
    const invocation = yield* resolveSelfInvocation();
    const runFork = Effect.runForkWith(yield* Effect.context<never>());
    return layerHost(
      invocation.command,
      selfInvocationArgs(invocation, ["__spawn-host"]),
      ({ failures, disabled, cause }) =>
        runFork(
          Effect.logWarning(
            disabled
              ? "The spawn host failed; child processes now start from the server."
              : "The spawn host stopped; it restarts for the next child process.",
            { failures, cause },
          ),
        ),
    );
  }),
);

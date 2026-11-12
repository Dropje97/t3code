// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - this process deliberately imports only Node builtins.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import type * as NodeNet from "node:net";

interface SpawnRequest {
  readonly type: "spawn";
  readonly id: number;
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly options: NodeChildProcess.SpawnOptions;
}
export type SpawnHostRequest =
  | SpawnRequest
  | { readonly type: "kill"; readonly id: number; readonly signal: NodeJS.Signals | number }
  | { readonly type: "handoff" | "ack"; readonly id: number };

export interface SpawnHostError {
  readonly message: string;
  readonly code?: string | undefined;
  readonly errno?: number | undefined;
  readonly syscall?: string | undefined;
  readonly path?: string | undefined;
  readonly spawnargs?: ReadonlyArray<string> | undefined;
}

/** started/handshake precedes stdio/spawn/ack; exit follows spawn, even for immediate exits. */
export type SpawnHostEvent =
  | { readonly type: "ready" }
  | { readonly type: "stdio"; readonly id: number; readonly fd: number }
  | { readonly type: "started" | "spawn"; readonly id: number; readonly pid: number }
  | {
      readonly type: "exit";
      readonly id: number;
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
    }
  | { readonly type: "error"; readonly id: number; readonly error: SpawnHostError };

const toSpawnHostError = (cause: unknown): SpawnHostError => {
  const error = cause as Partial<NodeJS.ErrnoException> & { spawnargs?: ReadonlyArray<string> };
  return {
    message: typeof error?.message === "string" ? error.message : String(cause),
    code: error?.code,
    errno: error?.errno,
    syscall: error?.syscall,
    path: error?.path,
    spawnargs: error?.spawnargs,
  };
};
export const processIdentity = (pid: number) => {
  try {
    const stat = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
    const fields = stat.slice(stat.lastIndexOf(") ") + 2).split(" ");
    return { group: Number(fields[2]), session: Number(fields[3]), birth: fields[19]! };
  } catch {
    return undefined;
  }
};
export const ownsProcessGroup = (group: number, birth: string) => {
  // A replacement leader proves that the numeric group id has been reused.
  const leader = processIdentity(group);
  if (leader && leader.birth !== birth) return false;
  return NodeFS.readdirSync("/proc").some((name) => {
    if (!/^\d+$/.test(name)) return false;
    const current = processIdentity(Number(name));
    return (
      current?.group === group &&
      current.session === group &&
      BigInt(current.birth) >= BigInt(birth)
    );
  });
};

interface HostChild {
  readonly child: NodeChildProcess.ChildProcess;
  readonly detached: boolean;
  readonly stdio: Array<NodeNet.Socket | null>;
  acknowledged: boolean;
  readonly birth: string | undefined;
  exit?: { code: number | null; signal: NodeJS.Signals | null };
}

export function runSpawnHost(): void {
  const channel = process.send?.bind(process);
  if (!channel) {
    process.stderr.write("t3 __spawn-host is started by the T3 server.\n");
    process.exitCode = 1;
    return;
  }
  const children = new Map<number, HostChild>();
  const groups = new Map<number, string>();
  const queue: SpawnRequest[] = [];
  const pendingKills = new Map<number, NodeJS.Signals | number>();
  let transferring: number | undefined;
  let closing = false;
  const kill = (pid: number, detached: boolean, signal: NodeJS.Signals | number) => {
    try {
      process.kill(detached ? -pid : pid, signal);
    } catch {
      /* Already gone. */
    }
  };
  const shutdown = () => {
    if (closing) return;
    closing = true;
    queue.length = 0;
    pendingKills.clear();
    for (const [id, entry] of children) {
      if (!entry.exit && entry.child.pid) kill(entry.child.pid, entry.detached, "SIGKILL");
      for (const socket of entry.stdio) socket?.destroy();
      if (entry.exit) children.delete(id);
    }
    // A numeric group id can be reused after its last member exits.
    for (const [pid, birth] of groups) if (ownsProcessGroup(pid, birth)) kill(pid, true, "SIGKILL");
    if (children.size === 0) process.exit(0);
    setTimeout(() => process.exit(0), 1000).unref();
  };
  const send = (event: SpawnHostEvent, socket?: NodeNet.Socket) => {
    if (closing || !process.connected) {
      socket?.destroy();
      shutdown();
      return;
    }
    try {
      channel(event, socket, (error: Error | null) => {
        if (error) shutdown();
      });
    } catch {
      socket?.destroy();
      shutdown();
    }
  };
  const reportExit = (id: number, entry: HostChild) => {
    if (!entry.exit || !entry.acknowledged) return;
    children.delete(id);
    send({ type: "exit", id, ...entry.exit });
  };
  const pump = () => {
    if (closing || transferring !== undefined) return;
    const request = queue.shift();
    if (!request) return;
    const { id } = request;
    transferring = id;
    // Retain only groups whose descendants survived their leader.
    for (const [pid, birth] of groups) {
      try {
        process.kill(-pid, 0);
        const leader = processIdentity(pid);
        if (leader && leader.birth !== birth) groups.delete(pid);
      } catch {
        groups.delete(pid);
      }
    }
    let child: NodeChildProcess.ChildProcess;
    try {
      child = NodeChildProcess.spawn(request.command, [...request.args], request.options);
    } catch (cause) {
      pendingKills.delete(id);
      transferring = undefined;
      send({ type: "error", id, error: toSpawnHostError(cause) });
      setImmediate(pump);
      return;
    }
    const stdio = child.stdio.map((socket) => {
      if (socket) {
        // Buffered JS bytes cannot transfer with the fd; stop native reads before the loop polls.
        (socket as unknown as { _handle?: { readStop?: () => void } | null })._handle?.readStop?.();
        socket.on("error", () => {});
      }
      return socket as NodeNet.Socket | null;
    });
    const entry: HostChild = {
      child,
      detached: request.options.detached === true,
      stdio,
      acknowledged: false,
      birth: child.pid === undefined ? undefined : processIdentity(child.pid)?.birth,
    };
    children.set(id, entry);
    // Prevent ChildProcess's exit-time destroy/flush from touching queued transfers.
    child.stdin = null;
    child.stdio.fill(null);
    child.on("error", (cause) => {
      children.delete(id);
      pendingKills.delete(id);
      for (const socket of stdio) socket?.destroy();
      send({ type: "error", id, error: toSpawnHostError(cause) });
      if (transferring === id) transferring = undefined;
      if (closing) {
        if (children.size === 0) process.exit(0);
      } else pump();
    });
    child.once("spawn", () => {
      if (closing) {
        if (child.pid) kill(child.pid, entry.detached, "SIGKILL");
        return;
      }
      const signal = pendingKills.get(id);
      pendingKills.delete(id);
      if (signal !== undefined) kill(child.pid!, entry.detached, signal);
      // Learn the pid before any handle-transfer congestion; only one handoff is in flight.
      send({ type: "started", id, pid: child.pid! });
    });
    child.on("exit", (code, signal) => {
      entry.exit = { code, signal };
      if (entry.detached && child.pid) {
        try {
          process.kill(-child.pid, 0);
          if (entry.birth !== undefined) groups.set(child.pid, entry.birth);
        } catch {
          /* No descendants. */
        }
      }
      if (closing) {
        children.delete(id);
        if (children.size === 0) process.exit(0);
        return;
      }
      reportExit(id, entry);
    });
  };
  // Effect releases normal scopes on signals; IPC EOF is the owner-death signal.
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as const) process.on(signal, () => {});
  process.on("disconnect", shutdown);
  process.on("error", shutdown);
  process.on("message", (request: SpawnHostRequest) => {
    if (closing) return;
    if (request.type === "spawn") {
      queue.push(request);
      pump();
      return;
    }
    const entry = children.get(request.id);
    if (request.type === "kill") {
      if (entry?.child.pid && !entry.exit) kill(entry.child.pid, entry.detached, request.signal);
      else if (queue.some((queued) => queued.id === request.id))
        pendingKills.set(request.id, request.signal);
    } else if (request.type === "handoff" && entry) {
      entry.stdio.forEach((socket, fd) => {
        if (socket) send({ type: "stdio", id: request.id, fd }, socket);
      });
      send({ type: "spawn", id: request.id, pid: entry.child.pid! });
    } else if (request.type === "ack" && entry) {
      entry.acknowledged = true;
      reportExit(request.id, entry);
      if (transferring === request.id) transferring = undefined;
      pump();
    }
  });
  send({ type: "ready" });
}

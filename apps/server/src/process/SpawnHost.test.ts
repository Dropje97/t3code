// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - tests exercise native IPC and lifecycle events.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import type { SpawnedChildProcess } from "@effect/platform-node-shared/NodeChildProcessSpawner";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { vi } from "vite-plus/test";
import { describe, expect, it } from "@effect/vitest";
import { SpawnHostClient, type SpawnHostFailure } from "./SpawnHost.ts";

const bin = NodeURL.fileURLToPath(new URL("../bin.ts", import.meta.url));
const options: NodeChildProcess.SpawnOptions = { stdio: ["pipe", "pipe", "pipe"], detached: true };
const nodeParent = "process.stdout.write(String(process.ppid))";
const collect = async (stream: SpawnedChildProcess["stdout"]) => {
  const chunks: Buffer[] = [];
  if (stream) for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
};
const run = async (client: SpawnHostClient, command: string, args: string[], opts = options) => {
  const child = client.spawn(command, args, opts);
  const exited = NodeEvents.EventEmitter.once(child, "exit");
  await NodeEvents.EventEmitter.once(child, "spawn");
  const [stdout, stderr, [code, signal]] = await Promise.all([
    collect(child.stdout),
    collect(child.stderr),
    exited,
  ]);
  return { stdout: stdout.toString(), stderr: stderr.toString(), code, signal, child };
};
const alive = (pid: number) => {
  try {
    // A killed orphan may temporarily remain a zombie until the container's init reaps it.
    return NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]?.[0] !== "Z";
  } catch {
    return false;
  }
};

describe.skipIf(HostProcess.Platform.defaultValue() !== "linux")("SpawnHost", () => {
  it("hosts ordinary children, preserves cwd/env, and falls back for unsupported options", async () => {
    const failures: SpawnHostFailure[] = [];
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], (f) =>
      failures.push(f),
    );
    const cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "spawn-host-"));
    try {
      // An unavailable host never queues the command.
      const early = await run(client, process.execPath, ["-e", nodeParent]);
      expect(Number(early.stdout)).toBe(process.pid);
      await client.ready;
      const hosted = await run(client, process.execPath, ["-e", nodeParent]);
      expect(Number(hosted.stdout)).not.toBe(process.pid);
      const hostPid = Number(hosted.stdout);
      const inherited = await run(
        client,
        process.execPath,
        ["-e", "console.log(process.env.INHERITED, process.env.OWN)"],
        { ...options, env: Object.assign(Object.create({ INHERITED: "yes" }), { OWN: "yes" }) },
      );
      expect(inherited.stdout).toBe("yes yes\n");
      const state = await run(
        client,
        process.execPath,
        ["-e", "console.log(JSON.stringify([process.cwd(), process.env.FOO, process.env.PATH]))"],
        { ...options, cwd, env: { FOO: "bar" } },
      );
      expect(JSON.parse(state.stdout)).toEqual([cwd, "bar", null]);
      const relative = await run(client, process.execPath, ["-e", "console.log(process.cwd())"], {
        ...options,
        cwd: NodePath.relative(process.cwd(), cwd),
      });
      expect(relative.stdout.trim()).toBe(cwd);
      const old = process.env.T3_SPAWN_HOST_TEST_ENV;
      try {
        process.env.T3_SPAWN_HOST_TEST_ENV = "changed-after-host-start";
        expect(
          (
            await run(client, process.execPath, [
              "-e",
              "console.log(process.env.T3_SPAWN_HOST_TEST_ENV)",
            ])
          ).stdout.trim(),
        ).toBe("changed-after-host-start");
      } finally {
        if (old === undefined) delete process.env.T3_SPAWN_HOST_TEST_ENV;
        else process.env.T3_SPAWN_HOST_TEST_ENV = old;
      }
      for (const extra of [
        { argv0: "different-argv" },
        { cwd: NodeURL.pathToFileURL(cwd) },
        { stdio: ["ignore", "pipe", "inherit"] as NodeChildProcess.StdioOptions },
        { stdio: ["ignore", "pipe", 2] as NodeChildProcess.StdioOptions },
      ]) {
        const fallback = await run(client, process.execPath, ["-e", nodeParent], {
          ...options,
          ...extra,
        });
        expect(Number(fallback.stdout)).toBe(process.pid);
      }
      const signalled = client.spawn("sleep", ["30"], options);
      const exit = NodeEvents.EventEmitter.once(signalled, "exit");
      await NodeEvents.EventEmitter.once(signalled, "spawn");
      expect(signalled.exitCode).toBe(null);
      expect(signalled.signalCode).toBe(null);
      expect(signalled.kill("sigterm" as NodeJS.Signals)).toBe(true);
      expect(await exit).toEqual([null, "SIGTERM"]);
      expect(signalled.signalCode).toBe("SIGTERM");
      expect(signalled.kill()).toBe(false);
      expect(failures).toEqual([]);
      await client.close();
      expect(alive(hostPid)).toBe(false);
    } finally {
      await client.close();
      NodeFS.rmSync(cwd, { recursive: true });
    }
  });

  it("reconstructs the same ENOENT error fields as direct spawn", async () => {
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], () => {});
    try {
      await client.ready;
      for (const opts of [options, { ...options, argv0: "fallback" }]) {
        const hosted = client.spawn("/missing-spawn-host-test", ["first", "second"], opts);
        const direct = NodeChildProcess.spawn(
          "/missing-spawn-host-test",
          ["first", "second"],
          opts,
        );
        const [[actual], [expected]] = await Promise.all([
          NodeEvents.EventEmitter.once(hosted, "error"),
          NodeEvents.EventEmitter.once(direct, "error"),
        ]);
        for (const key of ["message", "code", "errno", "syscall", "path", "spawnargs"])
          expect(actual[key]).toEqual(expected[key]);
      }
    } finally {
      await client.close();
    }
  });

  it("kills before pid acknowledgement and cleans up children when closing", async () => {
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], () => {});
    try {
      await client.ready;
      const pending = client.spawn("sleep", ["30"], options);
      const exited = NodeEvents.EventEmitter.once(pending, "exit");
      expect(pending.pid).toBeUndefined();
      expect(() => pending.kill("BAD" as NodeJS.Signals)).toThrow(
        expect.objectContaining({ code: "ERR_UNKNOWN_SIGNAL" }),
      );
      expect(pending.kill("SIGKILL")).toBe(true);
      expect(await exited).toEqual([null, "SIGKILL"]);
      expect(alive(pending.pid!)).toBe(false);
      const running = client.spawn("sleep", ["30"], options);
      const stopped = NodeEvents.EventEmitter.once(running, "exit");
      await NodeEvents.EventEmitter.once(running, "spawn");
      running.unref();
      running.ref();
      running.unref();
      await client.close();
      await stopped;
      expect(alive(running.pid!)).toBe(false);
      // Closing disables hosting but retains the direct-spawn contract.
      expect(Number((await run(client, process.execPath, ["-e", nodeParent])).stdout)).toBe(
        process.pid,
      );
    } finally {
      await client.close();
    }
  });

  it("cleans up a group after its original surviving member leaves that group", async () => {
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], () => {});
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "spawn-host-group-"));
    const middleFile = NodePath.join(dir, "middle"),
      tailFile = NodePath.join(dir, "tail");
    let middle: number | undefined, tail: number | undefined;
    let watcher: NodeFS.FSWatcher | undefined;
    try {
      await client.ready;
      const middleScript = `
        const cp=require("node:child_process"),fs=require("node:fs");
        fs.writeFileSync(${JSON.stringify(middleFile)},String(process.pid));
        process.on("SIGUSR1",()=>{
          const tail=cp.spawn("sleep",["30"],{stdio:"ignore"});
          tail.once("spawn",()=>{
            fs.writeFileSync(${JSON.stringify(tailFile)},String(tail.pid));
            process.execve("/usr/bin/setsid",["setsid","sleep","30"],process.env);
          });
        });
        process.send("ready");
        setInterval(()=>{},1000);
      `;
      const leaderScript = `
        const child=require("node:child_process").spawn(process.execPath,["-e",${JSON.stringify(middleScript)}],{stdio:["ignore","ignore","ignore","ipc"]});
        child.once("message",()=>process.exit(0));
      `;
      const leader = client.spawn(process.execPath, ["-e", leaderScript], {
        stdio: ["ignore", "ignore", "ignore"],
        detached: true,
      });
      await NodeEvents.EventEmitter.once(leader, "exit");
      middle = Number(NodeFS.readFileSync(middleFile, "utf8"));
      const ready = new Promise<void>((resolve) => {
        watcher = NodeFS.watch(dir, () => {
          if (NodeFS.existsSync(tailFile)) resolve();
        });
      });
      process.kill(middle, "SIGUSR1");
      await ready;
      tail = Number(NodeFS.readFileSync(tailFile, "utf8"));
      await vi.waitFor(() =>
        expect(
          Number(NodeFS.readFileSync(`/proc/${middle}/stat`, "utf8").split(") ")[1]!.split(" ")[2]),
        ).toBe(middle),
      );
      await client.close();
      await vi.waitFor(() => expect(alive(tail!)).toBe(false));
    } finally {
      watcher?.close();
      for (const pid of [middle, tail])
        if (pid !== undefined && alive(pid)) process.kill(pid, "SIGKILL");
      await client.close();
      NodeFS.rmSync(dir, { recursive: true });
    }
  });

  it("fails acquisition when the host dies after reporting a pid but before stdio", async () => {
    const script = `
      const {spawn}=require("node:child_process");
      process.on("message", (request) => {
        if(request.type !== "spawn") return;
        const child=spawn("sleep",["30"],{stdio:"ignore",detached:true});
        child.once("spawn", () => process.send({type:"started",id:request.id,pid:child.pid}, () => process.exit(0)));
      });
      process.send({type:"ready"});
    `;
    const client = new SpawnHostClient(process.execPath, ["-e", script], () => {});
    try {
      await client.ready;
      const child = client.spawn("sleep", ["30"], options);
      const [error] = await NodeEvents.EventEmitter.once(child, "error");
      expect(error.message).toBe("The spawn host exited before reporting the spawn.");
      expect(child.pid).toBeDefined();
      await vi.waitFor(() => expect(alive(child.pid!)).toBe(false));
    } finally {
      await client.close();
    }
  });

  it("cancels requests still queued behind another stdio handoff", async () => {
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], () => {});
    try {
      await client.ready;
      const pending = Array.from({ length: 20 }, () => {
        const child = client.spawn("sleep", ["30"], options);
        const exited = NodeEvents.EventEmitter.once(child, "exit");
        expect(child.pid).toBeUndefined();
        expect(child.kill("SIGKILL")).toBe(true);
        return { child, exited };
      });
      for (const { child, exited } of pending) {
        expect(await exited).toEqual([null, "SIGKILL"]);
        expect(alive(child.pid!)).toBe(false);
      }
    } finally {
      await client.close();
    }
  });

  it("bounds the ready deadline of an unresponsive host", async () => {
    vi.useFakeTimers();
    const failures: SpawnHostFailure[] = [];
    const client = new SpawnHostClient(
      process.execPath,
      ["-e", "setInterval(() => {}, 1000)"],
      (f) => failures.push(f),
    );
    try {
      await vi.advanceTimersByTimeAsync(10_000);
      await client.ready;
      expect(failures).toHaveLength(1);
      expect(failures[0]).toMatchObject({ failures: 1, disabled: false });
    } finally {
      vi.useRealTimers();
      await client.close();
    }
  });

  it("stops known children on host loss, falls back, and disables after three failures", async () => {
    const events = new NodeEvents.EventEmitter();
    const failures: SpawnHostFailure[] = [];
    const client = new SpawnHostClient(process.execPath, [bin, "__spawn-host"], (f) => {
      failures.push(f);
      events.emit("failure");
    });
    let survivor: SpawnedChildProcess | undefined;
    try {
      await client.ready;
      survivor = client.spawn("sleep", ["30"], {
        stdio: ["ignore", "ignore", "ignore"],
        detached: true,
      });
      await NodeEvents.EventEmitter.once(survivor, "spawn");
      survivor.unref();
      for (let failure = 1; failure <= 3; failure++) {
        const hosted = await run(client, process.execPath, ["-e", nodeParent]);
        const hostPid = Number(hosted.stdout);
        expect(hostPid).not.toBe(process.pid);
        const running = client.spawn("sleep", ["30"], options);
        const stopped = NodeEvents.EventEmitter.once(running, "exit");
        await NodeEvents.EventEmitter.once(running, "spawn");
        const failed = NodeEvents.EventEmitter.once(events, "failure");
        process.kill(hostPid, "SIGKILL");
        await failed;
        await stopped;
        expect(alive(survivor.pid!)).toBe(true);
        await vi.waitFor(() => expect(alive(running.pid!)).toBe(false));
        expect(failures.at(-1)).toMatchObject({ failures: failure, disabled: failure === 3 });
        const fallback = await run(client, process.execPath, ["-e", nodeParent]);
        expect(Number(fallback.stdout)).toBe(process.pid);
        if (failure < 3) {
          await client.ready;
        }
      }
      expect(Number((await run(client, process.execPath, ["-e", nodeParent])).stdout)).toBe(
        process.pid,
      );
      expect(failures).toHaveLength(3);
    } finally {
      await client.close();
      if (survivor?.pid) await vi.waitFor(() => expect(alive(survivor!.pid!)).toBe(false));
    }
  });
});

describe.skipIf(HostProcess.Platform.defaultValue() !== "linux")("host process lifetime", () => {
  it.each([true, false])(
    "referenced=%s preserves natural parent lifetime and cleans up on IPC EOF",
    async (referenced) => {
      const moduleUrl = new URL("./SpawnHost.ts", import.meta.url).href;
      const script = `
      import { SpawnHostClient } from ${JSON.stringify(moduleUrl)};
      import { readFileSync } from "node:fs";
      const client = new SpawnHostClient(process.execPath, [${JSON.stringify(bin)}, "__spawn-host"], () => {});
      await client.ready;
      const child = client.spawn("sleep", ["30"], {stdio:["ignore","ignore","ignore"],detached:true});
      let exited = false;
      child.on("exit", () => { exited = true; });
      child.once("spawn", () => {
        const hostPid = Number(readFileSync("/proc/" + child.pid + "/stat", "utf8").split(") ")[1].split(" ")[1]);
        console.log(JSON.stringify({childPid:child.pid,hostPid}));
        child.unref();
        if (${referenced}) child.ref();
      });
      process.once("beforeExit", () => console.log(JSON.stringify({exited})));
    `;
      const parent = NodeChildProcess.spawn(
        process.execPath,
        ["--input-type=module", "-e", script],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      const closed = NodeEvents.EventEmitter.once(parent, "close");
      let text = "";
      let ids: { childPid: number; hostPid: number } | undefined;
      parent.stdout.on("data", (chunk) => {
        text += chunk;
        if (!ids && text.includes("\n")) {
          ids = JSON.parse(text.split("\n")[0]!);
          if (referenced) process.kill(ids!.childPid, "SIGTERM");
        }
      });
      const stderr = collect(parent.stderr);
      try {
        expect((await closed)[0]).toBe(0);
        expect((await stderr).toString()).toBe("");
        expect(JSON.parse(text.trim().split("\n").at(-1)!)).toEqual({ exited: referenced });
        expect(ids).toBeDefined();
        expect(alive(ids!.hostPid)).toBe(false);
        expect(alive(ids!.childPid)).toBe(false);
      } finally {
        if (parent.exitCode === null) parent.kill("SIGKILL");
      }
    },
  );
});

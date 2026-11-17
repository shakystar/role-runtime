import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";

const here = dirname(fileURLToPath(import.meta.url));
const fixture = JSON.parse(readFileSync(join(here, "../fixtures/role-switch.json"), "utf8"));
const cli = join(here, "node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js");

// Test driver, deliberately sequential. Production RPC multiplexing is not implemented.
class RPC {
  constructor(cwd, agentDir, log, scopeTools) {
    this.child = spawn(process.execPath, [cli, "--mode", "rpc", "--offline", "--approve",
      "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files",
      "-e", join(here, "fake-provider.mjs"), "--provider", "role-spike", "--model", "deterministic"], {
      cwd, env: { PATH: process.env.PATH, HOME: agentDir, PI_CODING_AGENT_DIR: agentDir,
        PI_OFFLINE: "1", ROLE_SPIKE_LOG: log, ROLE_SPIKE_SCOPE_TOOLS: scopeTools ? "1" : "0",
        ROLE_SPIKE_FIXTURE: join(here, "../fixtures/role-switch.json") }, stdio: ["pipe", "pipe", "pipe"],
    });
    this.records = [];
    this.stderr = "";
    this.nextID = 0;
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", chunk => { this.stderr = (this.stderr + chunk).slice(-16000); });
    this.child.stdout.setEncoding("utf8");
    let buffer = "";
    this.child.stdout.on("data", chunk => {
      buffer += chunk;
      if (buffer.length > 2_000_000) { this.failure = Error("RPC record exceeded test bound"); this.child.kill(); return; }
      let end;
      while ((end = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, end).replace(/\r$/, "");
        buffer = buffer.slice(end + 1);
        if (line) {
          try { this.records.push(JSON.parse(line)); } catch (error) { this.failure = error; }
        }
      }
      if (this.records.length > 1000) { this.failure = Error("RPC event limit exceeded"); this.child.kill(); }
    });
    this.child.on("error", error => { this.failure = error; });
    this.child.stdin.on("error", error => { this.failure = error; });
    this.exited = new Promise(resolve => this.child.on("close", resolve));
  }
  async wait(predicate, timeout = 20000) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      if (this.failure) throw this.failure;
      const index = this.records.findIndex(predicate);
      if (index >= 0) return this.records.splice(index, 1)[0];
      if (this.child.exitCode !== null || this.child.signalCode !== null) throw Error(`pi exited: ${this.stderr}`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw Error(`RPC timeout: ${this.stderr}\n${JSON.stringify(this.records.slice(-5))}`);
  }
  async command(type, fields = {}) {
    const id = String(++this.nextID);
    this.child.stdin.write(JSON.stringify({ id, type, ...fields }) + "\n");
    const response = await this.wait(r => r.type === "response" && r.id === id);
    assert.equal(response.success, true, JSON.stringify(response));
    return response.data;
  }
  async prompt(message) {
    this.records = this.records.filter(r => r.type !== "agent_settled");
    const response = await this.command("prompt", { message });
    assert.equal(response.disposition, "started");
    await this.wait(r => r.type === "agent_settled");
    this.records = [];
  }
  async close() {
    this.child.stdin.end();
    const kill = setTimeout(() => this.child.kill("SIGKILL"), 5000);
    try { await this.exited; } finally { clearTimeout(kill); }
  }
}

for (const scopeTools of [false, true]) {
test(scopeTools
  ? "real pi RPC: role/tool/cwd isolation, A-B-A, cross-project isolation and process restart"
  : "baseline: defaultTools alone exposes both extension tools (known limitation)", { timeout: 120000 }, async () => {
  const root = mkdtempSync(join(tmpdir(), "role-runtime-pi-"));
  const agentDir = join(root, "agent");
  mkdirSync(agentDir);
  const log = join(root, "capture.jsonl");
  const sessions = new Map();
  const workspaces = new Map();
  let rpc;
  let initialPID;
  try {
    const steps = scopeTools ? fixture.steps : fixture.steps.slice(0, 1);
    for (const step of steps) {
      const key = `${step.project}-${step.role}`;
      if (sessions.has(key)) continue;
      // Roles in the same project share a workspace; role identity is session
      // metadata, not a different directory or a mutable project-wide prompt.
      const cwd = join(root, step.project);
      mkdirSync(join(cwd, ".pi"), { recursive: true });
      writeFileSync(join(cwd, ".pi/SYSTEM.md"), "Neutral harness fixture.");
      writeFileSync(join(cwd, ".pi/settings.json"), JSON.stringify({ defaultTools: ["dto_probe"],
        compaction: { enabled: false }, retry: { enabled: false } }));
      const manager = SessionManager.create(cwd, join(root, "sessions"));
      manager.appendCustomEntry("role-runtime", { project: step.project, role: step.role, version: "fixture-v1" });
      manager.appendModelChange("role-spike", "deterministic");
      manager.appendMessage({ role: "user", content: "synthetic-session-seed", timestamp: 1 });
      manager.appendMessage({ role: "assistant", content: [{ type: "text", text: "seed-ack" }],
        api: "role-spike", provider: "role-spike", model: "deterministic", stopReason: "stop", timestamp: 2,
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } });
      sessions.set(key, manager.getSessionFile());
      workspaces.set(key, cwd);
      assert.ok(existsSync(manager.getSessionFile()));
    }
    for (const step of steps) {
      const key = `${step.project}-${step.role}`;
      const role = fixture.roles[step.role];
      if (step.restart) { await rpc.close(); rpc = undefined; }
      if (!rpc) rpc = new RPC(workspaces.get(key), agentDir, log, scopeTools);
      if (!initialPID) initialPID = rpc.child.pid;
      if (!step.restart) assert.equal(rpc.child.pid, initialPID, "must reuse one process");
      else assert.notEqual(rpc.child.pid, initialPID, "must actually restart");
      const switched = await rpc.command("switch_session", { sessionPath: sessions.get(key) });
      assert.equal(switched.cancelled, false);
      writeFileSync(log, "");
      await rpc.prompt(step.prompt);
      const captures = readFileSync(log, "utf8").trim().split("\n").map(JSON.parse);
      const calls = captures.filter(c => c.kind === "model");
      assert.equal(calls.length, 2, "real model -> tool -> model loop");
      for (const call of calls) {
        assert.ok(call.system.includes(role.instruction));
        assert.deepEqual(call.tools, scopeTools ? [role.tool] : ["dto_probe", "qa_probe"]);
        const text = JSON.stringify(call.messages);
        assert.ok(text.includes(step.prompt), "current task absent from model input");
        for (const marker of step.required) assert.ok(text.includes(marker), `missing ${marker}`);
        for (const marker of step.forbidden) assert.ok(!text.includes(marker), `leaked ${marker}`);
        for (const other of Object.values(fixture.roles)) {
          if (other !== role) assert.ok(!text.includes(other.instruction), "previous role instruction leaked");
        }
      }
      assert.deepEqual(captures.filter(c => c.kind === "tool"), [{ kind: "tool", name: role.tool, cwd: workspaces.get(key) }]);
      assert.deepEqual(await rpc.command("get_last_assistant_text"), { text: "synthetic-complete" });
      console.log(JSON.stringify({ candidate: "pi", scopedTools: scopeTools, step: step.prompt, result: "pass", sameProcess: !step.restart }));
    }
  } finally {
    if (rpc) await rpc.close();
    rmSync(root, { recursive: true, force: true });
  }
});
}

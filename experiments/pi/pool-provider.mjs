// Deterministic model/provider for the Go integration test and demo only.
// The actual pi RPC, session replay and tool execution remain unmodified.
import assert from "node:assert/strict";
import { appendFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { Type, createAssistantMessageEventStream, getCurrentSystemPrompt, getCurrentTools } from "@earendil-works/pi-ai";

const text = message => typeof message.content === "string" ? message.content : message.content.filter(p => p.type === "text").map(p => p.text).join("");
export default function (pi) {
  const count = Number(process.env.ROLE_POOL_ROLES);
  if (!Number.isInteger(count) || count < 1 || count > 400) throw Error("Invalid fixture role count");
  for (let i = 0; i < count; i++) {
    const name = `probe_${String(i).padStart(2, "0")}`;
    pi.registerTool({
      name, label: name, description: `Synthetic ${name}`, parameters: Type.Object({ project: Type.String() }),
      async execute(_id, params, signal, _update, ctx) {
        assert.equal(readFileSync(join(ctx.cwd, "project-marker"), "utf8"), params.project);
        await new Promise((resolve, reject) => {
          if (signal?.aborted) return reject(Error("aborted"));
          const abort = () => { clearTimeout(timer); reject(Error("aborted")); };
          const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, 15);
          signal?.addEventListener("abort", abort, { once: true });
        });
        return { content: [{ type: "text", text: `executed:${name}` }], details: {} };
      },
    });
  }
  pi.registerProvider("role-pool", {
    api: "role-pool", apiKey: "synthetic-not-a-credential", baseUrl: "https://unused.invalid",
    models: [{ id: "deterministic", name: "Pool fixture", reasoning: false, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32768, maxTokens: 512 }],
    streamSimple(model, context, options) {
      const users = context.messages.filter(m => m.role === "user").map(m => JSON.parse(text(m)));
      const request = users.at(-1);
      const expectedTool = `probe_${request.role.slice("role-".length)}`;
      assert.deepEqual(getCurrentTools(context.messages).map(t => t.name), [expectedTool]);
      const system = getCurrentSystemPrompt(context.messages);
      assert.ok(system.includes(`ROLE_CONTEXT_${request.role}_END`));
      assert.ok(system.includes(`PROJECT_CONTEXT_${request.project}_END`));
      for (const prior of users) {
        assert.equal(prior.role, request.role, "cross-role history leak");
        assert.equal(prior.project, request.project, "cross-project history leak");
      }
      for (let prior = 0; prior < request.round; prior++) {
        assert.ok(users.some(u => u.round === prior), `lost round ${prior}`);
      }
      const callTool = context.messages.at(-1)?.role !== "toolResult";
      appendFileSync(process.env.ROLE_POOL_LOG, JSON.stringify({ pid: process.pid, task: request.id, role: request.role, round: request.round, phase: callTool ? "begin" : "end" }) + "\n");
      const message = {
        role: "assistant", api: model.api, provider: model.provider, model: model.id,
        content: callTool ? [{ type: "toolCall", id: `tool-${request.id}`, name: expectedTool, arguments: { project: request.project } }]
          : [{ type: "text", text: `completed:${request.id}` }],
        stopReason: callTool ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      };
      if (options?.signal?.aborted) { message.stopReason = "aborted"; message.content = []; }
      const stream = createAssistantMessageEventStream(); stream.end(message); return stream;
    },
  });
}

// Synthetic model boundary only: the actual pi CLI/session/tool loop runs unchanged.
import { appendFileSync, readFileSync } from "node:fs";
import { Type, createAssistantMessageEventStream, getCurrentSystemPrompt, getCurrentTools } from "@earendil-works/pi-ai";

export default function (pi) {
  const { roles } = JSON.parse(readFileSync(process.env.ROLE_SPIKE_FIXTURE, "utf8"));
  let activeRole;
  // defaultTools does not restrict tools registered by extensions. Apply the
  // role allowlist explicitly at activation; this is adapter work, not a fork.
  pi.on("session_start", async (_event, ctx) => {
    const entry = ctx.sessionManager.getEntries().findLast(e => e.type === "custom" && e.customType === "role-runtime");
    activeRole = entry ? roles[entry.data.role] : undefined;
    if (process.env.ROLE_SPIKE_SCOPE_TOOLS !== "1") return;
    pi.setActiveTools(activeRole ? [activeRole.tool] : []);
  });
  pi.on("before_agent_start", async () => {
    if (!activeRole) throw new Error("Missing role definition");
    return { systemPrompt: activeRole.instruction };
  });
  for (const name of ["dto_probe", "qa_probe"]) {
    pi.registerTool({
      name, label: name, description: `Synthetic ${name} tool`, parameters: Type.Object({}),
      async execute(_id, _params, _signal, _onUpdate, ctx) {
        appendFileSync(process.env.ROLE_SPIKE_LOG, JSON.stringify({ kind: "tool", name, cwd: ctx.cwd }) + "\n");
        return { content: [{ type: "text", text: `executed:${name}` }], details: {} };
      },
    });
  }
  pi.registerProvider("role-spike", {
    api: "role-spike", apiKey: "synthetic-not-a-credential", baseUrl: "https://unused.invalid",
    models: [{ id: "deterministic", name: "Deterministic fixture", reasoning: false, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32768, maxTokens: 512 }],
    streamSimple(model, context) {
      const tools = getCurrentTools(context.messages);
      const system = getCurrentSystemPrompt(context.messages);
      appendFileSync(process.env.ROLE_SPIKE_LOG, JSON.stringify({ kind: "model", system,
        tools: tools.map(t => t.name), messages: context.messages, pid: process.pid }) + "\n");
      const last = context.messages.at(-1);
      const callTool = last?.role !== "toolResult";
      const message = {
        role: "assistant", api: model.api, provider: model.provider, model: model.id,
        content: callTool
          ? [{ type: "toolCall", id: `probe-${Date.now()}`, name: tools[0]?.name ?? "missing_tool", arguments: {} }]
          : [{ type: "text", text: "synthetic-complete" }],
        stopReason: callTool ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      };
      const stream = createAssistantMessageEventStream();
      stream.end(message);
      return stream;
    },
  });
}

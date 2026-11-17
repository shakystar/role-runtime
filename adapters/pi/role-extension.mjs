// pi 1.5.2 extension. Definitions are fetched per activation, not preloaded for
// every role. This is a context boundary, NOT an OS security sandbox.
import { readFileSync, realpathSync, statSync } from "node:fs";
import { join } from "node:path";
import { createHash } from "node:crypto";

const digest = (...parts) => createHash("sha256").update(JSON.stringify(parts)).digest("hex");
const validID = value => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(value);
function definition(path) {
  if (statSync(path).size > 65536) throw Error("Role definition exceeds byte limit");
  return JSON.parse(readFileSync(path, "utf8"));
}

export default function (pi) {
  let active;
  const root = realpathSync(process.env.ROLE_RUNTIME_STATE);
  pi.on("session_start", async (_event, ctx) => {
    active = undefined;
    pi.setActiveTools([]);
    const entry = ctx.sessionManager.getEntries().findLast(e => e.type === "custom" && e.customType === "role-runtime/v1");
    if (!entry) return; // Idle bootstrap session is allowed, but cannot run.
    const ref = entry.data;
    if (![ref?.project, ref?.role, ref?.version, ref?.instance].every(validID)) throw Error("Invalid role reference");
    const role = definition(join(root, "roles", digest(ref.role, ref.version) + ".json"));
    const project = definition(join(root, "projects", digest(ref.project) + ".json"));
    if (role.id !== ref.role || role.version !== ref.version || project.id !== ref.project) throw Error("Role identity mismatch");
    if (realpathSync(ctx.cwd) !== realpathSync(project.workspace)) throw Error("Project workspace mismatch");
    if (ctx.sessionManager.getSessionId() !== digest(ref.project, ref.role, ref.version, ref.instance)) throw Error("Session identity mismatch");
    if (typeof role.instruction !== "string" || !Array.isArray(role.tools) || role.tools.length === 0) throw Error("Invalid role definition");
    const available = new Set(pi.getAllTools().map(t => t.name));
    if (role.tools.some(name => !available.has(name))) throw Error("Role requests unavailable tool");
    pi.setActiveTools(role.tools);
    active = { role, project };
  });
  pi.on("before_agent_start", async () => {
    if (!active) throw Error("No validated role activation");
    return { systemPrompt: `${active.project.rules}\n\n${active.role.instruction}` };
  });
}

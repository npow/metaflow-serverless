// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const SERVICE_AUTH_KEY = Deno.env.get("METAFLOW_SERVICE_AUTH_KEY") ?? "";
const REQUIRE_AUTH = Deno.env.get("REQUIRE_AUTH") !== "false";

const POSTGREST_BASE = `${SUPABASE_URL}/rest/v1`;

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type, apikey, x-api-key, x-client-info",
};

function corsResponse(status = 204): Response {
  return new Response(null, { status, headers: CORS_HEADERS });
}

function withCors(res: Response): Response {
  const headers = new Headers(res.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) {
    headers.set(k, v);
  }
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers });
}

// ---------------------------------------------------------------------------
// Route table
// ---------------------------------------------------------------------------

type HttpMethod = "GET" | "POST" | "PATCH" | "DELETE";

interface RouteDefinition {
  method: HttpMethod | HttpMethod[];
  // Pattern segments: plain strings or `:param` names.
  // A leading "/" is stripped before splitting.
  pattern: string;
  handler: RouteHandler;
}

type Params = Record<string, string>;
type QueryParams = URLSearchParams;

type RouteHandler = (
  req: Request,
  params: Params,
  query: QueryParams,
) => Promise<Response>;

interface MatchResult {
  handler: RouteHandler;
  params: Params;
}

// Compile a pattern string into a regex + param-name list once at startup.
interface CompiledRoute {
  method: HttpMethod[];
  regex: RegExp;
  paramNames: string[];
  handler: RouteHandler;
}

function compileRoute(def: RouteDefinition): CompiledRoute {
  const methods = Array.isArray(def.method) ? def.method : [def.method];
  const segments = def.pattern.replace(/^\//, "").split("/");
  const paramNames: string[] = [];

  const regexParts = segments.map((seg) => {
    if (seg.startsWith(":")) {
      paramNames.push(seg.slice(1));
      return "([^/]+)";
    }
    // Escape any regex special chars in literal segments.
    return seg.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  });

  const regex = new RegExp(`^/${regexParts.join("/")}$`);
  return { method: methods, regex, paramNames, handler: def.handler };
}

function matchRoute(
  compiledRoutes: CompiledRoute[],
  method: string,
  pathname: string,
): MatchResult | null {
  for (const route of compiledRoutes) {
    if (!route.method.includes(method as HttpMethod)) continue;
    const m = pathname.match(route.regex);
    if (!m) continue;
    const params: Params = {};
    route.paramNames.forEach((name, i) => {
      params[name] = decodeURIComponent(m[i + 1]);
    });
    return { handler: route.handler, params };
  }
  return null;
}

// ---------------------------------------------------------------------------
// PostgREST helpers
// ---------------------------------------------------------------------------

function pgHeaders(): HeadersInit {
  return {
    "Authorization": `Bearer ${SERVICE_ROLE_KEY}`,
    "apikey": SERVICE_ROLE_KEY,
    "Content-Type": "application/json",
    "Prefer": "return=representation",
  };
}

async function pgGet(path: string): Promise<Response> {
  const url = `${POSTGREST_BASE}${path}`;
  return await fetch(url, { method: "GET", headers: pgHeaders() });
}

async function pgPost(path: string, body: unknown): Promise<Response> {
  const url = `${POSTGREST_BASE}${path}`;
  return await fetch(url, {
    method: "POST",
    headers: pgHeaders(),
    body: JSON.stringify(body),
  });
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

// Forward a PostgREST response verbatim (preserving status/body) but add CORS.
async function forwardResponse(res: Response): Promise<Response> {
  const text = await res.text();
  return new Response(text, {
    status: res.status,
    headers: {
      "Content-Type": res.headers.get("Content-Type") ?? "application/json",
      ...CORS_HEADERS,
    },
  });
}

// PostgREST returns an array for table queries.
// When Metaflow expects a single object, unwrap the first element.
async function forwardSingle(res: Response): Promise<Response> {
  if (!res.ok) return forwardResponse(res);
  const data = await res.json();
  if (Array.isArray(data)) {
    if (data.length === 0) {
      return new Response(JSON.stringify({ detail: "not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json", ...CORS_HEADERS },
      });
    }
    return jsonResponse(data[0]);
  }
  return jsonResponse(data);
}

// ---------------------------------------------------------------------------
// JWT / Auth validation
// ---------------------------------------------------------------------------

async function validateAuth(req: Request): Promise<boolean> {
  if (!REQUIRE_AUTH) return true;
  if (!SERVICE_AUTH_KEY) return false;
  const provided = req.headers.get("x-api-key");
  return typeof provided === "string" && provided.length > 0 && provided === SERVICE_AUTH_KEY;
}

// ---------------------------------------------------------------------------
// Handler implementations
// ---------------------------------------------------------------------------

// --- Admin ---

const handlePing: RouteHandler = async (_req, _p, _q) => {
  return jsonResponse("pong");
};

const handleHealthcheck: RouteHandler = async (_req, _p, _q) => {
  const res = await pgGet("/flows_v3?select=flow_id&limit=1");
  if (!res.ok) {
    const text = await res.text();
    return new Response(JSON.stringify({ status: "DOWN", detail: text }), {
      status: 503,
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    });
  }
  return jsonResponse({ status: "UP" });
};

const handleVersion: RouteHandler = async (_req, _p, _q) => {
  return jsonResponse({ version: "1.0.0" });
};

// --- Flows ---

const handleListFlows: RouteHandler = async (_req, _p, _q) => {
  const res = await pgGet("/flows_v3?order=ts_epoch.desc&select=*");
  return forwardResponse(res);
};

const handleGetFlow: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(`/flows_v3?flow_id=eq.${enc(p.flowId)}&select=*`);
  return forwardSingle(res);
};

const handleCreateFlow: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/create_flow", {
    p_user_name: body.user_name ?? body.p_user_name ?? null,
    p_tags: body.tags ?? body.p_tags ?? [],
    p_system_tags: body.system_tags ?? body.p_system_tags ?? [],
    p_flow_id: p.flowId,
  });
  return forwardResponse(res);
};

// --- Runs ---

const handleListRuns: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/runs_v3?flow_id=eq.${enc(p.flowId)}&order=ts_epoch.desc&select=*`,
  );
  return forwardResponse(res);
};

const handleGetRun: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/runs_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&select=*`,
  );
  return forwardSingle(res);
};

const handleCreateRun: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/create_run", {
    p_run_id: body.run_id ?? body.p_run_id ?? null,
    p_user_name: body.user_name ?? body.p_user_name ?? null,
    p_ts_epoch: body.ts_epoch ?? body.p_ts_epoch ?? null,
    p_tags: body.tags ?? body.p_tags ?? [],
    p_system_tags: body.system_tags ?? body.p_system_tags ?? [],
    p_flow_id: p.flowId,
  });
  return forwardResponse(res);
};

const handleHeartbeatRun: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/heartbeat_run", {
    p_ts: body.ts ?? body.p_ts ?? null,
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
  });
  return forwardResponse(res);
};

const handleMutateRunTags: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/mutate_run_tags", {
    p_tags_to_add: body.tags_to_add ?? body.p_tags_to_add ?? [],
    p_tags_to_remove: body.tags_to_remove ?? body.p_tags_to_remove ?? [],
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
  });
  return forwardResponse(res);
};

// --- Run metadata ---

const handleGetRunMetadata: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/metadata_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&select=*`,
  );
  return forwardResponse(res);
};

// --- Steps ---

const handleListSteps: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/steps_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&select=*`,
  );
  return forwardResponse(res);
};

const handleGetStep: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/steps_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&step_name=eq.${enc(p.stepName)}&select=*`,
  );
  return forwardSingle(res);
};

const handleCreateStep: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/create_step", {
    p_user_name: body.user_name ?? body.p_user_name ?? null,
    p_ts_epoch: body.ts_epoch ?? body.p_ts_epoch ?? null,
    p_tags: body.tags ?? body.p_tags ?? [],
    p_system_tags: body.system_tags ?? body.p_system_tags ?? [],
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
    p_step_name: p.stepName,
  });
  return forwardResponse(res);
};

// --- Tasks ---

const handleListTasks: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/tasks_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&step_name=eq.${enc(p.stepName)}&select=*`,
  );
  return forwardResponse(res);
};

const handleGetTask: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/tasks_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&step_name=eq.${enc(p.stepName)}&task_id=eq.${enc(p.taskId)}&select=*`,
  );
  return forwardSingle(res);
};

const handleCreateTask: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const requestedTaskName =
    body.task_name ??
    body.p_task_name ??
    body.task_id ??
    body.p_task_id ??
    p.taskId ??
    null;
  // Metaflow's /task call may omit task_name. If we always default to "0",
  // foreach fanout collides on tasks_v3_unique_name. Use a unique fallback.
  const taskName = requestedTaskName === null || requestedTaskName === undefined
    ? `auto-${Date.now()}-${crypto.randomUUID().slice(0, 8)}`
    : String(requestedTaskName);
  const res = await pgPost("/rpc/create_task", {
    p_task_name: taskName,
    p_user_name: body.user_name ?? body.p_user_name ?? null,
    p_ts_epoch: body.ts_epoch ?? body.p_ts_epoch ?? null,
    p_tags: body.tags ?? body.p_tags ?? [],
    p_system_tags: body.system_tags ?? body.p_system_tags ?? [],
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
    p_step_name: p.stepName,
  });
  return forwardResponse(res);
};

const handleHeartbeatTask: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const res = await pgPost("/rpc/heartbeat_task", {
    p_ts: body.ts ?? body.p_ts ?? null,
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
    p_step_name: p.stepName,
    p_task_id: p.taskId,
  });
  return forwardResponse(res);
};

// --- Task metadata ---

const handleGetTaskMetadata: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/metadata_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&step_name=eq.${enc(p.stepName)}&task_id=eq.${enc(p.taskId)}&select=*`,
  );
  return forwardResponse(res);
};

const handleCreateTaskMetadata: RouteHandler = async (req, p, _q) => {
  // Metaflow sends an array of metadata objects; insert each with context fields.
  const body = await safeJson(req);
  const records = Array.isArray(body) ? body : [body];
  const enriched = records.map((r: Record<string, unknown>) => ({
    ...r,
    flow_id: p.flowId,
    run_number: Number(p.runNumber),
    step_name: p.stepName,
    task_id: Number(p.taskId),
  }));
  const res = await pgPost("/metadata_v3", enriched);
  return forwardResponse(res);
};

// --- Artifacts ---

const handleGetTaskArtifacts: RouteHandler = async (_req, p, _q) => {
  // Use RPC to get latest artifacts per name for this task.
  const res = await pgPost("/rpc/get_artifacts_latest", {
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
    p_step_name: p.stepName,
    p_task_id: p.taskId,
  });
  return forwardResponse(res);
};

const handleGetStepArtifacts: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/artifact_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&step_name=eq.${enc(p.stepName)}&select=*`,
  );
  return forwardResponse(res);
};

const handleGetRunArtifacts: RouteHandler = async (_req, p, _q) => {
  const res = await pgGet(
    `/artifact_v3?flow_id=eq.${enc(p.flowId)}&run_number=eq.${enc(p.runNumber)}&select=*`,
  );
  return forwardResponse(res);
};

const handleCreateArtifact: RouteHandler = async (req, p, _q) => {
  const body = await safeJson(req);
  const records = Array.isArray(body) ? body : [body];
  const enriched = records.map((r: Record<string, unknown>) => ({
    ...r,
    flow_id: p.flowId,
    run_number: Number(p.runNumber),
    step_name: p.stepName,
    task_id: Number(p.taskId),
  }));
  const res = await pgPost("/artifact_v3", enriched);
  return forwardResponse(res);
};

// --- Filtered tasks ---

const handleFilteredTasks: RouteHandler = async (_req, p, q) => {
  const metadataFieldName = q.get("metadata_field_name") ?? "";
  const pattern = q.get("pattern") ?? "";
  const res = await pgPost("/rpc/filter_tasks_by_metadata", {
    p_flow_id: p.flowId,
    p_run_id: p.runNumber,
    p_step_name: p.stepName,
    p_field_name: metadataFieldName,
    p_pattern: pattern,
  });
  return forwardResponse(res);
};

// ---------------------------------------------------------------------------
// Route table definition
// ---------------------------------------------------------------------------

const ROUTE_DEFINITIONS: RouteDefinition[] = [
  // Admin
  { method: "GET", pattern: "/ping", handler: handlePing },
  { method: "GET", pattern: "/healthcheck", handler: handleHealthcheck },
  { method: "GET", pattern: "/version", handler: handleVersion },

  // Flows
  { method: "GET", pattern: "/flows", handler: handleListFlows },
  { method: "GET", pattern: "/flows/:flowId", handler: handleGetFlow },
  { method: "POST", pattern: "/flows/:flowId", handler: handleCreateFlow },

  // Runs
  { method: "GET", pattern: "/flows/:flowId/runs", handler: handleListRuns },
  { method: "GET", pattern: "/flows/:flowId/runs/:runNumber", handler: handleGetRun },
  { method: "POST", pattern: "/flows/:flowId/run", handler: handleCreateRun },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/heartbeat",
    handler: handleHeartbeatRun,
  },
  {
    method: "PATCH",
    pattern: "/flows/:flowId/runs/:runNumber/tag/mutate",
    handler: handleMutateRunTags,
  },
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/metadata",
    handler: handleGetRunMetadata,
  },

  // Steps
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps",
    handler: handleListSteps,
  },
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName",
    handler: handleGetStep,
  },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/step",
    handler: handleCreateStep,
  },

  // Tasks
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks",
    handler: handleListTasks,
  },
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId",
    handler: handleGetTask,
  },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/task",
    handler: handleCreateTask,
  },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId/heartbeat",
    handler: handleHeartbeatTask,
  },

  // Task metadata
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId/metadata",
    handler: handleGetTaskMetadata,
  },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId/metadata",
    handler: handleCreateTaskMetadata,
  },

  // Artifacts
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId/artifacts",
    handler: handleGetTaskArtifacts,
  },
  {
    method: "POST",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/tasks/:taskId/artifact",
    handler: handleCreateArtifact,
  },
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/artifacts",
    handler: handleGetStepArtifacts,
  },
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/artifacts",
    handler: handleGetRunArtifacts,
  },

  // Filtered tasks (must come before generic tasks list to avoid shadowing)
  {
    method: "GET",
    pattern: "/flows/:flowId/runs/:runNumber/steps/:stepName/filtered_tasks",
    handler: handleFilteredTasks,
  },
];

// Compile all routes once at startup.
const COMPILED_ROUTES: CompiledRoute[] = ROUTE_DEFINITIONS.map(compileRoute);

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/** URL-encode a path segment value. */
function enc(value: string): string {
  return encodeURIComponent(value);
}

/** Safely parse request body as JSON; return empty object on failure. */
async function safeJson(req: Request): Promise<Record<string, unknown>> {
  try {
    const text = await req.text();
    if (!text) return {};
    return JSON.parse(text);
  } catch {
    return {};
  }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

Deno.serve(async (req: Request): Promise<Response> => {
  // Handle CORS preflight.
  if (req.method === "OPTIONS") {
    return corsResponse(204);
  }

  // Auth check.
  const authed = await validateAuth(req);
  if (!authed) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    });
  }

  const url = new URL(req.url);
  // Strip a function-name prefix if running under Supabase Edge Functions
  // (e.g. /metaflow-service/flows → /flows).
  const pathname = normalizePath(url.pathname);
  const query = url.searchParams;

  const match = matchRoute(COMPILED_ROUTES, req.method, pathname);

  if (!match) {
    return new Response(
      JSON.stringify({ error: "Not found", path: pathname, method: req.method }),
      {
        status: 404,
        headers: { "Content-Type": "application/json", ...CORS_HEADERS },
      },
    );
  }

  try {
    return await match.handler(req, match.params, query);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return new Response(JSON.stringify({ error: "Internal server error", detail: message }), {
      status: 500,
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    });
  }
});

// ---------------------------------------------------------------------------
// Path normalisation
// ---------------------------------------------------------------------------

/**
 * Supabase Edge Functions are invoked at paths like:
 *   /metaflow-service
 *   /metaflow-service/flows/MyFlow/runs
 *
 * Strip any leading path segment that is not a known Metaflow top-level
 * resource so that the router sees a clean path starting with /flows, /ping,
 * /healthcheck, or /version.
 */
const TOP_LEVEL_SEGMENTS = new Set(["flows", "ping", "healthcheck", "version"]);

function normalizePath(pathname: string): string {
  // Ensure single leading slash.
  const clean = "/" + pathname.replace(/^\/+/, "");
  const parts = clean.split("/"); // ["", "segment1", ...]
  // parts[0] is always "" due to leading "/".
  if (parts.length >= 2 && !TOP_LEVEL_SEGMENTS.has(parts[1])) {
    // Drop the first non-empty segment (function name prefix).
    return "/" + parts.slice(2).join("/");
  }
  return clean;
}

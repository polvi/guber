import { Hono } from "hono";
import { ApiServer, D1Storage, Resource, CustomResourceDefinition } from "@guber/core";

type Bindings = {
  DB: any;
};

const app = new Hono<{ Bindings: Bindings }>();

/**
 * Helper to initialize ApiServer with D1 data for the request.
 * In a production scenario, you might cache this or optimize queries.
 */
async function getHydratedServer(db: any) {
  const storage = new D1Storage(db);
  const api = new ApiServer();
  
  const crds = await storage.listCRDs();
  for (const crd of crds) {
    api.createCRD(crd);
  }

  // Note: For a real implementation, we'd only load resources relevant to the request
  // or implement a more direct D1-backed ApiServer.
  return { api, storage };
}

// --- Kubernetes Discovery Endpoints ---

app.get("/api", (c) => {
  return c.json({
    kind: "APIVersions",
    versions: ["v1"],
    serverAddressByClientCIDRs: [],
  });
});

app.get("/apis", (c) => {
  return c.json({
    kind: "APIGroupList",
    groups: [
      {
        name: "cloudflare.guber.dev",
        versions: [{ groupVersion: "cloudflare.guber.dev/v1", version: "v1" }],
        preferredVersion: { groupVersion: "cloudflare.guber.dev/v1", version: "v1" },
      },
    ],
  });
});

app.get("/apis/cloudflare.guber.dev/v1", (c) => {
  return c.json({
    kind: "APIResourceList",
    groupVersion: "cloudflare.guber.dev/v1",
    resources: [
      {
        name: "workers",
        singularName: "worker",
        namespaced: true,
        kind: "Worker",
        verbs: ["get", "list", "create", "update", "patch", "delete"],
      },
    ],
  });
});

// --- Resource Handlers ---

// List Resources
app.get("/apis/:group/:version/namespaces/:ns/:plural", async (c) => {
  const { ns, plural } = c.req.param();
  const { storage } = await getHydratedServer(c.env.DB);
  
  // Map plural to Kind (simplified)
  const kind = plural === "workers" ? "Worker" : "";
  const items = await storage.listResources(kind, ns);

  return c.json({
    kind: `${kind}List`,
    apiVersion: `${c.req.param("group")}/${c.req.param("version")}`,
    metadata: { selfLink: c.req.path },
    items,
  });
});

// Get Single Resource
app.get("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, plural, name } = c.req.param();
  const { storage } = await getHydratedServer(c.env.DB);
  const kind = plural === "workers" ? "Worker" : "";
  
  const resource = await storage.getResource(kind, name, ns);
  if (!resource) return c.json({ message: "Not Found" }, 404);
  
  return c.json(resource);
});

// Create Resource
app.post("/apis/:group/:version/namespaces/:ns/:plural", async (c) => {
  const { ns } = c.req.param();
  const body = await c.req.json();
  const { api, storage } = await getHydratedServer(c.env.DB);

  try {
    const resource = { ...body, metadata: { ...body.metadata, namespace: ns } };
    const created = api.create(resource);
    await storage.saveResource(created);
    return c.json(created, 201);
  } catch (e: any) {
    return c.json({ message: e.message }, 400);
  }
});

// Update Resource
app.put("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, name } = c.req.param();
  const body = await c.req.json();
  const { api, storage } = await getHydratedServer(c.env.DB);

  try {
    const existing = await storage.getResource(body.kind, name, ns);
    if (!existing) return c.json({ message: "Not Found" }, 404);

    const updated = api.update(body);
    await storage.saveResource(updated);
    return c.json(updated);
  } catch (e: any) {
    return c.json({ message: e.message }, 409);
  }
});

// Delete Resource
app.delete("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, plural, name } = c.req.param();
  const { api, storage } = await getHydratedServer(c.env.DB);
  const kind = plural === "workers" ? "Worker" : "";

  const existing = await storage.getResource(kind, name, ns);
  if (!existing) return c.json({ message: "Not Found" }, 404);

  api.delete(kind, name, ns);
  const updated = api.get(kind, name, ns);
  if (updated) {
    await storage.saveResource(updated);
  }

  return c.json({ status: "Success" });
});

export default app;

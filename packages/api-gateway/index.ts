import { Hono } from "hono";
import { ApiServer, D1Storage, Resource, CustomResourceDefinition } from "@guber/core";

type Bindings = {
  DB: any;
};

const app = new Hono<{ Bindings: Bindings }>();

/**
 * Helper to initialize ApiServer with D1 data for the request.
 */
async function getHydratedServer(db: any) {
  const storage = new D1Storage(db);
  const api = new ApiServer();
  
  const crds = await storage.listCRDs();
  for (const crd of crds) {
    api.createCRD(crd);
  }

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

app.get("/apis", async (c) => {
  const { storage } = await getHydratedServer(c.env.DB);
  const crds = await storage.listCRDs();
  
  // Extract unique groups from registered CRDs
  const groups = new Set(crds.map(crd => crd.spec.group));
  groups.add("apiextensions.k8s.io");

  return c.json({
    kind: "APIGroupList",
    groups: Array.from(groups).map(group => ({
      name: group,
      versions: [{ groupVersion: `${group}/v1`, version: "v1" }],
      preferredVersion: { groupVersion: `${group}/v1`, version: "v1" },
    })),
  });
});

// Discovery for CRDs themselves
app.get("/apis/apiextensions.k8s.io/v1", (c) => {
  return c.json({
    kind: "APIResourceList",
    groupVersion: "apiextensions.k8s.io/v1",
    resources: [
      {
        name: "customresourcedefinitions",
        singularName: "customresourcedefinition",
        namespaced: false,
        kind: "CustomResourceDefinition",
        verbs: ["get", "list", "create", "delete"],
      },
    ],
  });
});

// Dynamic Discovery for Custom Groups
app.get("/apis/:group/:version", async (c) => {
  const group = c.req.param("group");
  const { storage } = await getHydratedServer(c.env.DB);
  const crds = await storage.listCRDs();
  
  const groupCrds = crds.filter(crd => crd.spec.group === group);

  return c.json({
    kind: "APIResourceList",
    groupVersion: `${group}/${c.req.param("version")}`,
    resources: groupCrds.map(crd => ({
      name: crd.spec.names.plural,
      singularName: crd.spec.names.kind.toLowerCase(),
      namespaced: true,
      kind: crd.spec.names.kind,
      verbs: ["get", "list", "create", "update", "patch", "delete"],
    })),
  });
});

// --- CRD Management ---

app.get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions", async (c) => {
  const { storage } = await getHydratedServer(c.env.DB);
  const items = await storage.listCRDs();
  return c.json({
    kind: "CustomResourceDefinitionList",
    apiVersion: "apiextensions.k8s.io/v1",
    items,
  });
});

app.post("/apis/apiextensions.k8s.io/v1/customresourcedefinitions", async (c) => {
  const body = await c.req.json() as CustomResourceDefinition;
  const { api, storage } = await getHydratedServer(c.env.DB);
  
  try {
    api.createCRD(body);
    await storage.saveCRD(body);
    return c.json(body, 201);
  } catch (e: any) {
    return c.json({ message: e.message }, 400);
  }
});

// --- Generic Resource Handlers ---

app.get("/apis/:group/:version/namespaces/:ns/:plural", async (c) => {
  const { ns, plural, group } = c.req.param();
  const { storage } = await getHydratedServer(c.env.DB);
  
  const crds = await storage.listCRDs();
  const crd = crds.find(r => r.spec.names.plural === plural && r.spec.group === group);
  if (!crd) return c.json({ message: "Resource type not found" }, 404);

  const items = await storage.listResources(crd.spec.names.kind, ns);

  return c.json({
    kind: `${crd.spec.names.kind}List`,
    apiVersion: `${group}/${c.req.param("version")}`,
    metadata: { selfLink: c.req.path },
    items,
  });
});

app.get("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, plural, name, group } = c.req.param();
  const { storage } = await getHydratedServer(c.env.DB);
  
  const crds = await storage.listCRDs();
  const crd = crds.find(r => r.spec.names.plural === plural && r.spec.group === group);
  if (!crd) return c.json({ message: "Resource type not found" }, 404);
  
  const resource = await storage.getResource(crd.spec.names.kind, name, ns);
  if (!resource) return c.json({ message: "Not Found" }, 404);
  
  return c.json(resource);
});

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

app.put("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, name } = c.req.param();
  const body = await c.req.json();
  const { api, storage } = await getHydratedServer(c.env.DB);

  try {
    const updated = api.update(body);
    await storage.saveResource(updated);
    return c.json(updated);
  } catch (e: any) {
    return c.json({ message: e.message }, 409);
  }
});

app.delete("/apis/:group/:version/namespaces/:ns/:plural/:name", async (c) => {
  const { ns, plural, name, group } = c.req.param();
  const { api, storage } = await getHydratedServer(c.env.DB);
  
  const crds = await storage.listCRDs();
  const crd = crds.find(r => r.spec.names.plural === plural && r.spec.group === group);
  if (!crd) return c.json({ message: "Resource type not found" }, 404);

  api.delete(crd.spec.names.kind, name, ns);
  const updated = api.get(crd.spec.names.kind, name, ns);
  if (updated) {
    await storage.saveResource(updated);
  }

  return c.json({ status: "Success" });
});

export default app;

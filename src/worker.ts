import { Hono } from "hono";
import { v4 as uuid } from "uuid";
import type { GuberConfig } from "./config";

// Load config
let config: GuberConfig;
try {
  // @ts-ignore - Dynamic import for config
  config = (await import("../guber.config")).default;
} catch (error) {
  console.warn("No guber.config.ts found, using default configuration");
  config = { controllers: [] };
}

const app = new Hono<Env>();

// Helper function to notify controllers of resource events
async function notifyControllers(
  event: "created" | "deleted",
  context: ResourceContext,
) {
  for (const controller of config.controllers) {
    try {
      if (event === "created" && controller.onResourceCreated) {
        await controller.onResourceCreated(context);
      } else if (event === "deleted" && controller.onResourceDeleted) {
        await controller.onResourceDeleted(context);
      }
    } catch (error) {
      console.error(
        `Controller ${controller.constructor?.name || "unknown"} failed to handle ${event} event:`,
        error,
      );
    }
  }
}

// --- OpenAPI v3 endpoints for kubectl validation ---

// OpenAPI v3 index endpoint
app.get("/openapi/v3", async (c) => {
  const paths: Record<string, { serverRelativeURL: string }> = {};

  // Core API v1
  paths["api/v1"] = { serverRelativeURL: "/openapi/v3/api/v1" };

  // apiextensions.k8s.io/v1 for CRDs
  paths["apis/apiextensions.k8s.io/v1"] = {
    serverRelativeURL: "/openapi/v3/apis/apiextensions.k8s.io/v1",
  };

  // Get all unique group/version combinations from CRDs
  const { results } = await c.env.DB.prepare(
    "SELECT DISTINCT group_name, version FROM crds",
  ).all();

  const gvSet = new Set<string>();
  for (const row of results || []) {
    gvSet.add(`${row.group_name}/${row.version}`);
  }

  // Add CRD group/versions
  for (const gv of gvSet) {
    paths[`apis/${gv}`] = { serverRelativeURL: `/openapi/v3/apis/${gv}` };
  }

  return c.json({ paths });
});

// OpenAPI v3 spec for core v1
app.get("/openapi/v3/api/v1", async (c) => {
  const spec = {
    openapi: "3.0.0",
    info: {
      title: "Kubernetes API",
      version: "v1",
      description: "Kubernetes API server OpenAPI specification",
    },
    servers: [{ url: new URL(c.req.url).origin }],
    paths: {
      "/api/v1/namespaces/{name}": {
        patch: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "fieldValidation",
              in: "query",
              schema: {
                type: "string",
                enum: ["Ignore", "Warn", "Strict"],
              },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group: "",
            version: "v1",
            kind: "Namespace",
          },
          requestBody: {
            content: {
              "application/json": {
                schema: {
                  $ref: "#/components/schemas/io.k8s.api.core.v1.Namespace",
                },
              },
            },
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.api.core.v1.Namespace",
                  },
                },
              },
            },
          },
        },
      },
    },
    components: {
      schemas: {
        "io.k8s.api.core.v1.Namespace": {
          type: "object",
          required: ["apiVersion", "kind", "metadata"],
          properties: {
            apiVersion: {
              type: "string",
              enum: ["v1"],
            },
            kind: {
              type: "string",
              enum: ["Namespace"],
            },
            metadata: {
              $ref: "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
            },
            spec: {
              type: "object",
              properties: {
                finalizers: {
                  type: "array",
                  items: { type: "string" },
                },
              },
            },
            status: {
              type: "object",
              properties: {
                phase: {
                  type: "string",
                  enum: ["Active", "Terminating"],
                },
              },
            },
          },
        },
        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
          type: "object",
          properties: {
            name: { type: "string" },
            namespace: { type: "string" },
            creationTimestamp: { type: "string", format: "date-time" },
            labels: {
              type: "object",
              additionalProperties: { type: "string" },
            },
            annotations: {
              type: "object",
              additionalProperties: { type: "string" },
            },
          },
        },
      },
    },
  };

  return c.json(spec);
});

// OpenAPI v3 spec for apiextensions.k8s.io/v1
app.get("/openapi/v3/apis/apiextensions.k8s.io/v1", async (c) => {
  const spec = {
    openapi: "3.0.0",
    info: {
      title: "apiextensions.k8s.io/v1",
      version: "v1",
      description: "OpenAPI specification for apiextensions.k8s.io/v1",
    },
    servers: [{ url: new URL(c.req.url).origin }],
    paths: {
      "/apis/apiextensions.k8s.io/v1/customresourcedefinitions": {
        get: {
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinitionList",
                  },
                },
              },
            },
          },
        },
        post: {
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          requestBody: {
            content: {
              "application/json": {
                schema: {
                  $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                },
              },
            },
          },
          responses: {
            "201": {
              description: "Created",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                  },
                },
              },
            },
          },
        },
      },
      "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{name}": {
        get: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                  },
                },
              },
            },
          },
        },
        put: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "fieldValidation",
              in: "query",
              schema: {
                type: "string",
                enum: ["Ignore", "Warn", "Strict"],
              },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          requestBody: {
            content: {
              "application/json": {
                schema: {
                  $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                },
              },
            },
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                  },
                },
              },
            },
          },
        },
        patch: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "fieldValidation",
              in: "query",
              schema: {
                type: "string",
                enum: ["Ignore", "Warn", "Strict"],
              },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          requestBody: {
            content: {
              "application/json": {
                schema: {
                  $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                },
              },
            },
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                  },
                },
              },
            },
          },
        },
        delete: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group: "apiextensions.k8s.io",
            version: "v1",
            kind: "CustomResourceDefinition",
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: {
                    $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                  },
                },
              },
            },
          },
        },
      },
    },
    components: {
      schemas: {
        "io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition":
          {
            type: "object",
            required: ["apiVersion", "kind", "metadata", "spec"],
            properties: {
              apiVersion: {
                type: "string",
                enum: ["apiextensions.k8s.io/v1"],
              },
              kind: {
                type: "string",
                enum: ["CustomResourceDefinition"],
              },
              metadata: {
                $ref: "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
              },
              spec: {
                type: "object",
                required: ["group", "names", "scope", "versions"],
                properties: {
                  group: {
                    type: "string",
                    description:
                      "group is the API group of the defined custom resource",
                  },
                  names: {
                    type: "object",
                    required: ["kind", "plural"],
                    properties: {
                      kind: {
                        type: "string",
                        description:
                          "kind is the serialized kind of the resource",
                      },
                      plural: {
                        type: "string",
                        description:
                          "plural is the plural name of the resource to serve",
                      },
                      shortNames: {
                        type: "array",
                        items: { type: "string" },
                        description:
                          "shortNames are short names for the resource",
                      },
                    },
                  },
                  scope: {
                    type: "string",
                    enum: ["Cluster", "Namespaced"],
                    description:
                      "scope indicates whether the defined custom resource is cluster- or namespace-scoped",
                  },
                  versions: {
                    type: "array",
                    items: {
                      type: "object",
                      required: ["name", "served", "storage"],
                      properties: {
                        name: {
                          type: "string",
                          description: "name is the version name",
                        },
                        served: {
                          type: "boolean",
                          description:
                            "served is a flag enabling/disabling this version from being served via REST APIs",
                        },
                        storage: {
                          type: "boolean",
                          description:
                            "storage indicates this version should be used when persisting custom resources to storage",
                        },
                      },
                    },
                  },
                },
              },
              status: {
                type: "object",
                properties: {
                  acceptedNames: {
                    type: "object",
                    properties: {
                      kind: { type: "string" },
                      plural: { type: "string" },
                      shortNames: {
                        type: "array",
                        items: { type: "string" },
                      },
                    },
                  },
                  conditions: {
                    type: "array",
                    items: {
                      type: "object",
                      properties: {
                        type: { type: "string" },
                        status: { type: "string" },
                        lastTransitionTime: {
                          type: "string",
                          format: "date-time",
                        },
                        reason: { type: "string" },
                        message: { type: "string" },
                      },
                    },
                  },
                },
              },
            },
          },
        "io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinitionList":
          {
            type: "object",
            required: ["apiVersion", "kind", "items"],
            properties: {
              apiVersion: {
                type: "string",
                enum: ["apiextensions.k8s.io/v1"],
              },
              kind: {
                type: "string",
                enum: ["CustomResourceDefinitionList"],
              },
              metadata: {
                type: "object",
                properties: {
                  continue: { type: "string" },
                  resourceVersion: { type: "string" },
                },
              },
              items: {
                type: "array",
                items: {
                  $ref: "#/components/schemas/io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1.CustomResourceDefinition",
                },
              },
            },
          },
        "io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta": {
          type: "object",
          properties: {
            name: { type: "string" },
            namespace: { type: "string" },
            creationTimestamp: { type: "string", format: "date-time" },
            labels: {
              type: "object",
              additionalProperties: { type: "string" },
            },
            annotations: {
              type: "object",
              additionalProperties: { type: "string" },
            },
          },
        },
      },
    },
  };

  return c.json(spec);
});

// OpenAPI v3 spec for CRD group/version
app.get("/openapi/v3/apis/:group/:version", async (c) => {
  const { group, version } = c.req.param();

  const { results } = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=?",
  )
    .bind(group, version)
    .all();

  if (!results || results.length === 0) {
    return c.json({ message: "Not Found" }, 404);
  }

  const spec = {
    openapi: "3.0.0",
    info: {
      title: `${group}/${version}`,
      version: version,
      description: `OpenAPI specification for ${group}/${version}`,
    },
    servers: [{ url: new URL(c.req.url).origin }],
    paths: {} as Record<string, any>,
    components: {
      schemas: {} as Record<string, any>,
    },
  };

  for (const crd of results) {
    const plural = crd.plural;
    const kind = crd.kind;
    const scope = crd.scope;

    // Create a basic schema for the CRD if none exists
    const schema = {
      type: "object",
      required: ["apiVersion", "kind", "metadata"],
      properties: {
        apiVersion: {
          type: "string",
          enum: [`${group}/${version}`],
        },
        kind: {
          type: "string",
          enum: [kind],
        },
        metadata: {
          $ref: "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
        },
        spec: {
          type: "object",
          additionalProperties: true,
        },
        status: {
          type: "object",
          additionalProperties: true,
        },
      },
    };

    // Add paths for cluster-scoped resources
    if (scope === "Cluster") {
      spec.paths[`/apis/${group}/${version}/${plural}/{name}`] = {
        patch: {
          parameters: [
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "fieldValidation",
              in: "query",
              schema: {
                type: "string",
                enum: ["Ignore", "Warn", "Strict"],
              },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group,
            version,
            kind,
          },
          requestBody: {
            content: {
              "application/json": {
                schema: { $ref: `#/components/schemas/${kind}` },
              },
            },
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: { $ref: `#/components/schemas/${kind}` },
                },
              },
            },
          },
        },
      };
    } else {
      // Add paths for namespaced resources
      spec.paths[
        `/apis/${group}/${version}/namespaces/{namespace}/${plural}/{name}`
      ] = {
        patch: {
          parameters: [
            {
              name: "namespace",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "name",
              in: "path",
              required: true,
              schema: { type: "string" },
            },
            {
              name: "fieldValidation",
              in: "query",
              schema: {
                type: "string",
                enum: ["Ignore", "Warn", "Strict"],
              },
            },
          ],
          "x-kubernetes-group-version-kind": {
            group,
            version,
            kind,
          },
          requestBody: {
            content: {
              "application/json": {
                schema: { $ref: `#/components/schemas/${kind}` },
              },
            },
          },
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: { $ref: `#/components/schemas/${kind}` },
                },
              },
            },
          },
        },
      };
    }

    // Add schema for this CRD
    spec.components.schemas[kind] = schema;
  }

  // Add common ObjectMeta schema
  spec.components.schemas["io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"] = {
    type: "object",
    properties: {
      name: { type: "string" },
      namespace: { type: "string" },
      creationTimestamp: { type: "string", format: "date-time" },
      labels: {
        type: "object",
        additionalProperties: { type: "string" },
      },
      annotations: {
        type: "object",
        additionalProperties: { type: "string" },
      },
    },
  };

  return c.json(spec);
});

// --- Discovery endpoints for kubectl compatibility ---

// Root API discovery
app.get("/api", async (c) => {
  return c.json({
    kind: "APIVersions",
    versions: ["v1"],
    serverAddressByClientCIDRs: [
      {
        clientCIDR: "0.0.0.0/0",
        serverAddress: c.req.url.replace(/\/api$/, ""),
      },
    ],
  });
});

// Core API v1 discovery
app.get("/api/v1", async (c) => {
  return c.json({
    kind: "APIResourceList",
    apiVersion: "v1",
    groupVersion: "v1",
    resources: [],
  });
});

// API groups discovery
app.get("/apis", async (c) => {
  const { results } = await c.env.DB.prepare(
    "SELECT DISTINCT group_name, version FROM crds",
  ).all();
  const groups = new Map();

  // Always include apiextensions.k8s.io
  groups.set("apiextensions.k8s.io", {
    name: "apiextensions.k8s.io",
    versions: [{ groupVersion: "apiextensions.k8s.io/v1", version: "v1" }],
    preferredVersion: {
      groupVersion: "apiextensions.k8s.io/v1",
      version: "v1",
    },
  });

  // Add dynamic groups from CRDs
  for (const row of results || []) {
    const groupName = row.group_name;
    const version = row.version;
    if (!groups.has(groupName)) {
      groups.set(groupName, {
        name: groupName,
        versions: [],
        preferredVersion: { groupVersion: `${groupName}/${version}`, version },
      });
    }
    groups.get(groupName).versions.push({
      groupVersion: `${groupName}/${version}`,
      version,
    });
  }

  return c.json({
    kind: "APIGroupList",
    apiVersion: "v1",
    groups: Array.from(groups.values()),
  });
});

// Specific API group discovery
app.get("/apis/:group", async (c) => {
  const { group } = c.req.param();

  if (group === "apiextensions.k8s.io") {
    return c.json({
      kind: "APIGroup",
      apiVersion: "v1",
      name: "apiextensions.k8s.io",
      versions: [{ groupVersion: "apiextensions.k8s.io/v1", version: "v1" }],
      preferredVersion: {
        groupVersion: "apiextensions.k8s.io/v1",
        version: "v1",
      },
    });
  }

  const { results } = await c.env.DB.prepare(
    "SELECT DISTINCT version FROM crds WHERE group_name=?",
  )
    .bind(group)
    .all();
  if (!results || results.length === 0) {
    return c.json({ message: "Not Found" }, 404);
  }

  const versions = results.map((r: any) => ({
    groupVersion: `${group}/${r.version}`,
    version: r.version,
  }));

  return c.json({
    kind: "APIGroup",
    apiVersion: "v1",
    name: group,
    versions,
    preferredVersion: versions[0],
  });
});

// API resource discovery for specific group/version
app.get("/apis/:group/:version", async (c) => {
  const { group, version } = c.req.param();

  if (group === "apiextensions.k8s.io" && version === "v1") {
    return c.json({
      kind: "APIResourceList",
      apiVersion: "v1",
      groupVersion: "apiextensions.k8s.io/v1",
      resources: [
        {
          name: "customresourcedefinitions",
          singularName: "customresourcedefinition",
          namespaced: false,
          kind: "CustomResourceDefinition",
          verbs: [
            "create",
            "delete",
            "get",
            "list",
            "patch",
            "update",
            "watch",
          ],
          shortNames: ["crd", "crds"],
        },
      ],
    });
  }

  const { results } = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=?",
  )
    .bind(group, version)
    .all();

  if (!results || results.length === 0) {
    return c.json({ message: "Not Found" }, 404);
  }

  const resources = results.map((r: any) => {
    const resource: any = {
      name: r.plural,
      singularName: r.kind.toLowerCase(),
      namespaced: r.scope === "Namespaced",
      kind: r.kind,
      verbs: ["create", "delete", "get", "list", "patch", "update", "watch"],
    };

    if (r.short_names) {
      resource.shortNames = JSON.parse(r.short_names);
    }

    return resource;
  });

  return c.json({
    kind: "APIResourceList",
    apiVersion: "v1",
    groupVersion: `${group}/${version}`,
    resources,
  });
});

// --- 1. apiextensions.k8s.io/v1/customresourcedefinitions ---
app.get(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
  async (c) => {
    const { results } = await c.env.DB.prepare("SELECT * FROM crds").all();
    const items = (results || []).map((r: any) => {
      const names: any = { plural: r.plural, kind: r.kind };
      if (r.short_names) {
        names.shortNames = JSON.parse(r.short_names);
      }

      return {
        apiVersion: "apiextensions.k8s.io/v1",
        kind: "CustomResourceDefinition",
        metadata: { name: r.name, creationTimestamp: r.created_at },
        spec: {
          group: r.group_name,
          versions: [{ name: r.version, served: true, storage: true }],
          scope: r.scope,
          names,
        },
      };
    });

    // Handle kubectl table format requests
    const accept = c.req.header("Accept") || "";
    if (accept.includes("application/json;as=Table")) {
      return c.json({
        kind: "Table",
        apiVersion: "meta.k8s.io/v1",
        metadata: {},
        columnDefinitions: [
          { name: "Name", type: "string", format: "name" },
          { name: "Created At", type: "string" },
        ],
        rows: items.map((item) => ({
          cells: [item.metadata.name, item.metadata.creationTimestamp],
          object: item,
        })),
      });
    }

    return c.json({
      apiVersion: "apiextensions.k8s.io/v1",
      kind: "CustomResourceDefinitionList",
      items,
    });
  },
);

app.post(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
  async (c) => {
    const body = await c.req.json();
    const spec = body.spec;
    const group = spec.group;
    const version = spec.versions[0].name;
    const kind = spec.names.kind;
    const plural = spec.names.plural;
    const shortNames = spec.names.shortNames
      ? JSON.stringify(spec.names.shortNames)
      : null;
    const scope = spec.scope || "Cluster";
    const name = `${plural}.${group}`;

    await c.env.DB.prepare(
      "INSERT INTO crds (name, group_name, version, kind, plural, short_names, scope) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
      .bind(name, group, version, kind, plural, shortNames, scope)
      .run();

    return c.json(
      {
        apiVersion: "apiextensions.k8s.io/v1",
        kind: "CustomResourceDefinition",
        metadata: { name },
        spec,
      },
      201,
    );
  },
);

app.get(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
  async (c) => {
    const { name } = c.req.param();
    const result = await c.env.DB.prepare("SELECT * FROM crds WHERE name=?")
      .bind(name)
      .first();
    if (!result) return c.json({ message: "Not Found" }, 404);

    const names: any = { plural: result.plural, kind: result.kind };
    if (result.short_names) {
      names.shortNames = JSON.parse(result.short_names);
    }

    return c.json({
      apiVersion: "apiextensions.k8s.io/v1",
      kind: "CustomResourceDefinition",
      metadata: { name: result.name, creationTimestamp: result.created_at },
      spec: {
        group: result.group_name,
        versions: [{ name: result.version, served: true, storage: true }],
        scope: result.scope,
        names,
      },
    });
  },
);

app.put(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
  async (c) => {
    const { name } = c.req.param();
    const body = await c.req.json();
    const spec = body.spec;
    const group = spec.group;
    const version = spec.versions[0].name;
    const kind = spec.names.kind;
    const plural = spec.names.plural;
    const shortNames = spec.names.shortNames
      ? JSON.stringify(spec.names.shortNames)
      : null;
    const scope = spec.scope || "Cluster";

    // Update or insert the CRD
    await c.env.DB.prepare(
      "INSERT OR REPLACE INTO crds (name, group_name, version, kind, plural, short_names, scope) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
      .bind(name, group, version, kind, plural, shortNames, scope)
      .run();

    return c.json({
      apiVersion: "apiextensions.k8s.io/v1",
      kind: "CustomResourceDefinition",
      metadata: { name },
      spec,
    });
  },
);

app.patch(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
  async (c) => {
    const { name } = c.req.param();
    const body = await c.req.json();

    // Get existing CRD
    const result = await c.env.DB.prepare("SELECT * FROM crds WHERE name=?")
      .bind(name)
      .first();
    if (!result) return c.json({ message: "Not Found" }, 404);

    // Merge the patch with existing spec
    const existingSpec = {
      group: result.group_name,
      versions: [{ name: result.version, served: true, storage: true }],
      scope: result.scope,
      names: {
        plural: result.plural,
        kind: result.kind,
        shortNames: result.short_names
          ? JSON.parse(result.short_names)
          : undefined,
      },
    };

    const updatedSpec = { ...existingSpec, ...body.spec };
    const group = updatedSpec.group;
    const version = updatedSpec.versions[0].name;
    const kind = updatedSpec.names.kind;
    const plural = updatedSpec.names.plural;
    const shortNames = updatedSpec.names.shortNames
      ? JSON.stringify(updatedSpec.names.shortNames)
      : null;
    const scope = updatedSpec.scope || "Cluster";

    // Update the CRD
    await c.env.DB.prepare(
      "UPDATE crds SET group_name=?, version=?, kind=?, plural=?, short_names=?, scope=? WHERE name=?",
    )
      .bind(group, version, kind, plural, shortNames, scope, name)
      .run();

    return c.json({
      apiVersion: "apiextensions.k8s.io/v1",
      kind: "CustomResourceDefinition",
      metadata: { name, creationTimestamp: result.created_at },
      spec: updatedSpec,
    });
  },
);

app.delete(
  "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name",
  async (c) => {
    const { name } = c.req.param();

    // Get the CRD before deleting it
    const result = await c.env.DB.prepare("SELECT * FROM crds WHERE name=?")
      .bind(name)
      .first();
    if (!result) return c.json({ message: "Not Found" }, 404);

    // Delete all resources of this CRD type first
    await c.env.DB.prepare(
      "DELETE FROM resources WHERE group_name=? AND version=? AND plural=?",
    )
      .bind(result.group_name, result.version, result.plural)
      .run();

    // Delete the CRD itself
    await c.env.DB.prepare("DELETE FROM crds WHERE name=?").bind(name).run();

    const names: any = { plural: result.plural, kind: result.kind };
    if (result.short_names) {
      names.shortNames = JSON.parse(result.short_names);
    }

    // Return the deleted CRD object
    return c.json({
      apiVersion: "apiextensions.k8s.io/v1",
      kind: "CustomResourceDefinition",
      metadata: { name: result.name, creationTimestamp: result.created_at },
      spec: {
        group: result.group_name,
        versions: [{ name: result.version, served: true, storage: true }],
        scope: result.scope,
        names,
      },
    });
  },
);

// --- 2. Dynamic resource routes ---

// --- Cluster-scoped resources ---

// List cluster-scoped resources
app.get("/apis/:group/:version/:plural", async (c) => {
  const { group, version, plural } = c.req.param();
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND namespace IS NULL",
  )
    .bind(group, version, plural)
    .all();

  const items = (results || []).map((r: any) => ({
    apiVersion: `${group}/${version}`,
    kind: r.kind,
    metadata: {
      name: r.name,
      creationTimestamp: r.created_at,
    },
    spec: JSON.parse(r.spec),
    status: r.status ? JSON.parse(r.status) : {},
  }));

  const kind = items[0]?.kind || plural[0].toUpperCase() + plural.slice(1);

  // Handle kubectl table format requests
  const accept = c.req.header("Accept") || "";
  if (accept.includes("application/json;as=Table")) {
    return c.json({
      kind: "Table",
      apiVersion: "meta.k8s.io/v1",
      metadata: {},
      columnDefinitions: [
        {
          name: "Name",
          type: "string",
          format: "name",
          description: "Name must be unique within a namespace",
        },
        {
          name: "Age",
          type: "string",
          description:
            "CreationTimestamp is a timestamp representing the server time when this object was created",
        },
      ],
      rows: items.map((item) => ({
        cells: [item.metadata.name, item.metadata.creationTimestamp],
        object: item,
      })),
    });
  }

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: `${kind}List`,
    items,
  });
});

// Create cluster-scoped resource
app.post("/apis/:group/:version/:plural", async (c) => {
  const { group, version, plural } = c.req.param();
  const body = await c.req.json();
  const name = body.metadata?.name || uuid();

  const crd = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=? AND plural=?",
  )
    .bind(group, version, plural)
    .first();
  if (!crd) return c.json({ message: "Unknown resource type" }, 404);

  await c.env.DB.prepare(
    "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
  )
    .bind(
      uuid(),
      group,
      version,
      crd.kind,
      plural,
      name,
      JSON.stringify(body.spec),
      null,
    )
    .run();

  const response = c.json(
    {
      apiVersion: `${group}/${version}`,
      kind: crd.kind,
      metadata: { name, creationTimestamp: new Date().toISOString() },
      spec: body.spec,
    },
    201,
  );

  // Notify controllers of resource creation
  await notifyControllers("created", {
    group,
    version,
    plural,
    name,
    namespace: null,
    kind: crd.kind,
    spec: body.spec,
    env: c.env,
  });

  return response;
});

// Get single cluster-scoped resource
app.get("/apis/:group/:version/:plural/:name", async (c) => {
  const { group, version, plural, name } = c.req.param();
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL",
  )
    .bind(group, version, plural, name)
    .first();
  if (!result) return c.json({ message: "Not Found" }, 404);

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  });
});

// Patch cluster-scoped resource
app.patch("/apis/:group/:version/:plural/:name", async (c) => {
  const { group, version, plural, name } = c.req.param();
  const body = await c.req.json();
  const current = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL",
  )
    .bind(group, version, plural, name)
    .first();
  if (!current) return c.json({ message: "Not Found" }, 404);

  const updatedSpec = { ...JSON.parse(current.spec), ...body.spec };
  await c.env.DB.prepare(
    "UPDATE resources SET spec=? WHERE name=? AND namespace IS NULL",
  )
    .bind(JSON.stringify(updatedSpec), name)
    .run();

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: current.kind,
    metadata: { name, creationTimestamp: current.created_at },
    spec: updatedSpec,
  });
});

// Delete cluster-scoped resource
app.delete("/apis/:group/:version/:plural/:name", async (c) => {
  const { group, version, plural, name } = c.req.param();

  // Get the resource before deleting it
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL",
  )
    .bind(group, version, plural, name)
    .first();
  if (!result) return c.json({ message: "Not Found" }, 404);

  // Notify controllers of resource deletion BEFORE deleting from DB
  await notifyControllers("deleted", {
    group,
    version,
    plural,
    name,
    namespace: null,
    kind: result.kind,
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
    env: c.env,
  });

  // Delete the resource
  await c.env.DB.prepare(
    "DELETE FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL",
  )
    .bind(group, version, plural, name)
    .run();

  // Return the deleted object
  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  });
});

// --- Namespaced resources ---

// List namespaced resources
app.get("/apis/:group/:version/namespaces/:namespace/:plural", async (c) => {
  const { group, version, namespace, plural } = c.req.param();
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND namespace=?",
  )
    .bind(group, version, plural, namespace)
    .all();

  const items = (results || []).map((r: any) => ({
    apiVersion: `${group}/${version}`,
    kind: r.kind,
    metadata: {
      name: r.name,
      namespace: r.namespace,
      creationTimestamp: r.created_at,
    },
    spec: JSON.parse(r.spec),
    status: r.status ? JSON.parse(r.status) : {},
  }));

  const kind = items[0]?.kind || plural[0].toUpperCase() + plural.slice(1);

  // Handle kubectl table format requests
  const accept = c.req.header("Accept") || "";
  if (accept.includes("application/json;as=Table")) {
    return c.json({
      kind: "Table",
      apiVersion: "meta.k8s.io/v1",
      metadata: {},
      columnDefinitions: [
        {
          name: "Name",
          type: "string",
          format: "name",
          description: "Name must be unique within a namespace",
        },
        {
          name: "Namespace",
          type: "string",
          description:
            "Namespace defines the space within which each name must be unique",
        },
        {
          name: "Age",
          type: "string",
          description:
            "CreationTimestamp is a timestamp representing the server time when this object was created",
        },
      ],
      rows: items.map((item) => ({
        cells: [
          item.metadata.name,
          item.metadata.namespace,
          item.metadata.creationTimestamp,
        ],
        object: item,
      })),
    });
  }

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: `${kind}List`,
    items,
  });
});

// Create namespaced resource
app.post("/apis/:group/:version/namespaces/:namespace/:plural", async (c) => {
  const { group, version, namespace, plural } = c.req.param();
  const body = await c.req.json();
  const name = body.metadata?.name || uuid();

  const crd = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=? AND plural=?",
  )
    .bind(group, version, plural)
    .first();
  if (!crd) return c.json({ message: "Unknown resource type" }, 404);

  await c.env.DB.prepare(
    "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
  )
    .bind(
      uuid(),
      group,
      version,
      crd.kind,
      plural,
      name,
      JSON.stringify(body.spec),
      namespace,
    )
    .run();

  const response = c.json(
    {
      apiVersion: `${group}/${version}`,
      kind: crd.kind,
      metadata: {
        name,
        namespace,
        creationTimestamp: new Date().toISOString(),
      },
      spec: body.spec,
    },
    201,
  );

  // Notify controllers of resource creation
  await notifyControllers("created", {
    group,
    version,
    plural,
    name,
    namespace,
    kind: crd.kind,
    spec: body.spec,
    env: c.env,
  });

  return response;
});

// Get single namespaced resource
app.get(
  "/apis/:group/:version/namespaces/:namespace/:plural/:name",
  async (c) => {
    const { group, version, namespace, plural, name } = c.req.param();
    const result = await c.env.DB.prepare(
      "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?",
    )
      .bind(group, version, plural, name, namespace)
      .first();
    if (!result) return c.json({ message: "Not Found" }, 404);

    return c.json({
      apiVersion: `${group}/${version}`,
      kind: result.kind,
      metadata: {
        name: result.name,
        namespace: result.namespace,
        creationTimestamp: result.created_at,
      },
      spec: JSON.parse(result.spec),
      status: result.status ? JSON.parse(result.status) : {},
    });
  },
);

// Patch namespaced resource
app.patch(
  "/apis/:group/:version/namespaces/:namespace/:plural/:name",
  async (c) => {
    const { group, version, namespace, plural, name } = c.req.param();
    const body = await c.req.json();
    const current = await c.env.DB.prepare(
      "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?",
    )
      .bind(group, version, plural, name, namespace)
      .first();
    if (!current) return c.json({ message: "Not Found" }, 404);

    const updatedSpec = { ...JSON.parse(current.spec), ...body.spec };
    await c.env.DB.prepare(
      "UPDATE resources SET spec=? WHERE name=? AND namespace=?",
    )
      .bind(JSON.stringify(updatedSpec), name, namespace)
      .run();

    return c.json({
      apiVersion: `${group}/${version}`,
      kind: current.kind,
      metadata: { name, namespace, creationTimestamp: current.created_at },
      spec: updatedSpec,
    });
  },
);

// Delete namespaced resource
app.delete(
  "/apis/:group/:version/namespaces/:namespace/:plural/:name",
  async (c) => {
    const { group, version, namespace, plural, name } = c.req.param();

    // Get the resource before deleting it
    const result = await c.env.DB.prepare(
      "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?",
    )
      .bind(group, version, plural, name, namespace)
      .first();
    if (!result) return c.json({ message: "Not Found" }, 404);

    // Notify controllers of resource deletion BEFORE deleting from DB
    await notifyControllers("deleted", {
      group,
      version,
      plural,
      name,
      namespace,
      kind: result.kind,
      spec: JSON.parse(result.spec),
      status: result.status ? JSON.parse(result.status) : {},
      env: c.env,
    });

    // Delete the resource
    await c.env.DB.prepare(
      "DELETE FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?",
    )
      .bind(group, version, plural, name, namespace)
      .run();

    // Return the deleted object
    return c.json({
      apiVersion: `${group}/${version}`,
      kind: result.kind,
      metadata: {
        name: result.name,
        namespace: result.namespace,
        creationTimestamp: result.created_at,
      },
      spec: JSON.parse(result.spec),
      status: result.status ? JSON.parse(result.status) : {},
    });
  },
);

// Queue consumer and scheduled handler
export default {
  fetch: app.fetch,
  async queue(batch: MessageBatch<any>, env: Env): Promise<void> {
    // Let each controller handle the batch - they will filter messages themselves
    for (const controller of config.controllers) {
      if ("handleQueue" in controller) {
        try {
          await (controller as any).handleQueue(batch, env);
        } catch (error) {
          console.error(
            `Controller ${controller.constructor?.name || "unknown"} failed to handle queue:`,
            error,
          );
        }
      }
    }
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    console.log(
      `Running scheduled tasks at ${new Date(event.scheduledTime).toISOString()}`,
    );

    // Find the cloudflare controller specifically for scheduled handling
    const cloudflareController = config.controllers.find(
      (controller) => controller.constructor?.name === "CloudflareController",
    );

    if (cloudflareController && "handleScheduled" in cloudflareController) {
      await (cloudflareController as any).handleScheduled(event, env);
    }
  },
};

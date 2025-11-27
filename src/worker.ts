import { Hono } from "hono"
import { v4 as uuid } from "uuid"

type Env = { 
  Bindings: { 
    DB: D1Database
    GUBER_BUS: Queue
    CLOUDFLARE_API_TOKEN: string
    CLOUDFLARE_ACCOUNT_ID: string
    GUBER_NAME: string
    GUBER_DOMAIN: string
  } 
}

interface ScheduledEvent {
  scheduledTime: number
  cron: string
}

const app = new Hono<Env>()

// --- Discovery endpoints for kubectl compatibility ---

// Root API discovery
app.get("/api", async c => {
  return c.json({
    kind: "APIVersions",
    versions: ["v1"],
    serverAddressByClientCIDRs: [
      {
        clientCIDR: "0.0.0.0/0",
        serverAddress: c.req.url.replace(/\/api$/, "")
      }
    ]
  })
})

// Core API v1 discovery
app.get("/api/v1", async c => {
  return c.json({
    kind: "APIResourceList",
    apiVersion: "v1",
    groupVersion: "v1",
    resources: []
  })
})

// API groups discovery
app.get("/apis", async c => {
  const { results } = await c.env.DB.prepare("SELECT DISTINCT group_name, version FROM crds").all()
  const groups = new Map()
  
  // Always include apiextensions.k8s.io
  groups.set("apiextensions.k8s.io", {
    name: "apiextensions.k8s.io",
    versions: [{ groupVersion: "apiextensions.k8s.io/v1", version: "v1" }],
    preferredVersion: { groupVersion: "apiextensions.k8s.io/v1", version: "v1" }
  })
  
  // Add dynamic groups from CRDs
  for (const row of (results || [])) {
    const groupName = row.group_name
    const version = row.version
    if (!groups.has(groupName)) {
      groups.set(groupName, {
        name: groupName,
        versions: [],
        preferredVersion: { groupVersion: `${groupName}/${version}`, version }
      })
    }
    groups.get(groupName).versions.push({
      groupVersion: `${groupName}/${version}`,
      version
    })
  }

  return c.json({
    kind: "APIGroupList",
    apiVersion: "v1",
    groups: Array.from(groups.values())
  })
})

// Specific API group discovery
app.get("/apis/:group", async c => {
  const { group } = c.req.param()
  
  if (group === "apiextensions.k8s.io") {
    return c.json({
      kind: "APIGroup",
      apiVersion: "v1",
      name: "apiextensions.k8s.io",
      versions: [{ groupVersion: "apiextensions.k8s.io/v1", version: "v1" }],
      preferredVersion: { groupVersion: "apiextensions.k8s.io/v1", version: "v1" }
    })
  }
  
  const { results } = await c.env.DB.prepare("SELECT DISTINCT version FROM crds WHERE group_name=?").bind(group).all()
  if (!results || results.length === 0) {
    return c.json({ message: "Not Found" }, 404)
  }
  
  const versions = results.map((r: any) => ({
    groupVersion: `${group}/${r.version}`,
    version: r.version
  }))
  
  return c.json({
    kind: "APIGroup",
    apiVersion: "v1",
    name: group,
    versions,
    preferredVersion: versions[0]
  })
})

// API resource discovery for specific group/version
app.get("/apis/:group/:version", async c => {
  const { group, version } = c.req.param()
  
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
          verbs: ["create", "delete", "get", "list", "patch", "update", "watch"],
          shortNames: ["crd", "crds"]
        }
      ]
    })
  }
  
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=?"
  ).bind(group, version).all()
  
  if (!results || results.length === 0) {
    return c.json({ message: "Not Found" }, 404)
  }
  
  const resources = results.map((r: any) => {
    const resource: any = {
      name: r.plural,
      singularName: r.kind.toLowerCase(),
      namespaced: r.scope === "Namespaced",
      kind: r.kind,
      verbs: ["create", "delete", "get", "list", "patch", "update", "watch"]
    }
    
    if (r.short_names) {
      resource.shortNames = JSON.parse(r.short_names)
    }
    
    return resource
  })
  
  return c.json({
    kind: "APIResourceList",
    apiVersion: "v1",
    groupVersion: `${group}/${version}`,
    resources
  })
})

// --- 1. apiextensions.k8s.io/v1/customresourcedefinitions ---
app.get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions", async c => {
  const { results } = await c.env.DB.prepare("SELECT * FROM crds").all()
  const items = (results || []).map((r: any) => {
    const names: any = { plural: r.plural, kind: r.kind }
    if (r.short_names) {
      names.shortNames = JSON.parse(r.short_names)
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
    }
  })
  
  // Handle kubectl table format requests
  const accept = c.req.header("Accept") || ""
  if (accept.includes("application/json;as=Table")) {
    return c.json({
      kind: "Table",
      apiVersion: "meta.k8s.io/v1",
      metadata: {},
      columnDefinitions: [
        { name: "Name", type: "string", format: "name" },
        { name: "Created At", type: "string" }
      ],
      rows: items.map(item => ({
        cells: [
          item.metadata.name,
          item.metadata.creationTimestamp
        ],
        object: item
      }))
    })
  }
  
  return c.json({
    apiVersion: "apiextensions.k8s.io/v1",
    kind: "CustomResourceDefinitionList",
    items,
  })
})

app.post("/apis/apiextensions.k8s.io/v1/customresourcedefinitions", async c => {
  const body = await c.req.json()
  const spec = body.spec
  const group = spec.group
  const version = spec.versions[0].name
  const kind = spec.names.kind
  const plural = spec.names.plural
  const shortNames = spec.names.shortNames ? JSON.stringify(spec.names.shortNames) : null
  const scope = spec.scope || "Cluster"
  const name = `${plural}.${group}`

  await c.env.DB.prepare(
    "INSERT INTO crds (name, group_name, version, kind, plural, short_names, scope) VALUES (?, ?, ?, ?, ?, ?, ?)"
  ).bind(name, group, version, kind, plural, shortNames, scope).run()

  return c.json({
    apiVersion: "apiextensions.k8s.io/v1",
    kind: "CustomResourceDefinition",
    metadata: { name },
    spec,
  }, 201)
})

app.get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name", async c => {
  const { name } = c.req.param()
  const result = await c.env.DB.prepare("SELECT * FROM crds WHERE name=?").bind(name).first()
  if (!result) return c.json({ message: "Not Found" }, 404)
  
  const names: any = { plural: result.plural, kind: result.kind }
  if (result.short_names) {
    names.shortNames = JSON.parse(result.short_names)
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
  })
})

app.delete("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/:name", async c => {
  const { name } = c.req.param()
  
  // Get the CRD before deleting it
  const result = await c.env.DB.prepare("SELECT * FROM crds WHERE name=?").bind(name).first()
  if (!result) return c.json({ message: "Not Found" }, 404)
  
  // Delete all resources of this CRD type first
  await c.env.DB.prepare(
    "DELETE FROM resources WHERE group_name=? AND version=? AND plural=?"
  ).bind(result.group_name, result.version, result.plural).run()
  
  // Delete the CRD itself
  await c.env.DB.prepare("DELETE FROM crds WHERE name=?").bind(name).run()
  
  const names: any = { plural: result.plural, kind: result.kind }
  if (result.short_names) {
    names.shortNames = JSON.parse(result.short_names)
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
  })
})

// --- 2. Dynamic resource routes ---

// --- Cluster-scoped resources ---

// List cluster-scoped resources
app.get("/apis/:group/:version/:plural", async c => {
  const { group, version, plural } = c.req.param()
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND namespace IS NULL"
  ).bind(group, version, plural).all()

  const items = (results || []).map((r: any) => ({
    apiVersion: `${group}/${version}`,
    kind: r.kind,
    metadata: {
      name: r.name,
      creationTimestamp: r.created_at,
    },
    spec: JSON.parse(r.spec),
    status: r.status ? JSON.parse(r.status) : {},
  }))

  const kind = items[0]?.kind || plural[0].toUpperCase() + plural.slice(1)
  
  // Handle kubectl table format requests
  const accept = c.req.header("Accept") || ""
  if (accept.includes("application/json;as=Table")) {
    return c.json({
      kind: "Table",
      apiVersion: "meta.k8s.io/v1",
      metadata: {},
      columnDefinitions: [
        { name: "Name", type: "string", format: "name", description: "Name must be unique within a namespace" },
        { name: "Age", type: "string", description: "CreationTimestamp is a timestamp representing the server time when this object was created" }
      ],
      rows: items.map(item => ({
        cells: [
          item.metadata.name,
          item.metadata.creationTimestamp
        ],
        object: item
      }))
    })
  }
  
  return c.json({
    apiVersion: `${group}/${version}`,
    kind: `${kind}List`,
    items,
  })
})

// Create cluster-scoped resource
app.post("/apis/:group/:version/:plural", async c => {
  const { group, version, plural } = c.req.param()
  const body = await c.req.json()
  const name = body.metadata?.name || uuid()

  const crd = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=? AND plural=?"
  ).bind(group, version, plural).first()
  if (!crd) return c.json({ message: "Unknown resource type" }, 404)

  await c.env.DB.prepare(
    "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(uuid(), group, version, crd.kind, plural, name, JSON.stringify(body.spec), null).run()

  // If this is a Cloudflare resource, queue it for provisioning
  if (group === "cf.guber.proc.io" && (crd.kind === "D1" || crd.kind === "Queue" || crd.kind === "Worker") && c.env.GUBER_BUS) {
    await c.env.GUBER_BUS.send({
      action: "create",
      resourceType: crd.kind.toLowerCase(),
      resourceName: name,
      group: group,
      kind: crd.kind,
      plural: plural,
      namespace: null,
      spec: body.spec
    })
  }

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: crd.kind,
    metadata: { name, creationTimestamp: new Date().toISOString() },
    spec: body.spec,
  }, 201)
})

// Get single cluster-scoped resource
app.get("/apis/:group/:version/:plural/:name", async c => {
  const { group, version, plural, name } = c.req.param()
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL"
  ).bind(group, version, plural, name).first()
  if (!result) return c.json({ message: "Not Found" }, 404)

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  })
})

// Patch cluster-scoped resource
app.patch("/apis/:group/:version/:plural/:name", async c => {
  const { group, version, plural, name } = c.req.param()
  const body = await c.req.json()
  const current = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL"
  ).bind(group, version, plural, name).first()
  if (!current) return c.json({ message: "Not Found" }, 404)

  const updatedSpec = { ...JSON.parse(current.spec), ...body.spec }
  await c.env.DB.prepare(
    "UPDATE resources SET spec=? WHERE name=? AND namespace IS NULL"
  ).bind(JSON.stringify(updatedSpec), name).run()

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: current.kind,
    metadata: { name, creationTimestamp: current.created_at },
    spec: updatedSpec,
  })
})

// Delete cluster-scoped resource
app.delete("/apis/:group/:version/:plural/:name", async c => {
  const { group, version, plural, name } = c.req.param()
  
  // Get the resource before deleting it
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL"
  ).bind(group, version, plural, name).first()
  if (!result) return c.json({ message: "Not Found" }, 404)

  // If this is a Cloudflare resource, queue it for deletion BEFORE deleting from DB
  if (group === "cf.guber.proc.io" && (result.kind === "D1" || result.kind === "Queue" || result.kind === "Worker") && c.env.GUBER_BUS) {
    const spec = JSON.parse(result.spec)
    const status = result.status ? JSON.parse(result.status) : {}
    await c.env.GUBER_BUS.send({
      action: "delete",
      resourceType: result.kind.toLowerCase(),
      resourceName: name,
      group: group,
      kind: result.kind,
      plural: plural,
      namespace: null,
      spec: spec,
      status: status
    })
  }

  // Delete the resource
  await c.env.DB.prepare(
    "DELETE FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace IS NULL"
  ).bind(group, version, plural, name).run()
  
  // Return the deleted object
  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  })
})

// --- Namespaced resources ---

// List namespaced resources
app.get("/apis/:group/:version/namespaces/:namespace/:plural", async c => {
  const { group, version, namespace, plural } = c.req.param()
  const { results } = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND namespace=?"
  ).bind(group, version, plural, namespace).all()

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
  }))

  const kind = items[0]?.kind || plural[0].toUpperCase() + plural.slice(1)
  
  // Handle kubectl table format requests
  const accept = c.req.header("Accept") || ""
  if (accept.includes("application/json;as=Table")) {
    return c.json({
      kind: "Table",
      apiVersion: "meta.k8s.io/v1",
      metadata: {},
      columnDefinitions: [
        { name: "Name", type: "string", format: "name", description: "Name must be unique within a namespace" },
        { name: "Namespace", type: "string", description: "Namespace defines the space within which each name must be unique" },
        { name: "Age", type: "string", description: "CreationTimestamp is a timestamp representing the server time when this object was created" }
      ],
      rows: items.map(item => ({
        cells: [
          item.metadata.name,
          item.metadata.namespace,
          item.metadata.creationTimestamp
        ],
        object: item
      }))
    })
  }
  
  return c.json({
    apiVersion: `${group}/${version}`,
    kind: `${kind}List`,
    items,
  })
})

// Create namespaced resource
app.post("/apis/:group/:version/namespaces/:namespace/:plural", async c => {
  const { group, version, namespace, plural } = c.req.param()
  const body = await c.req.json()
  const name = body.metadata?.name || uuid()

  const crd = await c.env.DB.prepare(
    "SELECT * FROM crds WHERE group_name=? AND version=? AND plural=?"
  ).bind(group, version, plural).first()
  if (!crd) return c.json({ message: "Unknown resource type" }, 404)

  await c.env.DB.prepare(
    "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  ).bind(uuid(), group, version, crd.kind, plural, name, JSON.stringify(body.spec), namespace).run()

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: crd.kind,
    metadata: { name, namespace, creationTimestamp: new Date().toISOString() },
    spec: body.spec,
  }, 201)
})

// Get single namespaced resource
app.get("/apis/:group/:version/namespaces/:namespace/:plural/:name", async c => {
  const { group, version, namespace, plural, name } = c.req.param()
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?"
  ).bind(group, version, plural, name, namespace).first()
  if (!result) return c.json({ message: "Not Found" }, 404)

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, namespace: result.namespace, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  })
})

// Patch namespaced resource
app.patch("/apis/:group/:version/namespaces/:namespace/:plural/:name", async c => {
  const { group, version, namespace, plural, name } = c.req.param()
  const body = await c.req.json()
  const current = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?"
  ).bind(group, version, plural, name, namespace).first()
  if (!current) return c.json({ message: "Not Found" }, 404)

  const updatedSpec = { ...JSON.parse(current.spec), ...body.spec }
  await c.env.DB.prepare(
    "UPDATE resources SET spec=? WHERE name=? AND namespace=?"
  ).bind(JSON.stringify(updatedSpec), name, namespace).run()

  return c.json({
    apiVersion: `${group}/${version}`,
    kind: current.kind,
    metadata: { name, namespace, creationTimestamp: current.created_at },
    spec: updatedSpec,
  })
})

// Delete namespaced resource
app.delete("/apis/:group/:version/namespaces/:namespace/:plural/:name", async c => {
  const { group, version, namespace, plural, name } = c.req.param()
  
  // Get the resource before deleting it
  const result = await c.env.DB.prepare(
    "SELECT * FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?"
  ).bind(group, version, plural, name, namespace).first()
  if (!result) return c.json({ message: "Not Found" }, 404)

  // Delete the resource
  await c.env.DB.prepare(
    "DELETE FROM resources WHERE group_name=? AND version=? AND plural=? AND name=? AND namespace=?"
  ).bind(group, version, plural, name, namespace).run()
  
  // Return the deleted object
  return c.json({
    apiVersion: `${group}/${version}`,
    kind: result.kind,
    metadata: { name: result.name, namespace: result.namespace, creationTimestamp: result.created_at },
    spec: JSON.parse(result.spec),
    status: result.status ? JSON.parse(result.status) : {},
  })
})

// Queue consumer for D1 provisioning
export default {
  fetch: app.fetch,
  async queue(batch: MessageBatch<any>, env: Env): Promise<void> {
    for (const message of batch.messages) {
      try {
        const { action, resourceType, resourceName, group, kind, plural, namespace, spec, status } = message.body
        
        if (action === "create") {
          if (resourceType === "d1") {
            await provisionD1Database(env, resourceName, group, kind, plural, namespace, spec)
          } else if (resourceType === "queue") {
            await provisionQueue(env, resourceName, group, kind, plural, namespace, spec)
          } else if (resourceType === "worker") {
            await provisionWorker(env, resourceName, group, kind, plural, namespace, spec)
          }
        } else if (action === "delete") {
          if (resourceType === "d1") {
            await deleteD1Database(env, resourceName, group, kind, plural, namespace, spec, status)
          } else if (resourceType === "queue") {
            await deleteQueue(env, resourceName, group, kind, plural, namespace, spec, status)
          } else if (resourceType === "worker") {
            await deleteWorker(env, resourceName, group, kind, plural, namespace, spec, status)
          }
        }
        
        message.ack()
      } catch (error) {
        console.error(`Failed to process queue message:`, error)
        message.retry()
      }
    }
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    console.log(`Running Cloudflare resource reconciliation at ${new Date(event.scheduledTime).toISOString()}`)
    await reconcileD1Databases(env)
    await reconcileQueues(env)
    await reconcileWorkers(env)
  }
}

function buildFullDatabaseName(resourceName: string, group: string, plural: string, namespace: string | null, instanceName: string): string {
  // Construct full database name: name-namespace-resource-type-instance
  const namespaceStr = namespace || "c"
  const resourceType = `${plural}-${group.replace(/\./g, '-')}`
  return `${resourceName}-${namespaceStr}-${resourceType}-${instanceName}`
}

async function provisionD1Database(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any) {
  const fullDatabaseName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  
  const requestBody: any = {
    name: fullDatabaseName
  }
  
  // Only add primary_location_hint if location is specified
  if (spec.location) {
    requestBody.primary_location_hint = spec.location
  }
  
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(requestBody)
  })
  
  if (response.ok) {
    const result = await response.json()
    const databaseId = result.result.uuid
    
    // Update the resource status in the database
    await env.DB.prepare(
      "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
    ).bind(JSON.stringify({
      state: "Ready",
      database_id: databaseId,
      createdAt: new Date().toISOString(),
      endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`
    }), resourceName).run()
    
    console.log(`D1 database ${fullDatabaseName} provisioned successfully with ID: ${databaseId}`)
  } else {
    const errorResponse = await response.json()
    
    // Check if the error is because the database already exists
    if (errorResponse.errors && errorResponse.errors.some((err: any) => err.code === 7502)) {
      console.log(`Database ${fullDatabaseName} already exists, attempting to find and match existing database`)
      
      // List existing databases to find the one with matching name
      const listResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
        }
      })
      
      if (listResponse.ok) {
        const listResult = await listResponse.json()
        const existingDb = listResult.result.find((db: any) => db.name === fullDatabaseName)
        
        if (existingDb) {
          const databaseId = existingDb.uuid
          
          // Update the resource status to match the existing database
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          ).bind(JSON.stringify({
            state: "Ready",
            database_id: databaseId,
            createdAt: existingDb.created_on,
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`
          }), resourceName).run()
          
          console.log(`Matched existing D1 database ${fullDatabaseName} with ID: ${databaseId}`)
        } else {
          console.error(`Could not find existing database ${fullDatabaseName} in account`)
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          ).bind(JSON.stringify({
            state: "Failed",
            error: "Database exists but could not be found in account"
          }), resourceName).run()
        }
      } else {
        console.error(`Failed to list databases to find existing ${fullDatabaseName}`)
        await env.DB.prepare(
          "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
        ).bind(JSON.stringify({
          state: "Failed",
          error: JSON.stringify(errorResponse)
        }), resourceName).run()
      }
    } else {
      console.error(`Failed to provision D1 database ${fullDatabaseName}:`, JSON.stringify(errorResponse))
      
      // Update status to failed
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
      ).bind(JSON.stringify({
        state: "Failed",
        error: JSON.stringify(errorResponse)
      }), resourceName).run()
    }
  }
}

async function deleteD1Database(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any, status?: any) {
  const fullDatabaseName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  // Get database ID from the passed status or spec
  const databaseId = status?.database_id
  
  if (databaseId) {
    try {
      const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`, {
        method: "DELETE",
        headers: {
          "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
        }
      })
      
      if (response.ok) {
        console.log(`D1 database ${fullDatabaseName} (ID: ${databaseId}) deleted successfully`)
      } else {
        const error = await response.text()
        console.error(`Failed to delete D1 database ${fullDatabaseName} (ID: ${databaseId}):`, error)
      }
    } catch (error) {
      console.error(`Error deleting D1 database ${fullDatabaseName}:`, error)
    }
  } else {
    console.log(`No database ID found for ${fullDatabaseName}, skipping Cloudflare deletion`)
  }
}

async function provisionQueue(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any) {
  const fullQueueName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  
  const requestBody: any = {
    queue_name: fullQueueName
  }
  
  // Add settings if specified
  if (spec.settings) {
    requestBody.settings = spec.settings
  }
  
  const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(requestBody)
  })
  
  if (response.ok) {
    const result = await response.json()
    const queueId = result.result.queue_id
    
    // Update the resource status in the database
    await env.DB.prepare(
      "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
    ).bind(JSON.stringify({
      state: "Ready",
      queue_id: queueId,
      createdAt: new Date().toISOString(),
      endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`
    }), resourceName).run()
    
    console.log(`Queue ${fullQueueName} provisioned successfully with ID: ${queueId}`)
  } else {
    const errorResponse = await response.json()
    
    // Check if the error is because the queue already exists
    if (errorResponse.errors && errorResponse.errors.some((err: any) => err.code === 10026)) {
      console.log(`Queue ${fullQueueName} already exists, attempting to find and match existing queue`)
      
      // List existing queues to find the one with matching name
      const listResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`, {
        method: "GET",
        headers: {
          "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
        }
      })
      
      if (listResponse.ok) {
        const listResult = await listResponse.json()
        const existingQueue = listResult.result.find((queue: any) => queue.queue_name === fullQueueName)
        
        if (existingQueue) {
          const queueId = existingQueue.queue_id
          
          // Update the resource status to match the existing queue
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          ).bind(JSON.stringify({
            state: "Ready",
            queue_id: queueId,
            createdAt: existingQueue.created_on,
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`
          }), resourceName).run()
          
          console.log(`Matched existing Queue ${fullQueueName} with ID: ${queueId}`)
        } else {
          console.error(`Could not find existing queue ${fullQueueName} in account`)
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          ).bind(JSON.stringify({
            state: "Failed",
            error: "Queue exists but could not be found in account"
          }), resourceName).run()
        }
      } else {
        console.error(`Failed to list queues to find existing ${fullQueueName}`)
        await env.DB.prepare(
          "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
        ).bind(JSON.stringify({
          state: "Failed",
          error: JSON.stringify(errorResponse)
        }), resourceName).run()
      }
    } else {
      console.error(`Failed to provision Queue ${fullQueueName}:`, JSON.stringify(errorResponse))
      
      // Update status to failed
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
      ).bind(JSON.stringify({
        state: "Failed",
        error: JSON.stringify(errorResponse)
      }), resourceName).run()
    }
  }
}

async function deleteQueue(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any, status?: any) {
  const fullQueueName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  // Get queue ID from the passed status or spec
  const queueId = status?.queue_id
  
  if (queueId) {
    try {
      const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`, {
        method: "DELETE",
        headers: {
          "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
        }
      })
      
      if (response.ok) {
        console.log(`Queue ${fullQueueName} (ID: ${queueId}) deleted successfully`)
      } else {
        const error = await response.text()
        console.error(`Failed to delete Queue ${fullQueueName} (ID: ${queueId}):`, error)
      }
    } catch (error) {
      console.error(`Error deleting Queue ${fullQueueName}:`, error)
    }
  } else {
    console.log(`No queue ID found for ${fullQueueName}, skipping Cloudflare deletion`)
  }
}

async function reconcileQueues(env: Env) {
  try {
    console.log("Starting Queue reconciliation...")
    
    // Get all Queue resources from our API
    const { results: apiResources } = await env.DB.prepare(
      "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='Queue'"
    ).all()
    
    // Get all Queues from Cloudflare
    const cloudflareResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
      }
    })
    
    if (!cloudflareResponse.ok) {
      console.error("Failed to fetch Queues from Cloudflare:", await cloudflareResponse.text())
      return
    }
    
    const cloudflareResult = await cloudflareResponse.json()
    const cloudflareQueues = cloudflareResult.result || []
    
    // Create maps for easier comparison
    const apiQueueMap = new Map()
    const cloudflareQueueMap = new Map()
    
    // Build API queue map with full names
    for (const resource of (apiResources || [])) {
      const fullQueueName = buildFullDatabaseName(resource.name, resource.group_name, resource.plural, resource.namespace, env.GUBER_NAME)
      apiQueueMap.set(fullQueueName, resource)
    }
    
    // Build Cloudflare queue map
    for (const queue of cloudflareQueues) {
      cloudflareQueueMap.set(queue.queue_name, queue)
    }
    
    console.log(`Found ${apiQueueMap.size} Queue resources in API and ${cloudflareQueueMap.size} queues in Cloudflare`)
    
    // Find queues that exist in Cloudflare but not in our API (orphaned queues)
    const orphanedQueues = []
    for (const [queueName, cloudflareQueue] of cloudflareQueueMap) {
      // Only consider queues that match our naming pattern
      if (queueName.includes('-') && (queueName.includes('-qs-cf-guber-proc-io') || queueName.includes('-q-cf-guber-proc-io'))) {
        if (!apiQueueMap.has(queueName)) {
          orphanedQueues.push(cloudflareQueue)
        }
      }
    }
    
    // Find resources that exist in our API but not in Cloudflare (missing queues)
    const missingQueues = []
    for (const [fullName, apiResource] of apiQueueMap) {
      if (!cloudflareQueueMap.has(fullName)) {
        missingQueues.push({ fullName, resource: apiResource })
      }
    }
    
    console.log(`Found ${orphanedQueues.length} orphaned queues and ${missingQueues.length} missing queues`)
    
    // Delete orphaned queues from Cloudflare
    for (const orphanedQueue of orphanedQueues) {
      try {
        console.log(`Deleting orphaned queue: ${orphanedQueue.queue_name} (ID: ${orphanedQueue.queue_id})`)
        
        const deleteResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${orphanedQueue.queue_id}`, {
          method: "DELETE",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
          }
        })
        
        if (deleteResponse.ok) {
          console.log(`Successfully deleted orphaned queue: ${orphanedQueue.queue_name}`)
        } else {
          const error = await deleteResponse.text()
          console.error(`Failed to delete orphaned queue ${orphanedQueue.queue_name}:`, error)
        }
      } catch (error) {
        console.error(`Error deleting orphaned queue ${orphanedQueue.queue_name}:`, error)
      }
    }
    
    // Create missing queues in Cloudflare
    for (const { fullName, resource } of missingQueues) {
      try {
        console.log(`Creating missing queue: ${fullName}`)
        
        const spec = JSON.parse(resource.spec)
        const requestBody: any = { queue_name: fullName }
        
        if (spec.settings) {
          requestBody.settings = spec.settings
        }
        
        const createResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(requestBody)
        })
        
        if (createResponse.ok) {
          const result = await createResponse.json()
          const queueId = result.result.queue_id
          
          // Update the resource status in our database
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = resource.namespace
            ? [JSON.stringify({
                state: "Ready",
                queue_id: queueId,
                createdAt: new Date().toISOString(),
                endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
                reconciledAt: new Date().toISOString()
              }), resource.name, resource.namespace]
            : [JSON.stringify({
                state: "Ready",
                queue_id: queueId,
                createdAt: new Date().toISOString(),
                endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
                reconciledAt: new Date().toISOString()
              }), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
          
          console.log(`Successfully created missing queue: ${fullName} with ID: ${queueId}`)
        } else {
          const error = await createResponse.text()
          console.error(`Failed to create missing queue ${fullName}:`, error)
          
          // Update status to failed
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = resource.namespace
            ? [JSON.stringify({
                state: "Failed",
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name, resource.namespace]
            : [JSON.stringify({
                state: "Failed", 
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
        }
      } catch (error) {
        console.error(`Error creating missing queue ${fullName}:`, error)
      }
    }
    
    console.log("Queue reconciliation completed")
  } catch (error) {
    console.error("Error during Queue reconciliation:", error)
  }
}

async function provisionWorker(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any) {
  const fullWorkerName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  const customDomain = `${resourceName}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`
  
  try {
    // Get the worker script content
    let script: string
    
    if (spec.scriptUrl) {
      const scriptResponse = await fetch(spec.scriptUrl, {
        redirect: 'follow',
        headers: {
          'User-Agent': 'Guber-Worker-Provisioner/1.0'
        }
      })
      if (!scriptResponse.ok) {
        throw new Error(`Failed to fetch script from ${spec.scriptUrl}: ${scriptResponse.status} ${scriptResponse.statusText}`)
      }
      script = await scriptResponse.text()
    } else if (spec.script) {
      script = spec.script
    } else {
      throw new Error("Worker must have either 'script' or 'scriptUrl' specified")
    }
    
    // Step 1: Deploy the worker script
    // Create multipart form data for the worker upload
    const formData = new FormData()
    
    // Check for source map if scriptUrl is provided
    let sourceMap: string | null = null
    if (spec.scriptUrl) {
      try {
        const sourceMapUrl = spec.scriptUrl + '.map'
        const sourceMapResponse = await fetch(sourceMapUrl, {
          redirect: 'follow',
          headers: {
            'User-Agent': 'Guber-Worker-Provisioner/1.0'
          }
        })
        if (sourceMapResponse.ok) {
          sourceMap = await sourceMapResponse.text()
          console.log(`Found source map at ${sourceMapUrl}`)
        }
      } catch (error) {
        // Source map is optional, continue without it
        console.log(`No source map found for ${spec.scriptUrl}`)
      }
    }
    
    // Always use module format with main_module
    const metadata: any = {
      main_module: "index.js",
      compatibility_date: spec.compatibility_date || "2023-05-18"
    }
    
    // Add compatibility settings if specified
    if (spec.compatibility_date) {
      metadata.compatibility_date = spec.compatibility_date
    }
    if (spec.compatibility_flags) {
      metadata.compatibility_flags = spec.compatibility_flags
    }
    
    // Add bindings if specified
    if (spec.bindings) {
      console.log(`Processing bindings for worker ${fullWorkerName}:`, JSON.stringify(spec.bindings, null, 2))
      const bindings: any[] = []
      
      // Handle D1 database bindings
      if (spec.bindings.d1_databases) {
        console.log(`Processing ${spec.bindings.d1_databases.length} D1 database bindings`)
        for (const d1Binding of spec.bindings.d1_databases) {
          console.log(`Looking up D1 binding: ${d1Binding.database_name} -> ${d1Binding.binding}`)
          
          // Look up the D1 resource to get its database_id
          const d1Resource = await env.DB.prepare(
            "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
          ).bind(d1Binding.database_name).first()
          
          console.log(`D1 resource lookup result:`, d1Resource)
          
          if (d1Resource && d1Resource.status) {
            const status = JSON.parse(d1Resource.status)
            console.log(`D1 resource status:`, status)
            if (status.database_id) {
              const binding = {
                type: "d1",
                name: d1Binding.binding,
                database_id: status.database_id
              }
              bindings.push(binding)
              console.log(`✅ Added D1 binding:`, binding)
            } else {
              console.log(`❌ D1 resource ${d1Binding.database_name} has no database_id in status`)
            }
          } else {
            console.log(`❌ D1 resource ${d1Binding.database_name} not found or has no status`)
          }
        }
      }
      
      // Handle Queue bindings
      if (spec.bindings.queues) {
        console.log(`Processing ${spec.bindings.queues.length} Queue bindings`)
        for (const queueBinding of spec.bindings.queues) {
          console.log(`Looking up Queue binding: ${queueBinding.queue_name} -> ${queueBinding.binding}`)
          
          // Look up the Queue resource to get its queue_id
          const queueResource = await env.DB.prepare(
            "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
          ).bind(queueBinding.queue_name).first()
          
          console.log(`Queue resource lookup result:`, queueResource)
          
          if (queueResource && queueResource.status) {
            const status = JSON.parse(queueResource.status)
            console.log(`Queue resource status:`, status)
            if (status.queue_id) {
              const binding = {
                type: "queue",
                name: queueBinding.binding,
                queue_name: status.queue_id
              }
              bindings.push(binding)
              console.log(`✅ Added Queue binding:`, binding)
            } else {
              console.log(`❌ Queue resource ${queueBinding.queue_name} has no queue_id in status`)
            }
          } else {
            console.log(`❌ Queue resource ${queueBinding.queue_name} not found or has no status`)
          }
        }
      }
      
      console.log(`Total bindings collected: ${bindings.length}`)
      if (bindings.length > 0) {
        metadata.bindings = bindings
        console.log(`✅ Added bindings to metadata:`, JSON.stringify(bindings, null, 2))
      } else {
        console.log(`❌ No bindings to add to metadata`)
      }
    } else {
      console.log(`No bindings specified for worker ${fullWorkerName}`)
    }
    
    formData.append('metadata', JSON.stringify(metadata))
    formData.append('index.js', new Blob([script], { type: 'application/javascript+module' }), 'index.js')
    
    // Add source map if available
    if (sourceMap) {
      formData.append('index.js.map', new Blob([sourceMap], { type: 'text/plain' }), 'index.js.map')
    }
    
    // Debug logging
    console.log(`=== Worker Deployment Debug Info ===`)
    console.log(`Worker Name: ${fullWorkerName}`)
    console.log(`Script URL: ${spec.scriptUrl || 'inline'}`)
    console.log(`Metadata: ${JSON.stringify(metadata, null, 2)}`)
    console.log(`Script Content (first 500 chars): ${script.substring(0, 500)}...`)
    console.log(`Script Content (last 200 chars): ...${script.substring(script.length - 200)}`)
    console.log(`Has Source Map: ${sourceMap ? 'yes' : 'no'}`)
    console.log(`FormData entries:`)
    for (const [key, value] of formData.entries()) {
      if (value instanceof Blob) {
        console.log(`  ${key}: Blob (${value.type}, ${value.size} bytes)`)
      } else {
        console.log(`  ${key}: ${value}`)
      }
    }
    console.log(`=== End Debug Info ===`)
    
    const deployResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`, {
      method: "PUT",
      headers: {
        "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
      },
      body: formData
    })
    
    if (!deployResponse.ok) {
      const errorResponse = await deployResponse.json()
      throw new Error(`Failed to deploy worker script: ${JSON.stringify(errorResponse)}`)
    }
    
    console.log(`Worker script ${fullWorkerName} deployed successfully`)
    
    // Step 2: Create custom domain
    const domainResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`, {
      method: "PUT",
      headers: {
        "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        hostname: customDomain,
        service: fullWorkerName,
        environment: "production"
      })
    })
    
    if (!domainResponse.ok) {
      const domainError = await domainResponse.json()
      console.error(`Failed to create custom domain ${customDomain}:`, JSON.stringify(domainError))
      
      // Still update status as partially successful (script deployed but no custom domain)
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
      ).bind(JSON.stringify({
        state: "PartiallyReady",
        worker_id: fullWorkerName,
        createdAt: new Date().toISOString(),
        endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
        customDomain: customDomain,
        domainError: JSON.stringify(domainError)
      }), resourceName).run()
      
      console.log(`Worker ${fullWorkerName} deployed but custom domain setup failed`)
      return
    }
    
    const domainResult = await domainResponse.json()
    console.log(`Custom domain ${customDomain} created successfully for worker ${fullWorkerName}`)
    
    // Step 3: Update the resource status in the database
    await env.DB.prepare(
      "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
    ).bind(JSON.stringify({
      state: "Ready",
      worker_id: fullWorkerName,
      createdAt: new Date().toISOString(),
      endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
      customDomain: customDomain,
      domainId: domainResult.result?.id,
      url: `https://${customDomain}`
    }), resourceName).run()
    
    console.log(`Worker ${fullWorkerName} provisioned successfully at ${customDomain}`)
    
  } catch (error) {
    console.error(`Failed to provision Worker ${fullWorkerName}:`, error)
    
    // Update status to failed
    await env.DB.prepare(
      "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
    ).bind(JSON.stringify({
      state: "Failed",
      error: error.message || String(error)
    }), resourceName).run()
  }
}

async function deleteWorker(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any, status?: any) {
  const fullWorkerName = buildFullDatabaseName(resourceName, group, plural, namespace, env.GUBER_NAME)
  const customDomain = `${resourceName}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`
  
  try {
    // Step 1: Delete custom domain if it exists
    if (status?.domainId) {
      const domainResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains/${status.domainId}`, {
        method: "DELETE",
        headers: {
          "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
        }
      })
      
      if (domainResponse.ok) {
        console.log(`Custom domain ${customDomain} deleted successfully`)
      } else {
        const error = await domainResponse.text()
        console.error(`Failed to delete custom domain ${customDomain}:`, error)
      }
    }
    
    // Step 2: Delete the worker script
    const scriptResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`, {
      method: "DELETE",
      headers: {
        "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
      }
    })
    
    if (scriptResponse.ok) {
      console.log(`Worker ${fullWorkerName} deleted successfully`)
    } else {
      const error = await scriptResponse.text()
      console.error(`Failed to delete Worker ${fullWorkerName}:`, error)
    }
  } catch (error) {
    console.error(`Error deleting Worker ${fullWorkerName}:`, error)
  }
}

async function reconcileWorkers(env: Env) {
  try {
    console.log("Starting Worker reconciliation...")
    
    // Get all Worker resources from our API
    const { results: apiResources } = await env.DB.prepare(
      "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='Worker'"
    ).all()
    
    // Get all Workers from Cloudflare
    const [workersResponse, domainsResponse] = await Promise.all([
      fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts`, {
        method: "GET",
        headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` }
      }),
      fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`, {
        method: "GET",
        headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` }
      })
    ])
    
    if (!workersResponse.ok) {
      console.error("Failed to fetch Workers from Cloudflare:", await workersResponse.text())
      return
    }
    
    if (!domainsResponse.ok) {
      console.error("Failed to fetch Worker domains from Cloudflare:", await domainsResponse.text())
      return
    }
    
    const workersResult = await workersResponse.json()
    const domainsResult = await domainsResponse.json()
    const cloudflareWorkers = workersResult.result || []
    const cloudflareDomains = domainsResult.result || []
    
    // Create maps for easier comparison
    const apiWorkerMap = new Map()
    const cloudflareWorkerMap = new Map()
    const cloudflareDomainMap = new Map()
    
    // Build API worker map with full names
    for (const resource of (apiResources || [])) {
      const fullWorkerName = buildFullDatabaseName(resource.name, resource.group_name, resource.plural, resource.namespace, env.GUBER_NAME)
      apiWorkerMap.set(fullWorkerName, resource)
    }
    
    // Build Cloudflare worker and domain maps
    for (const worker of cloudflareWorkers) {
      cloudflareWorkerMap.set(worker.id, worker)
    }
    
    for (const domain of cloudflareDomains) {
      cloudflareDomainMap.set(domain.hostname, domain)
    }
    
    console.log(`Found ${apiWorkerMap.size} Worker resources in API, ${cloudflareWorkerMap.size} workers, and ${cloudflareDomainMap.size} domains in Cloudflare`)
    
    // Find workers that exist in Cloudflare but not in our API (orphaned workers)
    const orphanedWorkers = []
    for (const [workerName, cloudflareWorker] of cloudflareWorkerMap) {
      // Only consider workers that match our naming pattern
      if (workerName.includes('-') && (workerName.includes('-workers-cf-guber-proc-io') || workerName.includes('-worker-cf-guber-proc-io'))) {
        if (!apiWorkerMap.has(workerName)) {
          orphanedWorkers.push(cloudflareWorker)
        }
      }
    }
    
    // Find orphaned domains
    const orphanedDomains = []
    for (const [hostname, domain] of cloudflareDomainMap) {
      if (hostname.endsWith(`.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`)) {
        const workerName = hostname.split('.')[0]
        const found = Array.from(apiWorkerMap.values()).some(resource => resource.name === workerName)
        if (!found) {
          orphanedDomains.push(domain)
        }
      }
    }
    
    // Find resources that exist in our API but not in Cloudflare (missing workers)
    const missingWorkers = []
    for (const [fullName, apiResource] of apiWorkerMap) {
      if (!cloudflareWorkerMap.has(fullName)) {
        missingWorkers.push({ fullName, resource: apiResource })
      }
    }
    
    console.log(`Found ${orphanedWorkers.length} orphaned workers, ${orphanedDomains.length} orphaned domains, and ${missingWorkers.length} missing workers`)
    
    // Delete orphaned domains first
    for (const orphanedDomain of orphanedDomains) {
      try {
        console.log(`Deleting orphaned domain: ${orphanedDomain.hostname}`)
        
        const deleteResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains/${orphanedDomain.id}`, {
          method: "DELETE",
          headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` }
        })
        
        if (deleteResponse.ok) {
          console.log(`Successfully deleted orphaned domain: ${orphanedDomain.hostname}`)
        } else {
          const error = await deleteResponse.text()
          console.error(`Failed to delete orphaned domain ${orphanedDomain.hostname}:`, error)
        }
      } catch (error) {
        console.error(`Error deleting orphaned domain ${orphanedDomain.hostname}:`, error)
      }
    }
    
    // Delete orphaned workers
    for (const orphanedWorker of orphanedWorkers) {
      try {
        console.log(`Deleting orphaned worker: ${orphanedWorker.id}`)
        
        const deleteResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${orphanedWorker.id}`, {
          method: "DELETE",
          headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` }
        })
        
        if (deleteResponse.ok) {
          console.log(`Successfully deleted orphaned worker: ${orphanedWorker.id}`)
        } else {
          const error = await deleteResponse.text()
          console.error(`Failed to delete orphaned worker ${orphanedWorker.id}:`, error)
        }
      } catch (error) {
        console.error(`Error deleting orphaned worker ${orphanedWorker.id}:`, error)
      }
    }
    
    // Create missing workers in Cloudflare
    for (const { fullName, resource } of missingWorkers) {
      try {
        console.log(`Creating missing worker: ${fullName}`)
        
        const spec = JSON.parse(resource.spec)
        const customDomain = `${resource.name}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`
        
        // Get the worker script content
        let script: string
        if (spec.scriptUrl) {
          const scriptResponse = await fetch(spec.scriptUrl, {
            redirect: 'follow',
            headers: {
              'User-Agent': 'Guber-Worker-Provisioner/1.0'
            }
          })
          if (!scriptResponse.ok) {
            throw new Error(`Failed to fetch script from ${spec.scriptUrl}: ${scriptResponse.status} ${scriptResponse.statusText}`)
          }
          script = await scriptResponse.text()
        } else if (spec.script) {
          script = spec.script
        } else {
          throw new Error("Worker must have either 'script' or 'scriptUrl' specified")
        }
        
        // Create worker script
        // Create multipart form data for the worker upload
        const formData = new FormData()
        
        // Check for source map if scriptUrl is provided
        let sourceMap: string | null = null
        if (spec.scriptUrl) {
          try {
            const sourceMapUrl = spec.scriptUrl + '.map'
            const sourceMapResponse = await fetch(sourceMapUrl, {
              redirect: 'follow',
              headers: {
                'User-Agent': 'Guber-Worker-Provisioner/1.0'
              }
            })
            if (sourceMapResponse.ok) {
              sourceMap = await sourceMapResponse.text()
              console.log(`Found source map at ${sourceMapUrl}`)
            }
          } catch (error) {
            // Source map is optional, continue without it
            console.log(`No source map found for ${spec.scriptUrl}`)
          }
        }
        
        // Always use module format with main_module
        const metadata: any = {
          main_module: "index.js",
          compatibility_date: spec.compatibility_date || "2023-05-18"
        }
        
        // Add compatibility settings if specified
        if (spec.compatibility_date) {
          metadata.compatibility_date = spec.compatibility_date
        }
        if (spec.compatibility_flags) {
          metadata.compatibility_flags = spec.compatibility_flags
        }
        
        // Add bindings if specified
        if (spec.bindings) {
          console.log(`[Reconcile] Processing bindings for worker ${fullName}:`, JSON.stringify(spec.bindings, null, 2))
          const bindings: any[] = []
          
          // Handle D1 database bindings
          if (spec.bindings.d1_databases) {
            console.log(`[Reconcile] Processing ${spec.bindings.d1_databases.length} D1 database bindings`)
            for (const d1Binding of spec.bindings.d1_databases) {
              console.log(`[Reconcile] Looking up D1 binding: ${d1Binding.database_name} -> ${d1Binding.binding}`)
              
              // Look up the D1 resource to get its database_id
              const d1Resource = await env.DB.prepare(
                "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
              ).bind(d1Binding.database_name).first()
              
              console.log(`[Reconcile] D1 resource lookup result:`, d1Resource)
              
              if (d1Resource && d1Resource.status) {
                const status = JSON.parse(d1Resource.status)
                console.log(`[Reconcile] D1 resource status:`, status)
                if (status.database_id) {
                  const binding = {
                    type: "d1",
                    name: d1Binding.binding,
                    database_id: status.database_id
                  }
                  bindings.push(binding)
                  console.log(`[Reconcile] ✅ Added D1 binding:`, binding)
                } else {
                  console.log(`[Reconcile] ❌ D1 resource ${d1Binding.database_name} has no database_id in status`)
                }
              } else {
                console.log(`[Reconcile] ❌ D1 resource ${d1Binding.database_name} not found or has no status`)
              }
            }
          }
          
          // Handle Queue bindings
          if (spec.bindings.queues) {
            console.log(`[Reconcile] Processing ${spec.bindings.queues.length} Queue bindings`)
            for (const queueBinding of spec.bindings.queues) {
              console.log(`[Reconcile] Looking up Queue binding: ${queueBinding.queue_name} -> ${queueBinding.binding}`)
              
              // Look up the Queue resource to get its queue_id
              const queueResource = await env.DB.prepare(
                "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
              ).bind(queueBinding.queue_name).first()
              
              console.log(`[Reconcile] Queue resource lookup result:`, queueResource)
              
              if (queueResource && queueResource.status) {
                const status = JSON.parse(queueResource.status)
                console.log(`[Reconcile] Queue resource status:`, status)
                if (status.queue_id) {
                  const binding = {
                    type: "queue",
                    name: queueBinding.binding,
                    queue_name: status.queue_id
                  }
                  bindings.push(binding)
                  console.log(`[Reconcile] ✅ Added Queue binding:`, binding)
                } else {
                  console.log(`[Reconcile] ❌ Queue resource ${queueBinding.queue_name} has no queue_id in status`)
                }
              } else {
                console.log(`[Reconcile] ❌ Queue resource ${queueBinding.queue_name} not found or has no status`)
              }
            }
          }
          
          console.log(`[Reconcile] Total bindings collected: ${bindings.length}`)
          if (bindings.length > 0) {
            metadata.bindings = bindings
            console.log(`[Reconcile] ✅ Added bindings to metadata:`, JSON.stringify(bindings, null, 2))
          } else {
            console.log(`[Reconcile] ❌ No bindings to add to metadata`)
          }
        } else {
          console.log(`[Reconcile] No bindings specified for worker ${fullName}`)
        }
        
        formData.append('metadata', JSON.stringify(metadata))
        formData.append('index.js', new Blob([script], { type: 'application/javascript+module' }), 'index.js')
        
        // Add source map if available
        if (sourceMap) {
          formData.append('index.js.map', new Blob([sourceMap], { type: 'text/plain' }), 'index.js.map')
        }
        
        // Debug logging for reconciliation
        console.log(`=== Worker Reconciliation Debug Info ===`)
        console.log(`Worker Name: ${fullName}`)
        console.log(`Script URL: ${spec.scriptUrl || 'inline'}`)
        console.log(`Metadata: ${JSON.stringify(metadata, null, 2)}`)
        console.log(`Script Content (first 500 chars): ${script.substring(0, 500)}...`)
        console.log(`Script Content (last 200 chars): ...${script.substring(script.length - 200)}`)
        console.log(`Has Source Map: ${sourceMap ? 'yes' : 'no'}`)
        console.log(`=== End Reconciliation Debug Info ===`)
        
        const createResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`, {
          method: "PUT",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
          },
          body: formData
        })
        
        if (createResponse.ok) {
          console.log(`Successfully created missing worker script: ${fullName}`)
          
          // Create custom domain
          const domainResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`, {
            method: "PUT",
            headers: {
              "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({
              hostname: customDomain,
              service: fullName,
              environment: "production"
            })
          })
          
          let domainId = null
          if (domainResponse.ok) {
            const domainResult = await domainResponse.json()
            domainId = domainResult.result?.id
            console.log(`Successfully created custom domain: ${customDomain}`)
          } else {
            const domainError = await domainResponse.text()
            console.error(`Failed to create custom domain ${customDomain}:`, domainError)
          }
          
          // Update the resource status in our database
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const statusData = {
            state: domainId ? "Ready" : "PartiallyReady",
            worker_id: fullName,
            createdAt: new Date().toISOString(),
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`,
            customDomain: customDomain,
            url: `https://${customDomain}`,
            reconciledAt: new Date().toISOString()
          }
          
          if (domainId) {
            statusData.domainId = domainId
          }
          
          const updateParams = resource.namespace
            ? [JSON.stringify(statusData), resource.name, resource.namespace]
            : [JSON.stringify(statusData), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
          
          console.log(`Successfully reconciled missing worker: ${fullName}`)
        } else {
          const error = await createResponse.text()
          console.error(`Failed to create missing worker ${fullName}:`, error)
          
          // Update status to failed
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = resource.namespace
            ? [JSON.stringify({
                state: "Failed",
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name, resource.namespace]
            : [JSON.stringify({
                state: "Failed", 
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
        }
      } catch (error) {
        console.error(`Error creating missing worker ${fullName}:`, error)
      }
    }
    
    // Check existing workers for binding updates and health
    console.log("Starting worker binding checks and health checks...")
    for (const [fullName, apiResource] of apiWorkerMap) {
      if (cloudflareWorkerMap.has(fullName)) {
        try {
          const spec = JSON.parse(apiResource.spec)
          let status = {}
          try {
            status = apiResource.status ? JSON.parse(apiResource.status) : {}
          } catch (statusParseError) {
            console.error(`Failed to parse status for worker ${fullName}:`, statusParseError)
            console.error(`Status content:`, apiResource.status)
            status = {}
          }
          const customDomain = `${apiResource.name}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`
          
          // Check if bindings need to be updated
          let needsBindingUpdate = false
          const expectedBindings: any[] = []
          
          if (spec.bindings) {
            console.log(`[Reconcile] Checking bindings for existing worker ${fullName}`)
            
            // Build expected bindings from spec
            if (spec.bindings.d1_databases) {
              for (const d1Binding of spec.bindings.d1_databases) {
                const d1Resource = await env.DB.prepare(
                  "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
                ).bind(d1Binding.database_name).first()
                
                if (d1Resource && d1Resource.status) {
                  const d1Status = JSON.parse(d1Resource.status)
                  if (d1Status.database_id) {
                    expectedBindings.push({
                      type: "d1",
                      name: d1Binding.binding,
                      database_id: d1Status.database_id
                    })
                  }
                }
              }
            }
            
            if (spec.bindings.queues) {
              for (const queueBinding of spec.bindings.queues) {
                const queueResource = await env.DB.prepare(
                  "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL"
                ).bind(queueBinding.queue_name).first()
                
                if (queueResource && queueResource.status) {
                  const queueStatus = JSON.parse(queueResource.status)
                  if (queueStatus.queue_id) {
                    expectedBindings.push({
                      type: "queue",
                      name: queueBinding.binding,
                      queue_name: queueStatus.queue_id
                    })
                  }
                }
              }
            }
            
            // Get current worker metadata to check existing bindings
            console.log(`[Reconcile] Fetching worker metadata for ${fullName}`)
            console.log(`[Reconcile] Request URL: https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}/settings`)
            
            const workerResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}/settings`, {
              method: "GET",
              headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` }
            })
            
            console.log(`[Reconcile] Worker metadata response status: ${workerResponse.status} ${workerResponse.statusText}`)
            console.log(`[Reconcile] Worker metadata response headers:`, Object.fromEntries(workerResponse.headers.entries()))
            
            if (workerResponse.ok) {
              let workerData
              try {
                const responseText = await workerResponse.text()
                console.log(`[Reconcile] Worker metadata response body:`, responseText)
                
                workerData = JSON.parse(responseText)
                console.log(`[Reconcile] Parsed worker metadata:`, JSON.stringify(workerData, null, 2))
              } catch (parseError) {
                console.error(`[Reconcile] Failed to parse worker metadata response:`, parseError)
                console.log(`[Reconcile] Could not parse worker metadata for ${fullName}, skipping binding check`)
                continue
              }
              
              const currentBindings = workerData.result?.bindings || []
              console.log(`[Reconcile] Current bindings for ${fullName}:`, JSON.stringify(currentBindings, null, 2))
              console.log(`[Reconcile] Expected bindings for ${fullName}:`, JSON.stringify(expectedBindings, null, 2))
              
              // Compare expected vs current bindings
              if (expectedBindings.length !== currentBindings.length) {
                needsBindingUpdate = true
                console.log(`[Reconcile] Binding count mismatch for ${fullName}: expected ${expectedBindings.length}, current ${currentBindings.length}`)
              } else {
                // Check if bindings match
                for (const expectedBinding of expectedBindings) {
                  const matchingBinding = currentBindings.find((cb: any) => 
                    cb.name === expectedBinding.name && 
                    cb.type === expectedBinding.type &&
                    (expectedBinding.database_id ? cb.database_id === expectedBinding.database_id : true) &&
                    (expectedBinding.queue_name ? cb.queue_name === expectedBinding.queue_name : true)
                  )
                  
                  if (!matchingBinding) {
                    needsBindingUpdate = true
                    console.log(`[Reconcile] Missing or mismatched binding for ${fullName}:`, expectedBinding)
                    console.log(`[Reconcile] Available bindings:`, currentBindings)
                    break
                  }
                }
              }
              
              if (!needsBindingUpdate) {
                console.log(`[Reconcile] Bindings are up to date for ${fullName}`)
              }
            } else {
              const errorText = await workerResponse.text()
              console.log(`[Reconcile] Could not fetch current worker metadata for ${fullName}`)
              console.log(`[Reconcile] Error response:`, errorText)
            }
          }
          
          // Update worker if bindings don't match
          if (needsBindingUpdate) {
            console.log(`[Reconcile] Updating bindings for worker ${fullName}`)
            
            // Get the worker script content
            let script: string
            if (spec.scriptUrl) {
              const scriptResponse = await fetch(spec.scriptUrl, {
                redirect: 'follow',
                headers: { 'User-Agent': 'Guber-Worker-Provisioner/1.0' }
              })
              if (scriptResponse.ok) {
                script = await scriptResponse.text()
              } else {
                console.error(`[Reconcile] Failed to fetch script for ${fullName} from ${spec.scriptUrl}`)
                continue
              }
            } else if (spec.script) {
              script = spec.script
            } else {
              console.error(`[Reconcile] No script source for worker ${fullName}`)
              continue
            }
            
            // Create updated worker deployment
            const formData = new FormData()
            
            const metadata: any = {
              main_module: "index.js",
              compatibility_date: spec.compatibility_date || "2023-05-18"
            }
            
            if (spec.compatibility_date) {
              metadata.compatibility_date = spec.compatibility_date
            }
            if (spec.compatibility_flags) {
              metadata.compatibility_flags = spec.compatibility_flags
            }
            
            if (expectedBindings.length > 0) {
              metadata.bindings = expectedBindings
              console.log(`[Reconcile] Adding updated bindings to ${fullName}:`, JSON.stringify(expectedBindings, null, 2))
            }
            
            formData.append('metadata', JSON.stringify(metadata))
            formData.append('index.js', new Blob([script], { type: 'application/javascript+module' }), 'index.js')
            
            const updateResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`, {
              method: "PUT",
              headers: { "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
              body: formData
            })
            
            if (updateResponse.ok) {
              console.log(`[Reconcile] Successfully updated bindings for worker ${fullName}`)
              
              // Update status to reflect binding update
              const newStatus = {
                ...status,
                lastBindingUpdate: new Date().toISOString(),
                bindingsUpdated: true
              }
              
              const updateQuery = apiResource.namespace 
                ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
                : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
              
              const updateParams = apiResource.namespace
                ? [JSON.stringify(newStatus), apiResource.name, apiResource.namespace]
                : [JSON.stringify(newStatus), apiResource.name]
              
              await env.DB.prepare(updateQuery).bind(...updateParams).run()
            } else {
              const error = await updateResponse.text()
              console.error(`[Reconcile] Failed to update bindings for worker ${fullName}:`, error)
            }
          }
          
          // Test the worker endpoint for health check
          const healthResponse = await fetch(`https://${customDomain}`, {
            method: 'GET',
            headers: {
              'User-Agent': 'Guber-Health-Check/1.0'
            }
          })
          
          const isHealthy = healthResponse.ok
          const currentState = status.state
          
          // Update status if health state changed
          if ((isHealthy && currentState === "Failed") || (!isHealthy && currentState === "Ready")) {
            const newStatus = {
              ...status,
              state: isHealthy ? "Ready" : "Failed",
              lastHealthCheck: new Date().toISOString(),
              healthCheckStatus: healthResponse.status,
              healthCheckError: isHealthy ? undefined : `HTTP ${healthResponse.status}: ${healthResponse.statusText}`
            }
            
            if (!isHealthy) {
              try {
                const errorText = await healthResponse.text()
                if (errorText) {
                  newStatus.healthCheckError = `HTTP ${healthResponse.status}: ${errorText.substring(0, 500)}`
                }
              } catch (e) {
                newStatus.healthCheckError = `HTTP ${healthResponse.status}: Failed to read response body - ${e.message}`
              }
            }
            
            const updateQuery = apiResource.namespace 
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
            
            const updateParams = apiResource.namespace
              ? [JSON.stringify(newStatus), apiResource.name, apiResource.namespace]
              : [JSON.stringify(newStatus), apiResource.name]
            
            await env.DB.prepare(updateQuery).bind(...updateParams).run()
            
            console.log(`Updated worker ${fullName} health status: ${currentState} -> ${newStatus.state}`)
          }
        } catch (error) {
          console.error(`Error checking worker ${fullName}:`, error)
          
          // Update status to indicate check failed
          let status = {}
          try {
            status = apiResource.status ? JSON.parse(apiResource.status) : {}
          } catch (parseError) {
            console.error(`Failed to parse existing status for worker ${fullName}:`, parseError)
            status = {}
          }
          
          const newStatus = {
            ...status,
            state: "Failed",
            lastHealthCheck: new Date().toISOString(),
            healthCheckError: `Worker check failed: ${error.message || String(error)}`
          }
          
          const updateQuery = apiResource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = apiResource.namespace
            ? [JSON.stringify(newStatus), apiResource.name, apiResource.namespace]
            : [JSON.stringify(newStatus), apiResource.name]
          
          try {
            await env.DB.prepare(updateQuery).bind(...updateParams).run()
          } catch (dbError) {
            console.error(`Failed to update status for worker ${fullName}:`, dbError)
          }
        }
      }
    }
    
    console.log("Worker reconciliation completed")
  } catch (error) {
    console.error("Error during Worker reconciliation:", error)
  }
}

async function reconcileD1Databases(env: Env) {
  try {
    console.log("Starting D1 database reconciliation...")
    
    // Get all D1 resources from our API
    const { results: apiResources } = await env.DB.prepare(
      "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='D1'"
    ).all()
    
    // Get all D1 databases from Cloudflare
    const cloudflareResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
      }
    })
    
    if (!cloudflareResponse.ok) {
      console.error("Failed to fetch D1 databases from Cloudflare:", await cloudflareResponse.text())
      return
    }
    
    const cloudflareResult = await cloudflareResponse.json()
    const cloudflareDatabases = cloudflareResult.result || []
    
    // Create maps for easier comparison
    const apiDatabaseMap = new Map()
    const cloudflareDatabaseMap = new Map()
    
    // Build API database map with full names
    for (const resource of (apiResources || [])) {
      const fullDatabaseName = buildFullDatabaseName(resource.name, resource.group_name, resource.plural, resource.namespace, env.GUBER_NAME)
      apiDatabaseMap.set(fullDatabaseName, resource)
    }
    
    // Build Cloudflare database map
    for (const db of cloudflareDatabases) {
      cloudflareDatabaseMap.set(db.name, db)
    }
    
    console.log(`Found ${apiDatabaseMap.size} D1 resources in API and ${cloudflareDatabaseMap.size} databases in Cloudflare`)
    
    // Find databases that exist in Cloudflare but not in our API (orphaned databases)
    const orphanedDatabases = []
    for (const [dbName, cloudflareDb] of cloudflareDatabaseMap) {
      // Only consider databases that match our naming pattern
      if (dbName.includes('-') && (dbName.includes('-d1s-cf-guber-proc-io') || dbName.includes('-d1-cf-guber-proc-io'))) {
        if (!apiDatabaseMap.has(dbName)) {
          orphanedDatabases.push(cloudflareDb)
        }
      }
    }
    
    // Find resources that exist in our API but not in Cloudflare (missing databases)
    const missingDatabases = []
    for (const [fullName, apiResource] of apiDatabaseMap) {
      if (!cloudflareDatabaseMap.has(fullName)) {
        missingDatabases.push({ fullName, resource: apiResource })
      }
    }
    
    console.log(`Found ${orphanedDatabases.length} orphaned databases and ${missingDatabases.length} missing databases`)
    
    // Delete orphaned databases from Cloudflare
    for (const orphanedDb of orphanedDatabases) {
      try {
        console.log(`Deleting orphaned database: ${orphanedDb.name} (ID: ${orphanedDb.uuid})`)
        
        const deleteResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${orphanedDb.uuid}`, {
          method: "DELETE",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`
          }
        })
        
        if (deleteResponse.ok) {
          console.log(`Successfully deleted orphaned database: ${orphanedDb.name}`)
        } else {
          const error = await deleteResponse.text()
          console.error(`Failed to delete orphaned database ${orphanedDb.name}:`, error)
        }
      } catch (error) {
        console.error(`Error deleting orphaned database ${orphanedDb.name}:`, error)
      }
    }
    
    // Create missing databases in Cloudflare
    for (const { fullName, resource } of missingDatabases) {
      try {
        console.log(`Creating missing database: ${fullName}`)
        
        const spec = JSON.parse(resource.spec)
        const requestBody: any = { name: fullName }
        
        if (spec.location) {
          requestBody.primary_location_hint = spec.location
        }
        
        const createResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(requestBody)
        })
        
        if (createResponse.ok) {
          const result = await createResponse.json()
          const databaseId = result.result.uuid
          
          // Update the resource status in our database
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = resource.namespace
            ? [JSON.stringify({
                state: "Ready",
                database_id: databaseId,
                createdAt: new Date().toISOString(),
                endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
                reconciledAt: new Date().toISOString()
              }), resource.name, resource.namespace]
            : [JSON.stringify({
                state: "Ready",
                database_id: databaseId,
                createdAt: new Date().toISOString(),
                endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
                reconciledAt: new Date().toISOString()
              }), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
          
          console.log(`Successfully created missing database: ${fullName} with ID: ${databaseId}`)
        } else {
          const error = await createResponse.text()
          console.error(`Failed to create missing database ${fullName}:`, error)
          
          // Update status to failed
          const updateQuery = resource.namespace 
            ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
            : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL"
          
          const updateParams = resource.namespace
            ? [JSON.stringify({
                state: "Failed",
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name, resource.namespace]
            : [JSON.stringify({
                state: "Failed", 
                error: error,
                reconciledAt: new Date().toISOString()
              }), resource.name]
          
          await env.DB.prepare(updateQuery).bind(...updateParams).run()
        }
      } catch (error) {
        console.error(`Error creating missing database ${fullName}:`, error)
      }
    }
    
    console.log("D1 database reconciliation completed")
  } catch (error) {
    console.error("Error during D1 database reconciliation:", error)
  }
}

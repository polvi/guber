import { Hono } from "hono"
import { v4 as uuid } from "uuid"

type Env = { 
  Bindings: { 
    DB: D1Database
    D1_QUEUE: Queue
    CLOUDFLARE_API_TOKEN: string
    CLOUDFLARE_ACCOUNT_ID: string
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

  // If this is a D1 resource, queue it for provisioning
  if (group === "cloudflare.guber.proc.io" && crd.kind === "D1" && c.env.D1_QUEUE) {
    await c.env.D1_QUEUE.send({
      action: "create",
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

  // If this is a D1 resource, queue it for deletion BEFORE deleting from DB
  if (group === "cloudflare.guber.proc.io" && result.kind === "D1" && c.env.D1_QUEUE) {
    const spec = JSON.parse(result.spec)
    const status = result.status ? JSON.parse(result.status) : {}
    await c.env.D1_QUEUE.send({
      action: "delete",
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
        const { action, resourceName, group, kind, plural, namespace, spec, status } = message.body
        
        if (action === "create") {
          await provisionD1Database(env, resourceName, group, kind, plural, namespace, spec)
        } else if (action === "delete") {
          await deleteD1Database(env, resourceName, group, kind, plural, namespace, spec, status)
        }
        
        message.ack()
      } catch (error) {
        console.error(`Failed to process queue message:`, error)
        message.retry()
      }
    }
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    console.log(`Running D1 reconciliation at ${new Date(event.scheduledTime).toISOString()}`)
    await reconcileD1Databases(env)
  }
}

function buildFullDatabaseName(resourceName: string, group: string, plural: string, namespace: string | null): string {
  // Construct full database name: name.namespace.resource-type
  const namespaceStr = namespace || "cluster"
  const resourceType = `${plural}.${group}`
  return `${resourceName}.${namespaceStr}.${resourceType}`
}

async function provisionD1Database(env: Env, resourceName: string, group: string, kind: string, plural: string, namespace: string | null, spec: any) {
  const fullDatabaseName = buildFullDatabaseName(resourceName, group, plural, namespace)
  
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
  const fullDatabaseName = buildFullDatabaseName(resourceName, group, plural, namespace)
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

async function reconcileD1Databases(env: Env) {
  try {
    console.log("Starting D1 database reconciliation...")
    
    // Get all D1 resources from our API
    const { results: apiResources } = await env.DB.prepare(
      "SELECT * FROM resources WHERE group_name='cloudflare.guber.proc.io' AND kind='D1'"
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
      const fullDatabaseName = buildFullDatabaseName(resource.name, resource.group_name, resource.plural, resource.namespace)
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
      if (dbName.includes('.') && (dbName.includes('.d1s.cloudflare.guber.proc.io') || dbName.includes('.d1.cloudflare.guber.proc.io'))) {
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

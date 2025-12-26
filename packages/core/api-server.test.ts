import { expect, test, describe, beforeEach } from "bun:test";
import { ApiServer, Resource, CustomResourceDefinition } from "./index";

describe("ApiServer (TLA+ Resource Lifecycle)", () => {
  let api: ApiServer;

  const workerCRD: CustomResourceDefinition = {
    name: "workers.cloudflare.guber.dev",
    spec: {
      group: "cloudflare.guber.dev",
      names: { kind: "Worker", plural: "workers" },
      versions: [{ 
        name: "v1",
        schema: {
          openAPIV3Schema: {
            type: "object",
            required: ["script"],
            properties: {
              script: { type: "string" }
            }
          }
        }
      }]
    }
  };

  beforeEach(() => {
    api = new ApiServer();
    api.createCRD(workerCRD);
  });

  const baseResource: Resource = {
    kind: "Worker",
    apiVersion: "cloudflare.guber.dev/v1",
    metadata: { name: "my-worker", generation: 0, resourceVersion: "0" },
    spec: { script: "console.log('hi')" },
  };

  test("CreateResource fails if CRD does not exist", async () => {
    const unknownResource = { ...baseResource, kind: "Unknown" };
    try {
      await api.create(unknownResource);
      expect(true).toBe(false); // Should not reach here
    } catch (e: any) {
      expect(e.message).toContain("No CRD registered");
    }
  });

  test("CreateResource fails if schema validation fails", async () => {
    const invalidResource = { ...baseResource, spec: {} };
    try {
      await api.create(invalidResource);
      expect(true).toBe(false);
    } catch (e: any) {
      expect(e.message).toContain("missing required field 'script'");
    }
  });

  test("CreateResource sets initial state and finalizers", async () => {
    const created = await api.create(baseResource);
    expect(created.metadata.resourceVersion).toBe("1");
    expect(created.metadata.generation).toBe(1);
    expect(created.metadata.finalizers).toContain("guber-controller");
    expect(created.status?.phase).toBe("Initial");
  });

  test("UpdateResource increments generation and version", async () => {
    const created = await api.create(baseResource);
    const updated = await api.update({
      ...created,
      spec: { script: "updated" },
    });

    expect(updated.metadata.resourceVersion).toBe("2");
    expect(updated.metadata.generation).toBe(2);
  });

  test("patchStatus increments version but NOT generation", async () => {
    const created = await api.create(baseResource);
    const patched = api.patchStatus({
      ...created,
      status: { ...created.status, phase: "Ready" }
    });

    expect(patched.metadata.resourceVersion).toBe("2");
    expect(patched.metadata.generation).toBe(1);
    expect(patched.status?.phase).toBe("Ready");
  });

  test("UpdateResource fails if schema validation fails", async () => {
    const created = await api.create(baseResource);
    try {
      await api.update({ ...created, spec: {} });
      expect(true).toBe(false);
    } catch (e: any) {
      expect(e.message).toContain("missing required field 'script'");
    }
  });

  test("UpdateResource fails on version conflict", async () => {
    const created = await api.create(baseResource);
    try {
      await api.update({ ...created, metadata: { ...created.metadata, resourceVersion: "0" } });
      expect(true).toBe(false);
    } catch (e: any) {
      expect(e.message).toContain("Conflict");
    }
  });

  test("RequestDeleteResource sets deletionTimestamp", async () => {
    const created = await api.create(baseResource);
    const deleted = api.delete("Worker", "my-worker", created);
    
    expect(deleted.metadata.deletionTimestamp).toBeDefined();
    expect(deleted.metadata.resourceVersion).toBe("2");
  });

  test("ObserveGarbageCollection removes resource when finalizers are gone", async () => {
    const created = await api.create(baseResource);
    const deleting = api.delete("Worker", "my-worker", created);
    
    // Simulate controller removing finalizer
    const finalized = {
      ...deleting,
      metadata: { ...deleting.metadata, finalizers: [] }
    };
    
    // Explicitly trigger garbage collection as per TLA+ ObserveGarbageCollection
    const collected = api.collectGarbage(finalized);
    expect(collected).toBe(true);
  });

  test("DeleteCRD performs cascading deletion of resources", async () => {
    await api.create(baseResource);
    expect(api.get("Worker", "my-worker")).toBeDefined();

    api.deleteCRD(workerCRD.name);
    
    // Resource should be gone because its CRD was deleted
    expect(api.get("Worker", "my-worker")).toBeUndefined();
  });

  test("DeleteNamespace performs cascading deletion of resources", async () => {
    api.createNamespace("prod");
    const prodResource = {
      ...baseResource,
      metadata: { ...baseResource.metadata, name: "prod-worker", namespace: "prod" }
    };
    await api.create(prodResource);
    
    expect(api.get("Worker", "prod-worker", "prod")).toBeDefined();
    
    api.deleteNamespace("prod");
    expect(api.get("Worker", "prod-worker", "prod")).toBeUndefined();
  });
});

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

  test("CreateResource fails if CRD does not exist", () => {
    const unknownResource = { ...baseResource, kind: "Unknown" };
    expect(() => api.create(unknownResource)).toThrow("No CRD registered");
  });

  test("CreateResource fails if schema validation fails", () => {
    const invalidResource = { ...baseResource, spec: {} };
    expect(() => api.create(invalidResource)).toThrow("missing required field 'script'");
  });

  test("CreateResource sets initial state and finalizers", () => {
    const created = api.create(baseResource);
    expect(created.metadata.resourceVersion).toBe("1");
    expect(created.metadata.generation).toBe(1);
    expect(created.metadata.finalizers).toContain("guber-controller");
    expect(created.status?.phase).toBe("Initial");
  });

  test("UpdateResource increments generation and version", () => {
    const created = api.create(baseResource);
    const updated = api.update({
      ...created,
      spec: { script: "updated" },
    });

    expect(updated.metadata.resourceVersion).toBe("2");
    expect(updated.metadata.generation).toBe(2);
  });

  test("patchStatus increments version but NOT generation", () => {
    const created = api.create(baseResource);
    const patched = api.patchStatus({
      ...created,
      status: { ...created.status, phase: "Ready" }
    });

    expect(patched.metadata.resourceVersion).toBe("2");
    expect(patched.metadata.generation).toBe(1);
    expect(patched.status?.phase).toBe("Ready");
  });

  test("UpdateResource fails if schema validation fails", () => {
    const created = api.create(baseResource);
    expect(() => api.update({ ...created, spec: {} })).toThrow("missing required field 'script'");
  });

  test("UpdateResource fails on version conflict", () => {
    const created = api.create(baseResource);
    expect(() => api.update({ ...created, metadata: { ...created.metadata, resourceVersion: "0" } }))
      .toThrow("Conflict");
  });

  test("RequestDeleteResource sets deletionTimestamp", () => {
    api.create(baseResource);
    api.delete("Worker", "my-worker");
    
    const resource = api.get("Worker", "my-worker");
    expect(resource?.metadata.deletionTimestamp).toBeDefined();
    expect(resource?.metadata.resourceVersion).toBe("2");
  });

  test("ObserveGarbageCollection removes resource when finalizers are gone", () => {
    const created = api.create(baseResource);
    api.delete("Worker", "my-worker");
    
    // Simulate controller removing finalizer
    const deleting = api.get("Worker", "my-worker")!;
    const finalized = {
      ...deleting,
      metadata: { ...deleting.metadata, finalizers: [] }
    };
    api.update(finalized); 
    
    const result = api.get("Worker", "my-worker");
    expect(result).toBeUndefined();
  });

  test("DeleteCRD performs cascading deletion of resources", () => {
    api.create(baseResource);
    expect(api.get("Worker", "my-worker")).toBeDefined();

    api.deleteCRD(workerCRD.name);
    
    // Resource should be gone because its CRD was deleted
    expect(api.get("Worker", "my-worker")).toBeUndefined();
  });
});

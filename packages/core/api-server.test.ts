import { expect, test, describe, beforeEach } from "bun:test";
import { ApiServer, Resource } from "./index";

describe("ApiServer (TLA+ Resource Lifecycle)", () => {
  let api: ApiServer;

  beforeEach(() => {
    api = new ApiServer();
  });

  const baseResource: Resource = {
    kind: "Worker",
    apiVersion: "cloudflare.guber.dev/v1",
    metadata: { name: "my-worker", generation: 0, resourceVersion: "0" },
    spec: { script: "console.log('hi')" },
  };

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
    // We use a direct set or update for this simulation
    // In the spec, ObserveGarbageCollection is an API server internal check
    api.update(finalized); 
    
    const result = api.get("Worker", "my-worker");
    expect(result).toBeUndefined();
  });
});

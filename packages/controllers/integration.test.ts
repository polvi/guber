import { expect, test, describe, beforeEach } from "bun:test";
import { ApiServer, CustomResourceDefinition, Resource } from "../core/index";
import { Controller } from "./controller";

describe("Guber System Integration (TLA+ Full Lifecycle)", () => {
  let api: ApiServer;
  let controller: Controller;

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
            properties: { script: { type: "string" } }
          }
        }
      }]
    }
  };

  beforeEach(() => {
    api = new ApiServer();
    api.createCRD(workerCRD);
    controller = new Controller(api);
  });

  test("Full Lifecycle: Create -> Reconcile -> Update -> Reconcile -> Delete -> GC", async () => {
    const name = "test-worker";
    const kind = "Worker";

    // 1. CreateResource
    const initial: Resource = {
      kind,
      apiVersion: "cloudflare.guber.dev/v1",
      metadata: { name, generation: 0, resourceVersion: "0" },
      spec: { script: "console.log('v1')" },
    };
    await api.create(initial);
    
    let r = api.get(kind, name)!;
    expect(r.status?.observedGeneration).toBe(0);

    // 2. ReconcileResource
    await controller.runIteration(kind, name);
    r = api.get(kind, name)!;
    expect(r.status?.observedGeneration).toBe(1);
    expect(r.status?.phase).toBe("Ready");

    // 3. UpdateResource (Spec change)
    const updated = { ...r, spec: { script: "console.log('v2')" } };
    await api.update(updated);
    r = api.get(kind, name)!;
    expect(r.metadata.generation).toBe(2);
    expect(r.status?.observedGeneration).toBe(1); // Out of sync

    // 4. ReconcileResource (Again)
    await controller.runIteration(kind, name);
    r = api.get(kind, name)!;
    expect(r.status?.observedGeneration).toBe(2);

    // 5. RequestDeleteResource
    // api.delete returns the object with deletionTimestamp set and version incremented.
    // To simulate the API server behavior where the client sends a DELETE request:
    const deleting = api.delete(kind, name, r);
    
    // We manually update the internal state to reflect the deletion request
    // In the real API gateway, this is handled by the DELETE route logic.
    // We use a direct map set here to bypass the version check of 'update' 
    // because 'deleting' already has the incremented version.
    (api as any).resources.set((api as any).getResourceKey(kind, name, r.metadata.namespace), deleting);

    r = api.get(kind, name)!;
    expect(r.metadata.deletionTimestamp).toBeDefined();
    expect(r.metadata.finalizers).toContain("guber-controller");

    // 6. FinalizeResource
    await controller.runIteration(kind, name);
    r = api.get(kind, name)!;
    expect(r.metadata.finalizers).not.toContain("guber-controller");

    // 7. ObserveGarbageCollection
    // In the new controller, runIteration also handles GC if finalizers are gone
    await controller.runIteration(kind, name);
    
    const finalLookup = api.get(kind, name);
    expect(finalLookup).toBeUndefined();
  });

  test("Concurrency Conflict: Controller should retry on conflict", async () => {
    const name = "conflict-worker";
    const kind = "Worker";

    await api.create({
      kind,
      apiVersion: "cloudflare.guber.dev/v1",
      metadata: { name, generation: 0, resourceVersion: "0" },
      spec: { script: "v1" },
    });

    // We want to simulate a conflict. 
    // We'll wrap the api.patchStatus to fail once.
    const originalPatch = api.patchStatus.bind(api);
    let failedOnce = false;
    
    api.patchStatus = (resource: Resource) => {
      if (!failedOnce) {
        failedOnce = true;
        // Simulate someone else updated the resource version in the background
        const current = api.get(kind, name)!;
        api.update({ ...current, metadata: { ...current.metadata, annotations: { "touched": "true" } } });
        // Now this call should throw conflict because the version in 'resource' is stale
        return originalPatch(resource);
      }
      return originalPatch(resource);
    };

    await controller.runIteration(kind, name);
    
    const final = api.get(kind, name)!;
    expect(final.status?.observedGeneration).toBe(1);
    expect(failedOnce).toBe(true);
  });
});

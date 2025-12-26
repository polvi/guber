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
    // 1. CreateResource
    const initial: Resource = {
      kind: "Worker",
      apiVersion: "cloudflare.guber.dev/v1",
      metadata: { name: "test-worker", generation: 0, resourceVersion: "0" },
      spec: { script: "console.log('v1')" },
    };
    api.create(initial);
    
    let r = api.get("Worker", "test-worker")!;
    expect(r.status?.observedGeneration).toBe(0);

    // 2. ReconcileResource
    await controller.runIteration("Worker", "test-worker");
    r = api.get("Worker", "test-worker")!;
    expect(r.status?.observedGeneration).toBe(1);
    expect(r.status?.phase).toBe("Ready");

    // 3. UpdateResource (Spec change)
    const updated = { ...r, spec: { script: "console.log('v2')" } };
    api.update(updated);
    r = api.get("Worker", "test-worker")!;
    expect(r.metadata.generation).toBe(2);
    expect(r.status?.observedGeneration).toBe(1); // Out of sync

    // 4. ReconcileResource (Again)
    await controller.runIteration("Worker", "test-worker");
    r = api.get("Worker", "test-worker")!;
    expect(r.status?.observedGeneration).toBe(2);

    // 5. RequestDeleteResource
    api.delete("Worker", "test-worker");
    r = api.get("Worker", "test-worker")!;
    expect(r.metadata.deletionTimestamp).toBeDefined();
    expect(r.metadata.finalizers).toContain("guber-controller");

    // 6. FinalizeResource
    await controller.runIteration("Worker", "test-worker");
    r = api.get("Worker", "test-worker")!;
    expect(r.metadata.finalizers).not.toContain("guber-controller");

    // 7. ObserveGarbageCollection
    const collected = api.collectGarbage("Worker", "test-worker");
    expect(collected).toBe(true);
    
    const finalLookup = api.get("Worker", "test-worker");
    expect(finalLookup).toBeUndefined();
  });
});

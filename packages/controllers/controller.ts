import { ApiServer, Resource } from "@guber/core";
import { Reconciler } from "./reconciler";

export class Controller {
  constructor(private apiServer: ApiServer) {}

  /**
   * Processes a single resource through the reconciliation or finalization loop.
   * This maps to the individual actions in the TLA+ spec.
   * Implements retries to handle optimistic concurrency conflicts.
   */
  async processResource(kind: string, name: string, namespace?: string): Promise<void> {
    const maxRetries = 3;
    let attempt = 0;

    while (attempt < maxRetries) {
      const resource = this.apiServer.get(kind, name, namespace);
      if (!resource) return;

      try {
        // 1. Handle Garbage Collection (ObserveGarbageCollection in TLA+)
        if (resource.metadata.deletionTimestamp && (!resource.metadata.finalizers || resource.metadata.finalizers.length === 0)) {
          this.apiServer.collectGarbage(resource);
          return;
        }

        // 2. Handle Finalization (FinalizeResource in TLA+)
        if (Reconciler.shouldFinalize(resource)) {
          const finalized = Reconciler.finalize(resource);
          await this.apiServer.update(finalized);
          return;
        }

        // 3. Handle Reconciliation (ReconcileResource in TLA+)
        if (Reconciler.shouldReconcile(resource)) {
          const reconciled = Reconciler.reconcile(resource);
          this.apiServer.patchStatus(reconciled);
          return;
        }
        
        // If no action was needed, exit loop
        return;
      } catch (error: any) {
        if (error.message.includes("Conflict")) {
          attempt++;
          continue;
        }
        throw error;
      }
    }
  }

  /**
   * Simulates a full control loop iteration for a specific kind.
   */
  async reconcileAll(kind: string, namespace?: string): Promise<void> {
    const resources = this.apiServer.list(kind, namespace);
    for (const resource of resources) {
      await this.processResource(kind, resource.metadata.name, resource.metadata.namespace);
    }
  }

  /**
   * Simulates one "tick" of the controller loop for a specific resource.
   */
  async runIteration(kind: string, name: string, namespace?: string): Promise<void> {
    await this.processResource(kind, name, namespace);
  }
}

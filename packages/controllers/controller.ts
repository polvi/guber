import { ApiServer, Resource } from "@guber/core";
import { Reconciler } from "./reconciler";

export class Controller {
  constructor(private apiServer: ApiServer) {}

  /**
   * Processes a single resource through the reconciliation or finalization loop.
   * This maps to the individual actions in the TLA+ spec.
   */
  async processResource(kind: string, name: string): Promise<void> {
    const resource = this.apiServer.get(kind, name);
    if (!resource) return;

    // 1. Handle Garbage Collection (ObserveGarbageCollection in TLA+)
    // If the resource is ready to be deleted, we do it.
    if (resource.metadata.deletionTimestamp && (!resource.metadata.finalizers || resource.metadata.finalizers.length === 0)) {
      this.apiServer.collectGarbage(kind, name);
      return;
    }

    // 2. Handle Finalization (FinalizeResource in TLA+)
    if (Reconciler.shouldFinalize(resource)) {
      const finalized = Reconciler.finalize(resource);
      this.apiServer.update(finalized);
      return;
    }

    // 3. Handle Reconciliation (ReconcileResource in TLA+)
    if (Reconciler.shouldReconcile(resource)) {
      const reconciled = Reconciler.reconcile(resource);
      this.apiServer.patchStatus(reconciled);
      return;
    }
  }

  /**
   * Simulates a full control loop iteration for a specific kind.
   * In a real system, this would be triggered by a Watch event or a periodic resync.
   */
  async reconcileAll(kind: string): Promise<void> {
    const resources = this.apiServer.list(kind);
    for (const resource of resources) {
      await this.processResource(kind, resource.metadata.name);
    }
  }

  /**
   * Simulates one "tick" of the controller loop for a specific resource.
   * In TLA+, this corresponds to the non-deterministic firing of 
   * ReconcileResource or FinalizeResource for any resource 'r'.
   */
  async runIteration(kind: string, name: string): Promise<void> {
    await this.processResource(kind, name);
  }
}

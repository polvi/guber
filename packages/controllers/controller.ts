import { ApiServer, Resource } from "@guber/core";
import { Reconciler } from "./reconciler";

export class Controller {
  constructor(private apiServer: ApiServer) {}

  /**
   * Simulates one "tick" of the controller loop.
   * In TLA+, this corresponds to the non-deterministic firing of 
   * ReconcileResource or FinalizeResource for any resource 'r'.
   */
  async runIteration(kind: string, name: string): Promise<void> {
    const resource = this.apiServer.get(kind, name);
    if (!resource) return;

    // 1. Handle Finalization (FinalizeResource in TLA+)
    if (Reconciler.shouldFinalize(resource)) {
      const finalized = Reconciler.finalize(resource);
      this.apiServer.update(finalized);
      return;
    }

    // 2. Handle Reconciliation (ReconcileResource in TLA+)
    if (Reconciler.shouldReconcile(resource)) {
      const reconciled = Reconciler.reconcile(resource);
      this.apiServer.patchStatus(reconciled);
      return;
    }
  }
}

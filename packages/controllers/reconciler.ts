import { Resource } from "@guber/core";

export class Reconciler {
  /**
   * Implements ReconcileResource(r) from TLA+ spec:
   * /\ resourceObservedGen[r] < resourceGeneration[r]
   * /\ resourceDeletionTimestamp[r] = FALSE
   * /\ resourceStatus' = [ resourceStatus EXCEPT ![r] = "Ready" ]
   * /\ resourceObservedGen' = [ resourceObservedGen EXCEPT ![r] = resourceGeneration[r] ]
   */
  static shouldReconcile(resource: Resource): boolean {
    const isDeleted = !!resource.metadata.deletionTimestamp;
    const observedGen = resource.status?.observedGeneration ?? 0;
    const currentGen = resource.metadata.generation;

    return !isDeleted && observedGen < currentGen;
  }

  static reconcile<T extends Resource>(resource: T): T {
    if (!this.shouldReconcile(resource)) {
      return resource;
    }

    return {
      ...resource,
      status: {
        ...resource.status,
        observedGeneration: resource.metadata.generation,
        phase: "Ready", // Mapping the abstract "Ready" state from TLA+
      },
    };
  }
}

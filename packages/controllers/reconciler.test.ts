import { expect, test, describe } from "bun:test";
import { Reconciler } from "./reconciler";
import { Resource } from "@guber/core";

describe("Reconciler Logic (TLA+ ReconcileResource)", () => {
  const mockResource: Resource = {
    kind: "TestResource",
    apiVersion: "guber.dev/v1",
    metadata: {
      name: "test-1",
      generation: 2,
      resourceVersion: "100",
    },
    spec: { foo: "bar" },
    status: {
      observedGeneration: 1,
    },
  };

  test("shouldReconcile returns true when generation > observedGeneration", () => {
    expect(Reconciler.shouldReconcile(mockResource)).toBe(true);
  });

  test("shouldReconcile returns false when already reconciled", () => {
    const reconciled = {
      ...mockResource,
      status: { observedGeneration: 2 },
    };
    expect(Reconciler.shouldReconcile(reconciled)).toBe(false);
  });

  test("shouldReconcile returns false when resource is marked for deletion", () => {
    const deletedResource = {
      ...mockResource,
      metadata: {
        ...mockResource.metadata,
        deletionTimestamp: "2023-01-01T00:00:00Z",
      },
    };
    expect(Reconciler.shouldReconcile(deletedResource)).toBe(false);
  });

  test("reconcile updates observedGeneration to match generation", () => {
    const result = Reconciler.reconcile(mockResource);
    expect(result.status?.observedGeneration).toBe(2);
    expect(result.status?.phase).toBe("Ready");
  });
});

describe("Finalizer Logic (TLA+ FinalizeResource)", () => {
  const deletingResource: Resource = {
    kind: "TestResource",
    apiVersion: "guber.dev/v1",
    metadata: {
      name: "test-1",
      generation: 1,
      resourceVersion: "105",
      deletionTimestamp: "2023-01-01T00:00:00Z",
      finalizers: ["guber-controller", "other-finalizer"],
    },
    spec: {},
  };

  test("shouldFinalize returns true when deletionTimestamp is set and finalizer exists", () => {
    expect(Reconciler.shouldFinalize(deletingResource)).toBe(true);
  });

  test("shouldFinalize returns false when no deletionTimestamp", () => {
    const activeResource = {
      ...deletingResource,
      metadata: { ...deletingResource.metadata, deletionTimestamp: undefined },
    };
    expect(Reconciler.shouldFinalize(activeResource)).toBe(false);
  });

  test("shouldFinalize returns false when finalizer already removed", () => {
    const finalizedResource = {
      ...deletingResource,
      metadata: { ...deletingResource.metadata, finalizers: ["other-finalizer"] },
    };
    expect(Reconciler.shouldFinalize(finalizedResource)).toBe(false);
  });

  test("finalize removes the guber-controller finalizer", () => {
    const result = Reconciler.finalize(deletingResource);
    expect(result.metadata.finalizers).not.toContain("guber-controller");
    expect(result.metadata.finalizers).toContain("other-finalizer");
  });
});

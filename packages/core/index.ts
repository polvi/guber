export interface Metadata {
  name: string;
  namespace?: string;
  generation: number;
  resourceVersion: string;
  deletionTimestamp?: string | null;
  finalizers?: string[];
}

export interface Resource<Spec = any, Status = any> {
  kind: string;
  apiVersion: string;
  metadata: Metadata;
  spec: Spec;
  status?: Status & {
    observedGeneration?: number;
    phase?: string;
  };
}

export interface ResourceContext extends Resource {
  group: string;
  version: string;
  plural: string;
  env: any;
}

export interface GuberConfig {
  controllers: any[];
}

export class ApiServer {
  private resources: Map<string, Resource> = new Map();

  /**
   * Implements CreateResource from TLA+ spec.
   * Sets initial version/generation and adds the default finalizer.
   */
  create<T extends Resource>(resource: T): T {
    const key = `${resource.kind}/${resource.metadata.name}`;
    if (this.resources.has(key)) {
      throw new Error("Resource already exists");
    }

    const newResource: T = {
      ...resource,
      metadata: {
        ...resource.metadata,
        resourceVersion: "1",
        generation: 1,
        finalizers: ["guber-controller"],
      },
      status: {
        ...resource.status,
        observedGeneration: 0,
        phase: "Initial",
      } as any,
    };

    this.resources.set(key, newResource);
    return newResource;
  }

  /**
   * Implements UpdateResource from TLA+ spec.
   * Increments version and generation.
   */
  update<T extends Resource>(resource: T): T {
    const key = `${resource.kind}/${resource.metadata.name}`;
    const existing = this.resources.get(key);

    if (!existing) throw new Error("Not found");
    if (existing.metadata.resourceVersion !== resource.metadata.resourceVersion) {
      throw new Error("Conflict: Optimistic concurrency failure");
    }
    if (existing.metadata.deletionTimestamp) {
      throw new Error("Cannot update resource marked for deletion");
    }

    const updated: T = {
      ...resource,
      metadata: {
        ...resource.metadata,
        resourceVersion: (parseInt(existing.metadata.resourceVersion) + 1).toString(),
        generation: existing.metadata.generation + 1,
      },
    };

    this.resources.set(key, updated);
    return updated;
  }

  /**
   * Implements RequestDeleteResource from TLA+ spec.
   * Sets deletionTimestamp instead of immediate removal.
   */
  delete(kind: string, name: string): void {
    const key = `${kind}/${name}`;
    const existing = this.resources.get(key);

    if (!existing) return;

    const updated: Resource = {
      ...existing,
      metadata: {
        ...existing.metadata,
        deletionTimestamp: new Date().toISOString(),
        resourceVersion: (parseInt(existing.metadata.resourceVersion) + 1).toString(),
      },
    };

    this.resources.set(key, updated);
  }

  /**
   * Implements ObserveGarbageCollection from TLA+ spec.
   * Removes resource only if deletionTimestamp is set and finalizers are empty.
   */
  get(kind: string, name: string): Resource | undefined {
    const key = `${kind}/${name}`;
    const resource = this.resources.get(key);

    if (resource?.metadata.deletionTimestamp && (!resource.metadata.finalizers || resource.metadata.finalizers.length === 0)) {
      this.resources.delete(key);
      return undefined;
    }

    return resource;
  }
}

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

export interface Namespace {
  metadata: {
    name: string;
  };
  status?: {
    phase: string;
  };
}

export interface CustomResourceDefinition {
  name: string;
  spec: {
    group: string;
    names: {
      kind: string;
      plural: string;
    };
    versions: Array<{
      name: string;
      schema?: {
        openAPIV3Schema: {
          type: string;
          required?: string[];
          properties?: Record<string, any>;
        };
      };
    }>;
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
  private crds: Map<string, CustomResourceDefinition> = new Map();
  private namespaces: Set<string> = new Set(["default"]);

  private getResourceKey(kind: string, name: string, namespace?: string): string {
    return `${namespace ?? "default"}/${kind}/${name}`;
  }

  /**
   * Implements SchemaValid(spec, schema) from TLA+ spec.
   * Performs basic validation based on required fields.
   */
  private validateSchema(resource: Resource, crd: CustomResourceDefinition): void {
    const version = resource.apiVersion.split("/").pop();
    const vDef = crd.spec.versions.find(v => v.name === version);
    
    if (!vDef || !vDef.schema) return;

    const schema = vDef.schema.openAPIV3Schema;
    if (schema.required) {
      for (const field of schema.required) {
        if (resource.spec[field] === undefined) {
          throw new Error(`Validation failed: missing required field '${field}'`);
        }
      }
    }
  }

  createNamespace(name: string): void {
    this.namespaces.add(name);
  }

  deleteNamespace(name: string): void {
    if (name === "default") return;
    
    // Cascading deletion of resources in this namespace
    for (const [key, resource] of this.resources.entries()) {
      if (resource.metadata.namespace === name) {
        this.resources.delete(key);
      }
    }
    this.namespaces.delete(name);
  }

  /**
   * Implements CreateCRD from TLA+ spec.
   */
  createCRD(crd: CustomResourceDefinition): CustomResourceDefinition {
    if (this.crds.has(crd.name)) {
      throw new Error(`CRD ${crd.name} already exists`);
    }
    this.crds.set(crd.name, crd);
    return crd;
  }

  /**
   * Implements DeleteCRD from TLA+ spec.
   * Models cascading deletion: remaining == { r \in resources : resourceCRD[r] # c }
   */
  deleteCRD(name: string): void {
    const crd = this.crds.get(name);
    if (!crd) return;

    const kindToDelete = crd.spec.names.kind;
    
    // Remove all resources associated with this CRD's kind
    for (const [key, resource] of this.resources.entries()) {
      if (resource.kind === kindToDelete) {
        this.resources.delete(key);
      }
    }

    this.crds.delete(name);
  }

  /**
   * Implements CreateResource from TLA+ spec.
   * Sets initial version/generation and adds the default finalizer.
   * Validates against registered CRDs (ValidResourceCRD invariant).
   */
  create<T extends Resource>(resource: T): T {
    const ns = resource.metadata.namespace ?? "default";
    if (!this.namespaces.has(ns)) {
      throw new Error(`Namespace ${ns} does not exist`);
    }

    // Validate CRD exists
    const crd = Array.from(this.crds.values()).find(
      crd => crd.spec.names.kind === resource.kind
    );

    if (!crd) {
      throw new Error(`No CRD registered for kind: ${resource.kind}`);
    }

    // Implements SchemaValid check
    this.validateSchema(resource, crd);

    const key = this.getResourceKey(resource.kind, resource.metadata.name, resource.metadata.namespace);
    if (this.resources.has(key)) {
      throw new Error("Resource already exists");
    }

    const newResource: T = {
      ...resource,
      metadata: {
        ...resource.metadata,
        namespace: ns,
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
    const key = this.getResourceKey(resource.kind, resource.metadata.name, resource.metadata.namespace);
    const existing = this.resources.get(key);

    if (!existing) throw new Error("Not found");
    if (existing.metadata.resourceVersion !== resource.metadata.resourceVersion) {
      throw new Error("Conflict: Optimistic concurrency failure");
    }

    const crd = Array.from(this.crds.values()).find(
      crd => crd.spec.names.kind === resource.kind
    );
    if (crd) {
      this.validateSchema(resource, crd);
    }

    // If the spec is changing, we block it if deletion is in progress.
    // However, metadata changes (like finalizers) must be allowed.
    if (existing.metadata.deletionTimestamp && JSON.stringify(existing.spec) !== JSON.stringify(resource.spec)) {
      throw new Error("Cannot update resource spec marked for deletion");
    }

    const isSpecChanged = JSON.stringify(existing.spec) !== JSON.stringify(resource.spec);

    const updated: T = {
      ...resource,
      metadata: {
        ...resource.metadata,
        resourceVersion: (parseInt(existing.metadata.resourceVersion) + 1).toString(),
        generation: isSpecChanged ? existing.metadata.generation + 1 : existing.metadata.generation,
      },
    };

    this.resources.set(key, updated);
    return updated;
  }

  /**
   * Implements the status subresource update.
   * Increments resourceVersion but NOT generation.
   */
  patchStatus<T extends Resource>(resource: T): T {
    const key = this.getResourceKey(resource.kind, resource.metadata.name, resource.metadata.namespace);
    const existing = this.resources.get(key);

    if (!existing) throw new Error("Not found");
    if (existing.metadata.resourceVersion !== resource.metadata.resourceVersion) {
      throw new Error("Conflict: Optimistic concurrency failure");
    }

    const updated: T = {
      ...existing,
      status: { ...resource.status },
      metadata: {
        ...existing.metadata,
        resourceVersion: (parseInt(existing.metadata.resourceVersion) + 1).toString(),
      },
    };

    this.resources.set(key, updated);
    return updated;
  }

  /**
   * Implements RequestDeleteResource from TLA+ spec.
   * Sets deletionTimestamp instead of immediate removal.
   */
  delete(kind: string, name: string, namespace?: string): void {
    const key = this.getResourceKey(kind, name, namespace);
    const existing = this.resources.get(key);

    if (!existing) return;
    if (existing.metadata.deletionTimestamp) return;

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
  collectGarbage(kind: string, name: string, namespace?: string): boolean {
    const key = this.getResourceKey(kind, name, namespace);
    const resource = this.resources.get(key);

    if (resource?.metadata.deletionTimestamp && (!resource.metadata.finalizers || resource.metadata.finalizers.length === 0)) {
      this.resources.delete(key);
      return true;
    }
    return false;
  }

  get(kind: string, name: string, namespace?: string): Resource | undefined {
    const key = this.getResourceKey(kind, name, namespace);
    return this.resources.get(key);
  }

  list(kind: string, namespace?: string): Resource[] {
    return Array.from(this.resources.values()).filter(r => {
      const kindMatch = r.kind === kind;
      const nsMatch = namespace ? r.metadata.namespace === namespace : true;
      return kindMatch && nsMatch;
    });
  }
}

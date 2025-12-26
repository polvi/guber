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

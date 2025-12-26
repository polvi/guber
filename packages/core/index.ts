export interface ResourceContext {
  group: string;
  version: string;
  plural: string;
  name: string;
  namespace?: string | null;
  kind: string;
  spec: any;
  status?: any;
  env: any;
}

export interface GuberConfig {
  controllers: any[];
}

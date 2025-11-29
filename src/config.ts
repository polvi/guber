export interface Controller {
  register(app: Hono<any>): void;
  onResourceCreated?(context: ResourceContext): Promise<void>;
  onResourceDeleted?(context: ResourceContext): Promise<void>;
}

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
  controllers: Controller[];
}

export function defineConfig(config: GuberConfig): GuberConfig {
  return config;
}

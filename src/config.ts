export interface Controller {
  register(app: Hono<any>): void;
  handleQueue?(batch: any, env: any): Promise<void>;
  handleScheduled?(event: any, env: any): Promise<void>;
}

export interface GuberConfig {
  controllers: Controller[];
}

export function defineConfig(config: GuberConfig): GuberConfig {
  return config;
}

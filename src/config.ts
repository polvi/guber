import type { Controller } from "./cloudflare";

export interface GuberConfig {
  controllers: Controller[];
}

export function defineConfig(config: GuberConfig): GuberConfig {
  return config;
}

import { Hono } from "hono";
import cloudflare from "./cloudflare";
import github from "./github";

export function registerControllers(app: Hono<any>) {
  // Register cloudflare controller
  // Register github controller
  // No middleware registration needed - we use post-processing hooks
}

export { cloudflare, github };

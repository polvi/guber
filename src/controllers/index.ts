import { Hono } from "hono";
import cloudflare from "./cloudflare";

export function registerControllers(app: Hono<any>) {
  // Register cloudflare controller
  // No middleware registration needed - we use post-processing hooks
}

export { cloudflare };

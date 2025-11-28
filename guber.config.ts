import { defineConfig } from "./src/config";
import cloudflare from "./src/cloudflare";

export default defineConfig({
  controllers: [cloudflare()],
});


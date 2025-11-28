import { defineConfig } from "./src/config";
import cloudflare from "./src/controllers/cloudflare";

export default defineConfig({
  controllers: [cloudflare()],
});

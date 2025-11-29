import { defineConfig } from "./src/config";
import { cloudflare } from "./src/controllers";

export default defineConfig({
  controllers: [cloudflare()],
});

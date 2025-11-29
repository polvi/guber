import { defineConfig } from "orval";

export default defineConfig({
  guberCore: {
    output: {
      mode: "tags-split",
      target: "src/client/gen/core/index.ts",
      schemas: "src/client/gen/core/models",
      client: "fetch",
      baseUrl: "http://fake/",
      override: {
        mutator: {
          path: "./src/client/custom-fetch.ts",
          name: "customFetch",
        },
      },
    },
    input: {
      target: "http://localhost:8787/openapi/v3/api/v1",
    },
    /*
    hooks: {
      afterAllFilesWrite: "bun fmt",
    },
   */
  },
  guberApiextensions: {
    output: {
      mode: "tags-split",
      target: "src/client/gen/apiextensions/index.ts",
      schemas: "src/client/gen/apiextensions/models",
      client: "fetch",
      baseUrl: "http://fake/",
      override: {
        mutator: {
          path: "./src/client/custom-fetch.ts",
          name: "customFetch",
        },
      },
    },
    input: {
      target: "http://localhost:8787/openapi/v3/apis/apiextensions.k8s.io/v1",
    },
  },
  guberCloudflare: {
    output: {
      mode: "tags-split",
      target: "src/client/gen/cloudflare/index.ts",
      schemas: "src/client/gen/cloudflare/models",
      client: "fetch",
      baseUrl: "http://fake/",
      override: {
        mutator: {
          path: "./src/client/custom-fetch.ts",
          name: "customFetch",
        },
      },
    },
    input: {
      target: "http://localhost:8787/openapi/v3/apis/cf.guber.proc.io/v1",
    },
  },
  guberGithub: {
    output: {
      mode: "tags-split",
      target: "src/client/gen/github/index.ts",
      schemas: "src/client/gen/github/models",
      client: "fetch",
      baseUrl: "http://fake/",
      override: {
        mutator: {
          path: "./src/client/custom-fetch.ts",
          name: "customFetch",
        },
      },
    },
    input: {
      target: "http://localhost:8787/openapi/v3/apis/gh.guber.proc.io/v1",
    },
  },
});

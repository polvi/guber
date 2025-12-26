import { expect, test, describe, beforeEach, spyOn } from "bun:test";
import { D1Storage, D1Database } from "./d1-storage";
import { Resource } from "./index";

// Simple Mock for D1
class MockD1 implements D1Database {
  data: Record<string, any[]> = { resources: [], crds: [] };

  prepare(query: string) {
    return {
      bind: (...args: any[]) => ({
        first: async () => this.data.resources[0] || null,
        run: async () => ({ success: true }),
        all: async () => ({ results: this.data.resources, success: true })
      }),
    } as any;
  }
  async batch(stmts: any[]) { return []; }
  async exec(query: string) { return { success: true }; }
}

describe("D1Storage", () => {
  let storage: D1Storage;
  let mockDb: MockD1;

  beforeEach(() => {
    mockDb = new MockD1();
    storage = new D1Storage(mockDb);
  });

  test("bootstrap should execute table creation", async () => {
    const spy = spyOn(mockDb, "exec");
    await storage.bootstrap();
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][0]).toContain("CREATE TABLE IF NOT EXISTS resources");
  });

  test("saveResource should call prepare with correct SQL", async () => {
    const spy = spyOn(mockDb, "prepare");
    const resource: Resource = {
      kind: "Worker",
      apiVersion: "v1",
      metadata: { name: "test", generation: 1, resourceVersion: "1" },
      spec: { foo: "bar" }
    };

    await storage.saveResource(resource);
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][0]).toContain("INSERT INTO resources");
  });
});

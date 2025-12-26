import { Resource, CustomResourceDefinition } from "./index";

export interface D1Database {
  prepare(query: string): D1PreparedStatement;
  batch<T = any>(statements: D1PreparedStatement[]): Promise<T[]>;
  exec<T = any>(query: string): Promise<T>;
}

export interface D1PreparedStatement {
  bind(...values: any[]): D1PreparedStatement;
  first<T = any>(column?: string): Promise<T | null>;
  run<T = any>(): Promise<D1Response>;
  all<T = any>(): Promise<D1Result<T>>;
}

export interface D1Response {
  success: boolean;
  error?: string;
}

export interface D1Result<T = any> {
  results: T[];
  success: boolean;
  error?: string;
}

export class D1Storage {
  constructor(private db: D1Database) {}

  async bootstrap() {
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS resources (
        key TEXT PRIMARY KEY,
        kind TEXT,
        name TEXT,
        namespace TEXT,
        apiVersion TEXT,
        generation INTEGER,
        resourceVersion TEXT,
        spec TEXT,
        status TEXT,
        metadata TEXT
      );
      CREATE TABLE IF NOT EXISTS crds (
        name TEXT PRIMARY KEY,
        data TEXT
      );
    `);
  }

  private getResourceKey(kind: string, name: string, namespace: string = "default"): string {
    return `${namespace}/${kind}/${name}`;
  }

  async saveResource(resource: Resource): Promise<void> {
    const ns = resource.metadata.namespace ?? "default";
    const key = this.getResourceKey(resource.kind, resource.metadata.name, ns);
    
    await this.db.prepare(`
      INSERT INTO resources (key, kind, name, namespace, apiVersion, generation, resourceVersion, spec, status, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET
        generation = excluded.generation,
        resourceVersion = excluded.resourceVersion,
        spec = excluded.spec,
        status = excluded.status,
        metadata = excluded.metadata
    `).bind(
      key,
      resource.kind,
      resource.metadata.name,
      ns,
      resource.apiVersion,
      resource.metadata.generation,
      resource.metadata.resourceVersion,
      JSON.stringify(resource.spec),
      JSON.stringify(resource.status || {}),
      JSON.stringify(resource.metadata)
    ).run();
  }

  async getResource(kind: string, name: string, namespace: string = "default"): Promise<Resource | undefined> {
    const key = this.getResourceKey(kind, name, namespace);
    const row = await this.db.prepare("SELECT * FROM resources WHERE key = ?").bind(key).first<any>();
    
    if (!row) return undefined;

    return {
      kind: row.kind,
      apiVersion: row.apiVersion,
      spec: JSON.parse(row.spec),
      status: JSON.parse(row.status),
      metadata: JSON.parse(row.metadata)
    };
  }

  async deleteResource(kind: string, name: string, namespace: string = "default"): Promise<void> {
    const key = this.getResourceKey(kind, name, namespace);
    await this.db.prepare("DELETE FROM resources WHERE key = ?").bind(key).run();
  }

  async listResources(kind: string, namespace?: string): Promise<Resource[]> {
    let query = "SELECT * FROM resources WHERE kind = ?";
    const params: any[] = [kind];

    if (namespace) {
      query += " AND namespace = ?";
      params.push(namespace);
    }

    const { results } = await this.db.prepare(query).bind(...params).all<any>();
    
    return results.map(row => ({
      kind: row.kind,
      apiVersion: row.apiVersion,
      spec: JSON.parse(row.spec),
      status: JSON.parse(row.status),
      metadata: JSON.parse(row.metadata)
    }));
  }

  async saveCRD(crd: CustomResourceDefinition): Promise<void> {
    await this.db.prepare(`
      INSERT INTO crds (name, data) VALUES (?, ?)
      ON CONFLICT(name) DO UPDATE SET data = excluded.data
    `).bind(crd.name, JSON.stringify(crd)).run();
  }

  async getCRD(name: string): Promise<CustomResourceDefinition | undefined> {
    const row = await this.db.prepare("SELECT data FROM crds WHERE name = ?").bind(name).first<{ data: string }>();
    return row ? JSON.parse(row.data) : undefined;
  }

  async listCRDs(): Promise<CustomResourceDefinition[]> {
    const { results } = await this.db.prepare("SELECT data FROM crds").all<{ data: string }>();
    return results.map(r => JSON.parse(r.data));
  }
}

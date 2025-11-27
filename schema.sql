-- Table for CRD definitions (like apiextensions.k8s.io/v1/CRDs)
CREATE TABLE crds (
  name TEXT PRIMARY KEY,         -- e.g. "boardposts.aopa.bulletin"
  group_name TEXT NOT NULL,      -- "aopa.bulletin"
  version TEXT NOT NULL,         -- "v1"
  kind TEXT NOT NULL,            -- "BoardPost"
  plural TEXT NOT NULL,          -- "boardposts"
  short_names TEXT,              -- JSON array of short names, e.g. ["bp"]
  schema TEXT,                   -- JSON schema (optional)
  scope TEXT DEFAULT 'Cluster',  -- "Namespaced" or "Cluster"
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Table for actual resource instances
CREATE TABLE resources (
  id TEXT PRIMARY KEY,
  group_name TEXT NOT NULL,
  version TEXT NOT NULL,
  kind TEXT NOT NULL,
  plural TEXT NOT NULL,
  name TEXT NOT NULL,            -- metadata.name
  namespace TEXT,                -- metadata.namespace (NULL for cluster-scoped)
  spec TEXT NOT NULL,            -- JSON
  status TEXT,                   -- JSON
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

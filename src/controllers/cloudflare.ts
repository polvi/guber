import { Hono } from "hono";
import { v4 as uuid } from "uuid";
import type { Controller, ResourceContext } from "../config";

export default function cloudflare(): Controller {
  return new CloudflareController();
}

class CloudflareController implements Controller {
  register(app: Hono<any>): void {
    // No middleware registration needed - we use post-processing hooks
  }

  async onResourceCreated(context: ResourceContext): Promise<void> {
    const { group, kind, name, spec, env } = context;

    // Only handle cf.guber.proc.io resources
    if (group !== "cf.guber.proc.io") return;

    // Queue for provisioning if it's a Cloudflare resource type
    if (
      (kind === "D1" || kind === "Queue" || kind === "Worker") &&
      env.GUBER_BUS
    ) {
      await env.GUBER_BUS.send({
        action: "create",
        resourceType: kind.toLowerCase(),
        resourceName: name,
        group: context.group,
        kind: context.kind,
        plural: context.plural,
        namespace: context.namespace,
        spec: spec,
      });
    }
  }

  async onResourceDeleted(context: ResourceContext): Promise<void> {
    const { group, kind, name, spec, status, env } = context;

    // Only handle cf.guber.proc.io resources
    if (group !== "cf.guber.proc.io") return;

    // Queue for deletion if it's a Cloudflare resource type
    if (
      (kind === "D1" || kind === "Queue" || kind === "Worker") &&
      env.GUBER_BUS
    ) {
      await env.GUBER_BUS.send({
        action: "delete",
        resourceType: kind.toLowerCase(),
        resourceName: name,
        group: context.group,
        kind: context.kind,
        plural: context.plural,
        namespace: context.namespace,
        spec: spec,
        status: status,
      });
    }
  }

  async handleQueue(batch: any, env: any): Promise<void> {
    for (const message of batch.messages) {
      try {
        const {
          action,
          resourceType,
          resourceName,
          group,
          kind,
          plural,
          namespace,
          spec,
          status,
        } = message.body;

        if (action === "create") {
          let provisioningSuccessful = false;

          if (resourceType === "d1") {
            provisioningSuccessful = await this.provisionD1Database(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
            );
          } else if (resourceType === "queue") {
            provisioningSuccessful = await this.provisionQueue(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
            );
          } else if (resourceType === "worker") {
            provisioningSuccessful = await this.provisionWorker(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
            );
          }

          // After successful provisioning, check for dependent resources
          if (provisioningSuccessful) {
            await this.checkAndProvisionDependentResources(
              env,
              resourceName,
              group,
              kind,
            );
          }
        } else if (action === "delete") {
          if (resourceType === "d1") {
            await this.deleteD1Database(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
              status,
            );
          } else if (resourceType === "queue") {
            await this.deleteQueue(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
              status,
            );
          } else if (resourceType === "worker") {
            await this.deleteWorker(
              env,
              resourceName,
              group,
              kind,
              plural,
              namespace,
              spec,
              status,
            );
          }
        }

        message.ack();
      } catch (error) {
        console.error(`Failed to process queue message:`, error);
        message.retry();
      }
    }
  }

  async handleScheduled(event: any, env: any): Promise<void> {
    console.log(
      `Running Cloudflare resource reconciliation at ${new Date(event.scheduledTime).toISOString()}`,
    );
    await this.reconcileD1Databases(env);
    await this.reconcileQueues(env);
    await this.reconcileWorkers(env);
  }

  private async checkAndProvisionDependentResources(
    env: any,
    resolvedResourceName: string,
    resolvedGroup: string,
    resolvedKind: string,
  ) {
    console.log(
      `Checking for resources dependent on ${resolvedKind}/${resolvedResourceName}`,
    );

    // Find all resources that depend on this newly resolved resource
    const { results } = await env.DB.prepare(
      `
      SELECT * FROM resources 
      WHERE group_name='cf.guber.proc.io' 
      AND kind='Worker' 
      AND json_extract(status, '$.state') = 'Pending'
    `,
    ).all();

    for (const resource of results || []) {
      try {
        const spec = JSON.parse(resource.spec);
        const status = resource.status ? JSON.parse(resource.status) : {};

        if (spec.dependencies && status.pendingDependencies) {
          const hasDependency = spec.dependencies.some(
            (dep: any) =>
              dep.name === resolvedResourceName &&
              dep.kind === resolvedKind &&
              (dep.group || "cf.guber.proc.io") === resolvedGroup,
          );

          if (hasDependency) {
            console.log(
              `Found dependent resource ${resource.name}, checking if all dependencies are now ready`,
            );

            // Check if ALL dependencies are now ready
            let allDependenciesReady = true;
            const unresolvedDependencies = [];

            for (const dependency of spec.dependencies) {
              const depGroup = dependency.group || "cf.guber.proc.io";
              const depResource = await env.DB.prepare(
                "SELECT * FROM resources WHERE name=? AND kind=? AND group_name=? AND namespace IS NULL",
              )
                .bind(dependency.name, dependency.kind, depGroup)
                .first();

              if (!depResource || !depResource.status) {
                allDependenciesReady = false;
                unresolvedDependencies.push(dependency);
                continue;
              }

              const depStatus = JSON.parse(depResource.status);
              if (depStatus.state !== "Ready") {
                allDependenciesReady = false;
                unresolvedDependencies.push(dependency);
              }
            }

            if (allDependenciesReady) {
              console.log(
                `✅ All dependencies resolved for worker ${resource.name}, re-queuing for provisioning`,
              );

              // Queue for provisioning
              if (env.GUBER_BUS) {
                await env.GUBER_BUS.send({
                  action: "create",
                  resourceType: "worker",
                  resourceName: resource.name,
                  group: resource.group_name,
                  kind: resource.kind,
                  plural: resource.plural,
                  namespace: resource.namespace,
                  spec: spec,
                });
              }
            } else {
              console.log(
                `⏳ Worker ${resource.name} still has unresolved dependencies:`,
                unresolvedDependencies.map((d) => `${d.kind}/${d.name}`),
              );

              // Update status to reflect current dependency state
              const updatedStatus = {
                ...status,
                state: "Pending",
                message: `Waiting for dependencies: ${unresolvedDependencies.map((d) => `${d.kind}/${d.name}`).join(", ")}`,
                pendingDependencies: unresolvedDependencies,
                lastDependencyCheck: new Date().toISOString(),
              };

              await env.DB.prepare(
                "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
              )
                .bind(JSON.stringify(updatedStatus), resource.name)
                .run();
            }
          }
        }
      } catch (error) {
        console.error(
          `Error checking dependencies for resource ${resource.name}:`,
          error,
        );
      }
    }
  }

  private buildFullDatabaseName(
    resourceName: string,
    group: string,
    plural: string,
    namespace: string | null,
    instanceName: string,
  ): string {
    // Construct full database name: name-namespace-resource-type-instance
    const namespaceStr = namespace || "c";
    const resourceType = `${plural}-${group.replace(/\./g, "-")}`;
    return `${resourceName}-${namespaceStr}-${resourceType}-${instanceName}`;
  }

  private async provisionD1Database(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
  ): Promise<boolean> {
    const fullDatabaseName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );

    const requestBody: any = {
      name: fullDatabaseName,
    };

    // Only add primary_location_hint if location is specified
    if (spec.location) {
      requestBody.primary_location_hint = spec.location;
    }

    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      },
    );

    if (response.ok) {
      const result = await response.json();
      const databaseId = result.result.uuid;

      // Update the resource status in the database
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
      )
        .bind(
          JSON.stringify({
            state: "Ready",
            database_id: databaseId,
            createdAt: new Date().toISOString(),
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
          }),
          resourceName,
        )
        .run();

      console.log(
        `D1 database ${fullDatabaseName} provisioned successfully with ID: ${databaseId}`,
      );
      return true;
    } else {
      const errorResponse = await response.json();

      // Check if the error is because the database already exists
      if (
        errorResponse.errors &&
        errorResponse.errors.some((err: any) => err.code === 7502)
      ) {
        console.log(
          `Database ${fullDatabaseName} already exists, attempting to find and match existing database`,
        );

        // List existing databases to find the one with matching name
        const listResponse = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            },
          },
        );

        if (listResponse.ok) {
          const listResult = await listResponse.json();
          const existingDb = listResult.result.find(
            (db: any) => db.name === fullDatabaseName,
          );

          if (existingDb) {
            const databaseId = existingDb.uuid;

            // Update the resource status to match the existing database
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Ready",
                  database_id: databaseId,
                  createdAt: existingDb.created_on,
                  endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
                }),
                resourceName,
              )
              .run();

            console.log(
              `Matched existing D1 database ${fullDatabaseName} with ID: ${databaseId}`,
            );
          } else {
            console.error(
              `Could not find existing database ${fullDatabaseName} in account`,
            );
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Failed",
                  error: "Database exists but could not be found in account",
                }),
                resourceName,
              )
              .run();
          }
        } else {
          console.error(
            `Failed to list databases to find existing ${fullDatabaseName}`,
          );
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
          )
            .bind(
              JSON.stringify({
                state: "Failed",
                error: JSON.stringify(errorResponse),
              }),
              resourceName,
            )
            .run();
        }
      } else {
        console.error(
          `Failed to provision D1 database ${fullDatabaseName}:`,
          JSON.stringify(errorResponse),
        );

        // Update status to failed
        await env.DB.prepare(
          "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
        )
          .bind(
            JSON.stringify({
              state: "Failed",
              error: JSON.stringify(errorResponse),
            }),
            resourceName,
          )
          .run();
      }
    }

    return false;
  }

  private async deleteD1Database(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
    status?: any,
  ) {
    const fullDatabaseName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );
    // Get database ID from the passed status or spec
    const databaseId = status?.database_id;

    if (databaseId) {
      try {
        const response = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
          {
            method: "DELETE",
            headers: {
              Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            },
          },
        );

        if (response.ok) {
          console.log(
            `D1 database ${fullDatabaseName} (ID: ${databaseId}) deleted successfully`,
          );
        } else {
          const error = await response.text();
          console.error(
            `Failed to delete D1 database ${fullDatabaseName} (ID: ${databaseId}):`,
            error,
          );
        }
      } catch (error) {
        console.error(`Error deleting D1 database ${fullDatabaseName}:`, error);
      }
    } else {
      console.log(
        `No database ID found for ${fullDatabaseName}, skipping Cloudflare deletion`,
      );
    }
  }

  private async provisionQueue(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
  ): Promise<boolean> {
    const fullQueueName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );

    const requestBody: any = {
      queue_name: fullQueueName,
    };

    // Add settings if specified
    if (spec.settings) {
      requestBody.settings = spec.settings;
    }

    const response = await fetch(
      `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      },
    );

    if (response.ok) {
      const result = await response.json();
      const queueId = result.result.queue_id;

      // Update the resource status in the database
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
      )
        .bind(
          JSON.stringify({
            state: "Ready",
            queue_id: queueId,
            createdAt: new Date().toISOString(),
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
          }),
          resourceName,
        )
        .run();

      console.log(
        `Queue ${fullQueueName} provisioned successfully with ID: ${queueId}`,
      );
      return true;
    } else {
      const errorResponse = await response.json();

      // Check if the error is because the queue already exists
      if (
        errorResponse.errors &&
        errorResponse.errors.some((err: any) => err.code === 10026)
      ) {
        console.log(
          `Queue ${fullQueueName} already exists, attempting to find and match existing queue`,
        );

        // List existing queues to find the one with matching name
        const listResponse = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            },
          },
        );

        if (listResponse.ok) {
          const listResult = await listResponse.json();
          const existingQueue = listResult.result.find(
            (queue: any) => queue.queue_name === fullQueueName,
          );

          if (existingQueue) {
            const queueId = existingQueue.queue_id;

            // Update the resource status to match the existing queue
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Ready",
                  queue_id: queueId,
                  createdAt: existingQueue.created_on,
                  endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
                }),
                resourceName,
              )
              .run();

            console.log(
              `Matched existing Queue ${fullQueueName} with ID: ${queueId}`,
            );
          } else {
            console.error(
              `Could not find existing queue ${fullQueueName} in account`,
            );
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Failed",
                  error: "Queue exists but could not be found in account",
                }),
                resourceName,
              )
              .run();
          }
        } else {
          console.error(
            `Failed to list queues to find existing ${fullQueueName}`,
          );
          await env.DB.prepare(
            "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
          )
            .bind(
              JSON.stringify({
                state: "Failed",
                error: JSON.stringify(errorResponse),
              }),
              resourceName,
            )
            .run();
        }
      } else {
        console.error(
          `Failed to provision Queue ${fullQueueName}:`,
          JSON.stringify(errorResponse),
        );

        // Update status to failed
        await env.DB.prepare(
          "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
        )
          .bind(
            JSON.stringify({
              state: "Failed",
              error: JSON.stringify(errorResponse),
            }),
            resourceName,
          )
          .run();
      }
    }

    return false;
  }

  private async deleteQueue(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
    status?: any,
  ) {
    const fullQueueName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );
    // Get queue ID from the passed status or spec
    const queueId = status?.queue_id;

    if (queueId) {
      try {
        const response = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
          {
            method: "DELETE",
            headers: {
              Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            },
          },
        );

        if (response.ok) {
          console.log(
            `Queue ${fullQueueName} (ID: ${queueId}) deleted successfully`,
          );
        } else {
          const error = await response.text();
          console.error(
            `Failed to delete Queue ${fullQueueName} (ID: ${queueId}):`,
            error,
          );
        }
      } catch (error) {
        console.error(`Error deleting Queue ${fullQueueName}:`, error);
      }
    } else {
      console.log(
        `No queue ID found for ${fullQueueName}, skipping Cloudflare deletion`,
      );
    }
  }

  private async reconcileQueues(env: any) {
    try {
      console.log("Starting Queue reconciliation...");

      // Get all Queue resources from our API
      const { results: apiResources } = await env.DB.prepare(
        "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='Queue'",
      ).all();

      // Get all Queues from Cloudflare
      const cloudflareResponse = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          },
        },
      );

      if (!cloudflareResponse.ok) {
        console.error(
          "Failed to fetch Queues from Cloudflare:",
          await cloudflareResponse.text(),
        );
        return;
      }

      const cloudflareResult = await cloudflareResponse.json();
      const cloudflareQueues = cloudflareResult.result || [];

      // Create maps for easier comparison
      const apiQueueMap = new Map();
      const cloudflareQueueMap = new Map();

      // Build API queue map with full names
      for (const resource of apiResources || []) {
        const fullQueueName = this.buildFullDatabaseName(
          resource.name,
          resource.group_name,
          resource.plural,
          resource.namespace,
          env.GUBER_NAME,
        );
        apiQueueMap.set(fullQueueName, resource);
      }

      // Build Cloudflare queue map
      for (const queue of cloudflareQueues) {
        cloudflareQueueMap.set(queue.queue_name, queue);
      }

      console.log(
        `Found ${apiQueueMap.size} Queue resources in API and ${cloudflareQueueMap.size} queues in Cloudflare`,
      );

      // Find queues that exist in Cloudflare but not in our API (orphaned queues)
      const orphanedQueues = [];
      for (const [queueName, cloudflareQueue] of cloudflareQueueMap) {
        // Only consider queues that match our naming pattern
        if (
          queueName.includes("-") &&
          (queueName.includes("-qs-cf-guber-proc-io") ||
            queueName.includes("-q-cf-guber-proc-io"))
        ) {
          if (!apiQueueMap.has(queueName)) {
            orphanedQueues.push(cloudflareQueue);
          }
        }
      }

      // Find resources that exist in our API but not in Cloudflare (missing queues)
      const missingQueues = [];
      for (const [fullName, apiResource] of apiQueueMap) {
        if (!cloudflareQueueMap.has(fullName)) {
          missingQueues.push({ fullName, resource: apiResource });
        }
      }

      console.log(
        `Found ${orphanedQueues.length} orphaned queues and ${missingQueues.length} missing queues`,
      );

      // Delete orphaned queues from Cloudflare
      for (const orphanedQueue of orphanedQueues) {
        try {
          console.log(
            `Deleting orphaned queue: ${orphanedQueue.queue_name} (ID: ${orphanedQueue.queue_id})`,
          );

          const deleteResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${orphanedQueue.queue_id}`,
            {
              method: "DELETE",
              headers: {
                Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
              },
            },
          );

          if (deleteResponse.ok) {
            console.log(
              `Successfully deleted orphaned queue: ${orphanedQueue.queue_name}`,
            );
          } else {
            const error = await deleteResponse.text();
            console.error(
              `Failed to delete orphaned queue ${orphanedQueue.queue_name}:`,
              error,
            );
          }
        } catch (error) {
          console.error(
            `Error deleting orphaned queue ${orphanedQueue.queue_name}:`,
            error,
          );
        }
      }

      // Create missing queues in Cloudflare
      for (const { fullName, resource } of missingQueues) {
        try {
          console.log(`Creating missing queue: ${fullName}`);

          const spec = JSON.parse(resource.spec);
          const requestBody: any = { queue_name: fullName };

          if (spec.settings) {
            requestBody.settings = spec.settings;
          }

          const createResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify(requestBody),
            },
          );

          if (createResponse.ok) {
            const result = await createResponse.json();
            const queueId = result.result.queue_id;

            // Update the resource status in our database
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = resource.namespace
              ? [
                  JSON.stringify({
                    state: "Ready",
                    queue_id: queueId,
                    createdAt: new Date().toISOString(),
                    endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                  resource.namespace,
                ]
              : [
                  JSON.stringify({
                    state: "Ready",
                    queue_id: queueId,
                    createdAt: new Date().toISOString(),
                    endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/queues/${queueId}`,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                ];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();

            console.log(
              `Successfully created missing queue: ${fullName} with ID: ${queueId}`,
            );
          } else {
            const error = await createResponse.text();
            console.error(`Failed to create missing queue ${fullName}:`, error);

            // Update status to failed
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = resource.namespace
              ? [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                  resource.namespace,
                ]
              : [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                ];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();
          }
        } catch (error) {
          console.error(`Error creating missing queue ${fullName}:`, error);
        }
      }

      console.log("Queue reconciliation completed");
    } catch (error) {
      console.error("Error during Queue reconciliation:", error);
    }
  }

  private async provisionWorker(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
  ): Promise<boolean> {
    const fullWorkerName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );
    const customDomain = `${resourceName}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`;

    try {
      // Check dependencies first
      if (spec.dependencies && spec.dependencies.length > 0) {
        console.log(
          `Checking ${spec.dependencies.length} dependencies for worker ${fullWorkerName}`,
        );

        for (const dependency of spec.dependencies) {
          const depGroup = dependency.group || "cf.guber.proc.io";
          const depKind = dependency.kind;
          const depName = dependency.name;

          const depResource = await env.DB.prepare(
            "SELECT * FROM resources WHERE name=? AND kind=? AND group_name=? AND namespace IS NULL",
          )
            .bind(depName, depKind, depGroup)
            .first();

          if (!depResource) {
            console.log(
              `Dependency ${depKind}/${depName} not found, deferring provisioning`,
            );
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Pending",
                  message: `Waiting for dependency: ${depKind}/${depName}`,
                  pendingDependencies: [dependency],
                }),
                resourceName,
              )
              .run();
            return false;
          }

          if (!depResource.status) {
            console.log(
              `Dependency ${depKind}/${depName} has no status, deferring provisioning`,
            );
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Pending",
                  message: `Waiting for dependency to be provisioned: ${depKind}/${depName}`,
                  pendingDependencies: [dependency],
                }),
                resourceName,
              )
              .run();
            return false;
          }

          const depStatus = JSON.parse(depResource.status);
          if (depStatus.state !== "Ready") {
            console.log(
              `Dependency ${depKind}/${depName} not ready (${depStatus.state}), deferring provisioning`,
            );
            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(
                JSON.stringify({
                  state: "Pending",
                  message: `Waiting for dependency to be ready: ${depKind}/${depName} (current state: ${depStatus.state})`,
                  pendingDependencies: [dependency],
                }),
                resourceName,
              )
              .run();
            return false;
          }
        }

        console.log(`All dependencies satisfied for worker ${fullWorkerName}`);
      }

      // Get the worker script content
      let script: string;

      if (spec.scriptUrl) {
        const scriptResponse = await fetch(spec.scriptUrl, {
          redirect: "follow",
          headers: {
            "User-Agent": "Guber-Worker-Provisioner/1.0",
          },
        });
        if (!scriptResponse.ok) {
          throw new Error(
            `Failed to fetch script from ${spec.scriptUrl}: ${scriptResponse.status} ${scriptResponse.statusText}`,
          );
        }
        script = await scriptResponse.text();
      } else if (spec.script) {
        script = spec.script;
      } else {
        throw new Error(
          "Worker must have either 'script' or 'scriptUrl' specified",
        );
      }

      // Step 1: Deploy the worker script
      // Create multipart form data for the worker upload
      const formData = new FormData();

      // Check for source map if scriptUrl is provided
      let sourceMap: string | null = null;
      if (spec.scriptUrl) {
        try {
          const sourceMapUrl = spec.scriptUrl + ".map";
          const sourceMapResponse = await fetch(sourceMapUrl, {
            redirect: "follow",
            headers: {
              "User-Agent": "Guber-Worker-Provisioner/1.0",
            },
          });
          if (sourceMapResponse.ok) {
            sourceMap = await sourceMapResponse.text();
            console.log(`Found source map at ${sourceMapUrl}`);
          }
        } catch (error) {
          // Source map is optional, continue without it
          console.log(`No source map found for ${spec.scriptUrl}`);
        }
      }

      // Always use module format with main_module
      const metadata: any = {
        main_module: "index.js",
        compatibility_date: spec.compatibility_date || "2023-05-18",
      };

      // Add compatibility settings if specified
      if (spec.compatibility_date) {
        metadata.compatibility_date = spec.compatibility_date;
      }
      if (spec.compatibility_flags) {
        metadata.compatibility_flags = spec.compatibility_flags;
      }

      // Add bindings if specified
      const bindings: any[] = [];

      if (spec.bindings) {
        // Handle D1 database bindings
        if (spec.bindings.d1_databases) {
          for (const d1Binding of spec.bindings.d1_databases) {
            // Look up the D1 resource to get its database_id
            const d1Resource = await env.DB.prepare(
              "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
            )
              .bind(d1Binding.database_name)
              .first();

            if (d1Resource && d1Resource.status) {
              const status = JSON.parse(d1Resource.status);
              if (status.database_id) {
                const binding = {
                  type: "d1",
                  name: d1Binding.binding,
                  id: status.database_id,
                };
                bindings.push(binding);
                console.log(
                  `Added D1 binding: ${d1Binding.database_name} -> ${d1Binding.binding}`,
                );
              } else {
                console.log(
                  `D1 resource ${d1Binding.database_name} has no database_id`,
                );
              }
            } else {
              console.log(`D1 resource ${d1Binding.database_name} not found`);
            }
          }
        }

        // Handle Queue bindings
        if (spec.bindings.queues) {
          for (const queueBinding of spec.bindings.queues) {
            // Look up the Queue resource to get its queue name
            const queueResource = await env.DB.prepare(
              "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
            )
              .bind(queueBinding.queue_name)
              .first();

            if (queueResource && queueResource.status) {
              const status = JSON.parse(queueResource.status);
              if (status.queue_id) {
                // Build the full queue name that was created in Cloudflare
                const fullQueueName = this.buildFullDatabaseName(
                  queueResource.name,
                  queueResource.group_name,
                  queueResource.plural,
                  queueResource.namespace,
                  env.GUBER_NAME,
                );
                const binding = {
                  type: "queue",
                  name: queueBinding.binding,
                  queue_name: fullQueueName,
                };
                bindings.push(binding);
                console.log(
                  `Added Queue binding: ${queueBinding.queue_name} -> ${queueBinding.binding}`,
                );
              } else {
                console.log(
                  `Queue resource ${queueBinding.queue_name} has no queue_id`,
                );
              }
            } else {
              console.log(
                `Queue resource ${queueBinding.queue_name} not found`,
              );
            }
          }
        }
      }

      if (bindings.length > 0) {
        metadata.bindings = bindings;
      }

      formData.append("metadata", JSON.stringify(metadata));
      formData.append(
        "index.js",
        new Blob([script], { type: "application/javascript+module" }),
        "index.js",
      );

      // Add source map if available
      if (sourceMap) {
        formData.append(
          "index.js.map",
          new Blob([sourceMap], { type: "text/plain" }),
          "index.js.map",
        );
      }

      console.log(
        `Deploying worker ${fullWorkerName} with ${bindings.length} bindings`,
      );

      const deployResponse = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          },
          body: formData,
        },
      );

      if (!deployResponse.ok) {
        const errorResponse = await deployResponse.json();
        throw new Error(
          `Failed to deploy worker script: ${JSON.stringify(errorResponse)}`,
        );
      }

      console.log(`Worker script ${fullWorkerName} deployed successfully`);

      // Step 2: Create custom domain
      const domainResponse = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            hostname: customDomain,
            service: fullWorkerName,
            environment: "production",
          }),
        },
      );

      if (!domainResponse.ok) {
        const domainError = await domainResponse.json();
        console.error(
          `Failed to create custom domain ${customDomain}:`,
          JSON.stringify(domainError),
        );

        // Still update status as partially successful (script deployed but no custom domain)
        await env.DB.prepare(
          "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
        )
          .bind(
            JSON.stringify({
              state: "PartiallyReady",
              worker_id: fullWorkerName,
              createdAt: new Date().toISOString(),
              endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
              customDomain: customDomain,
              domainError: JSON.stringify(domainError),
            }),
            resourceName,
          )
          .run();

        console.log(
          `Worker ${fullWorkerName} deployed but custom domain setup failed`,
        );
        return false;
      }

      const domainResult = await domainResponse.json();
      console.log(
        `Custom domain ${customDomain} created successfully for worker ${fullWorkerName}`,
      );

      // Step 3: Update the resource status in the database
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
      )
        .bind(
          JSON.stringify({
            state: "Ready",
            worker_id: fullWorkerName,
            createdAt: new Date().toISOString(),
            endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
            customDomain: customDomain,
            domainId: domainResult.result?.id,
            url: `https://${customDomain}`,
          }),
          resourceName,
        )
        .run();

      console.log(
        `Worker ${fullWorkerName} provisioned successfully at ${customDomain}`,
      );
      return true;
    } catch (error) {
      console.error(`Failed to provision Worker ${fullWorkerName}:`, error);

      // Update status to failed
      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
      )
        .bind(
          JSON.stringify({
            state: "Failed",
            error: error.message || String(error),
          }),
          resourceName,
        )
        .run();

      return false;
    }
  }

  private async deleteWorker(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
    status?: any,
  ) {
    const fullWorkerName = this.buildFullDatabaseName(
      resourceName,
      group,
      plural,
      namespace,
      env.GUBER_NAME,
    );
    const customDomain = `${resourceName}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`;

    try {
      // Step 1: Delete custom domain if it exists
      if (status?.domainId) {
        const domainResponse = await fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains/${status.domainId}`,
          {
            method: "DELETE",
            headers: {
              Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
            },
          },
        );

        if (domainResponse.ok) {
          console.log(`Custom domain ${customDomain} deleted successfully`);
        } else {
          const error = await domainResponse.text();
          console.error(
            `Failed to delete custom domain ${customDomain}:`,
            error,
          );
        }
      }

      // Step 2: Delete the worker script
      const scriptResponse = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullWorkerName}`,
        {
          method: "DELETE",
          headers: {
            Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          },
        },
      );

      if (scriptResponse.ok) {
        console.log(`Worker ${fullWorkerName} deleted successfully`);
      } else {
        const error = await scriptResponse.text();
        console.error(`Failed to delete Worker ${fullWorkerName}:`, error);
      }
    } catch (error) {
      console.error(`Error deleting Worker ${fullWorkerName}:`, error);
    }
  }

  private async reconcileWorkers(env: any) {
    try {
      console.log("Starting Worker reconciliation...");

      // First, check pending workers for dependency resolution
      await this.reconcilePendingWorkers(env);

      // Get all Worker resources from our API
      const { results: apiResources } = await env.DB.prepare(
        "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='Worker'",
      ).all();

      // Get all Workers from Cloudflare
      const [workersResponse, domainsResponse] = await Promise.all([
        fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts`,
          {
            method: "GET",
            headers: { Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
          },
        ),
        fetch(
          `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`,
          {
            method: "GET",
            headers: { Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
          },
        ),
      ]);

      if (!workersResponse.ok) {
        console.error(
          "Failed to fetch Workers from Cloudflare:",
          await workersResponse.text(),
        );
        return;
      }

      if (!domainsResponse.ok) {
        console.error(
          "Failed to fetch Worker domains from Cloudflare:",
          await domainsResponse.text(),
        );
        return;
      }

      const workersResult = await workersResponse.json();
      const domainsResult = await domainsResponse.json();
      const cloudflareWorkers = workersResult.result || [];
      const cloudflareDomains = domainsResult.result || [];

      // Create maps for easier comparison
      const apiWorkerMap = new Map();
      const cloudflareWorkerMap = new Map();
      const cloudflareDomainMap = new Map();

      // Build API worker map with full names
      for (const resource of apiResources || []) {
        const fullWorkerName = this.buildFullDatabaseName(
          resource.name,
          resource.group_name,
          resource.plural,
          resource.namespace,
          env.GUBER_NAME,
        );
        apiWorkerMap.set(fullWorkerName, resource);
      }

      // Build Cloudflare worker and domain maps
      for (const worker of cloudflareWorkers) {
        cloudflareWorkerMap.set(worker.id, worker);
      }

      for (const domain of cloudflareDomains) {
        cloudflareDomainMap.set(domain.hostname, domain);
      }

      console.log(
        `Found ${apiWorkerMap.size} Worker resources in API, ${cloudflareWorkerMap.size} workers, and ${cloudflareDomainMap.size} domains in Cloudflare`,
      );

      // Find workers that exist in Cloudflare but not in our API (orphaned workers)
      const orphanedWorkers = [];
      for (const [workerName, cloudflareWorker] of cloudflareWorkerMap) {
        // Only consider workers that match our naming pattern
        if (
          workerName.includes("-") &&
          (workerName.includes("-workers-cf-guber-proc-io") ||
            workerName.includes("-worker-cf-guber-proc-io"))
        ) {
          if (!apiWorkerMap.has(workerName)) {
            orphanedWorkers.push(cloudflareWorker);
          }
        }
      }

      // Find orphaned domains
      const orphanedDomains = [];
      for (const [hostname, domain] of cloudflareDomainMap) {
        if (hostname.endsWith(`.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`)) {
          const workerName = hostname.split(".")[0];
          const found = Array.from(apiWorkerMap.values()).some(
            (resource) => resource.name === workerName,
          );
          if (!found) {
            orphanedDomains.push(domain);
          }
        }
      }

      // Find resources that exist in our API but not in Cloudflare (missing workers)
      const missingWorkers = [];
      for (const [fullName, apiResource] of apiWorkerMap) {
        if (!cloudflareWorkerMap.has(fullName)) {
          missingWorkers.push({ fullName, resource: apiResource });
        }
      }

      console.log(
        `Found ${orphanedWorkers.length} orphaned workers, ${orphanedDomains.length} orphaned domains, and ${missingWorkers.length} missing workers`,
      );

      // Delete orphaned domains first
      for (const orphanedDomain of orphanedDomains) {
        try {
          console.log(`Deleting orphaned domain: ${orphanedDomain.hostname}`);

          const deleteResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains/${orphanedDomain.id}`,
            {
              method: "DELETE",
              headers: { Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
            },
          );

          if (deleteResponse.ok) {
            console.log(
              `Successfully deleted orphaned domain: ${orphanedDomain.hostname}`,
            );
          } else {
            const error = await deleteResponse.text();
            console.error(
              `Failed to delete orphaned domain ${orphanedDomain.hostname}:`,
              error,
            );
          }
        } catch (error) {
          console.error(
            `Error deleting orphaned domain ${orphanedDomain.hostname}:`,
            error,
          );
        }
      }

      // Delete orphaned workers
      for (const orphanedWorker of orphanedWorkers) {
        try {
          console.log(`Deleting orphaned worker: ${orphanedWorker.id}`);

          const deleteResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${orphanedWorker.id}`,
            {
              method: "DELETE",
              headers: { Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}` },
            },
          );

          if (deleteResponse.ok) {
            console.log(
              `Successfully deleted orphaned worker: ${orphanedWorker.id}`,
            );
          } else {
            const error = await deleteResponse.text();
            console.error(
              `Failed to delete orphaned worker ${orphanedWorker.id}:`,
              error,
            );
          }
        } catch (error) {
          console.error(
            `Error deleting orphaned worker ${orphanedWorker.id}:`,
            error,
          );
        }
      }

      // Create missing workers in Cloudflare
      for (const { fullName, resource } of missingWorkers) {
        try {
          console.log(`Creating missing worker: ${fullName}`);

          const spec = JSON.parse(resource.spec);
          const customDomain = `${resource.name}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`;

          // Get the worker script content
          let script: string;
          if (spec.scriptUrl) {
            const scriptResponse = await fetch(spec.scriptUrl, {
              redirect: "follow",
              headers: {
                "User-Agent": "Guber-Worker-Provisioner/1.0",
              },
            });
            if (!scriptResponse.ok) {
              throw new Error(
                `Failed to fetch script from ${spec.scriptUrl}: ${scriptResponse.status} ${scriptResponse.statusText}`,
              );
            }
            script = await scriptResponse.text();
          } else if (spec.script) {
            script = spec.script;
          } else {
            throw new Error(
              "Worker must have either 'script' or 'scriptUrl' specified",
            );
          }

          // Create worker script
          // Create multipart form data for the worker upload
          const formData = new FormData();

          // Check for source map if scriptUrl is provided
          let sourceMap: string | null = null;
          if (spec.scriptUrl) {
            try {
              const sourceMapUrl = spec.scriptUrl + ".map";
              const sourceMapResponse = await fetch(sourceMapUrl, {
                redirect: "follow",
                headers: {
                  "User-Agent": "Guber-Worker-Provisioner/1.0",
                },
              });
              if (sourceMapResponse.ok) {
                sourceMap = await sourceMapResponse.text();
                console.log(`Found source map at ${sourceMapUrl}`);
              }
            } catch (error) {
              // Source map is optional, continue without it
              console.log(`No source map found for ${spec.scriptUrl}`);
            }
          }

          // Always use module format with main_module
          const metadata: any = {
            main_module: "index.js",
            compatibility_date: spec.compatibility_date || "2023-05-18",
          };

          // Add compatibility settings if specified
          if (spec.compatibility_date) {
            metadata.compatibility_date = spec.compatibility_date;
          }
          if (spec.compatibility_flags) {
            metadata.compatibility_flags = spec.compatibility_flags;
          }

          // Add bindings if specified
          if (spec.bindings) {
            const bindings: any[] = [];

            // Handle D1 database bindings
            if (spec.bindings.d1_databases) {
              for (const d1Binding of spec.bindings.d1_databases) {
                const d1Resource = await env.DB.prepare(
                  "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
                )
                  .bind(d1Binding.database_name)
                  .first();

                if (d1Resource && d1Resource.status) {
                  const status = JSON.parse(d1Resource.status);
                  if (status.database_id) {
                    bindings.push({
                      type: "d1",
                      name: d1Binding.binding,
                      id: status.database_id,
                    });
                  }
                }
              }
            }

            // Handle Queue bindings
            if (spec.bindings.queues) {
              for (const queueBinding of spec.bindings.queues) {
                const queueResource = await env.DB.prepare(
                  "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
                )
                  .bind(queueBinding.queue_name)
                  .first();

                if (queueResource && queueResource.status) {
                  const status = JSON.parse(queueResource.status);
                  if (status.queue_id) {
                    const fullQueueName = this.buildFullDatabaseName(
                      queueResource.name,
                      queueResource.group_name,
                      queueResource.plural,
                      queueResource.namespace,
                      env.GUBER_NAME,
                    );
                    bindings.push({
                      type: "queue",
                      name: queueBinding.binding,
                      queue_name: fullQueueName,
                    });
                  }
                }
              }
            }

            if (bindings.length > 0) {
              metadata.bindings = bindings;
            }
          }

          formData.append("metadata", JSON.stringify(metadata));
          formData.append(
            "index.js",
            new Blob([script], { type: "application/javascript+module" }),
            "index.js",
          );

          // Add source map if available
          if (sourceMap) {
            formData.append(
              "index.js.map",
              new Blob([sourceMap], { type: "text/plain" }),
              "index.js.map",
            );
          }

          console.log(`Reconciling worker ${fullName}`);

          const createResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`,
            {
              method: "PUT",
              headers: {
                Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
              },
              body: formData,
            },
          );

          if (createResponse.ok) {
            console.log(
              `Successfully created missing worker script: ${fullName}`,
            );

            // Create custom domain
            const domainResponse = await fetch(
              `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/domains`,
              {
                method: "PUT",
                headers: {
                  Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({
                  hostname: customDomain,
                  service: fullName,
                  environment: "production",
                }),
              },
            );

            let domainId = null;
            if (domainResponse.ok) {
              const domainResult = await domainResponse.json();
              domainId = domainResult.result?.id;
              console.log(
                `Successfully created custom domain: ${customDomain}`,
              );
            } else {
              const domainError = await domainResponse.text();
              console.error(
                `Failed to create custom domain ${customDomain}:`,
                domainError,
              );
            }

            // Update the resource status in our database
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const statusData = {
              state: domainId ? "Ready" : "PartiallyReady",
              worker_id: fullName,
              createdAt: new Date().toISOString(),
              endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`,
              customDomain: customDomain,
              url: `https://${customDomain}`,
              reconciledAt: new Date().toISOString(),
            };

            if (domainId) {
              statusData.domainId = domainId;
            }

            const updateParams = resource.namespace
              ? [JSON.stringify(statusData), resource.name, resource.namespace]
              : [JSON.stringify(statusData), resource.name];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();

            console.log(`Successfully reconciled missing worker: ${fullName}`);
          } else {
            const error = await createResponse.text();
            console.error(
              `Failed to create missing worker ${fullName}:`,
              error,
            );

            // Update status to failed
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = resource.namespace
              ? [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                  resource.namespace,
                ]
              : [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                ];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();
          }
        } catch (error) {
          console.error(`Error creating missing worker ${fullName}:`, error);
        }
      }

      // Check existing workers for binding updates and health
      console.log("Starting worker binding checks and health checks...");
      for (const [fullName, apiResource] of apiWorkerMap) {
        if (cloudflareWorkerMap.has(fullName)) {
          try {
            const spec = JSON.parse(apiResource.spec);
            let status = {};
            try {
              status = apiResource.status ? JSON.parse(apiResource.status) : {};
            } catch (statusParseError) {
              console.error(
                `Failed to parse status for worker ${fullName}:`,
                statusParseError,
              );
              console.error(`Status content:`, apiResource.status);
              status = {};
            }
            const customDomain = `${apiResource.name}.${env.GUBER_NAME}.${env.GUBER_DOMAIN}`;

            // Check if bindings need to be updated
            let needsBindingUpdate = false;
            const expectedBindings: any[] = [];
            let missingResourceIds = false;

            if (spec.bindings) {
              // Build expected bindings from spec
              if (spec.bindings.d1_databases) {
                for (const d1Binding of spec.bindings.d1_databases) {
                  const d1Resource = await env.DB.prepare(
                    "SELECT * FROM resources WHERE name=? AND kind='D1' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
                  )
                    .bind(d1Binding.database_name)
                    .first();

                  if (d1Resource && d1Resource.status) {
                    const d1Status = JSON.parse(d1Resource.status);
                    if (d1Status.database_id) {
                      expectedBindings.push({
                        type: "d1",
                        name: d1Binding.binding,
                        id: d1Status.database_id,
                      });
                    } else {
                      missingResourceIds = true;
                    }
                  } else {
                    missingResourceIds = true;
                  }
                }
              }

              if (spec.bindings.queues) {
                for (const queueBinding of spec.bindings.queues) {
                  const queueResource = await env.DB.prepare(
                    "SELECT * FROM resources WHERE name=? AND kind='Queue' AND group_name='cf.guber.proc.io' AND namespace IS NULL",
                  )
                    .bind(queueBinding.queue_name)
                    .first();

                  if (queueResource && queueResource.status) {
                    const queueStatus = JSON.parse(queueResource.status);
                    if (queueStatus.queue_id) {
                      const fullQueueName = this.buildFullDatabaseName(
                        queueResource.name,
                        queueResource.group_name,
                        queueResource.plural,
                        queueResource.namespace,
                        env.GUBER_NAME,
                      );
                      expectedBindings.push({
                        type: "queue",
                        name: queueBinding.binding,
                        queue_name: fullQueueName,
                      });
                    } else {
                      missingResourceIds = true;
                    }
                  } else {
                    missingResourceIds = true;
                  }
                }
              }

              // Skip binding check if we're missing resource IDs
              if (missingResourceIds) {
                continue;
              }

              // Get current worker metadata to check existing bindings
              const workerResponse = await fetch(
                `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}/settings`,
                {
                  method: "GET",
                  headers: {
                    Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
                  },
                },
              );

              if (workerResponse.ok) {
                let workerData;
                try {
                  const responseText = await workerResponse.text();
                  workerData = JSON.parse(responseText);
                } catch (parseError) {
                  console.error(
                    `Failed to parse worker metadata for ${fullName}:`,
                    parseError,
                  );
                  continue;
                }

                const currentBindings = workerData.result?.bindings || [];

                // Compare expected vs current bindings
                if (expectedBindings.length !== currentBindings.length) {
                  needsBindingUpdate = true;
                } else {
                  // Check if bindings match
                  for (const expectedBinding of expectedBindings) {
                    const matchingBinding = currentBindings.find(
                      (cb: any) =>
                        cb.name === expectedBinding.name &&
                        cb.type === expectedBinding.type &&
                        (expectedBinding.id
                          ? cb.id === expectedBinding.id
                          : true) &&
                        (expectedBinding.queue_name
                          ? cb.queue_name === expectedBinding.queue_name
                          : true),
                    );

                    if (!matchingBinding) {
                      needsBindingUpdate = true;
                      break;
                    }
                  }
                }
              }
            }

            // Update worker if bindings don't match
            if (needsBindingUpdate) {
              console.log(`Updating bindings for worker ${fullName}`);

              // Get the worker script content
              let script: string;
              if (spec.scriptUrl) {
                const scriptResponse = await fetch(spec.scriptUrl, {
                  redirect: "follow",
                  headers: { "User-Agent": "Guber-Worker-Provisioner/1.0" },
                });
                if (scriptResponse.ok) {
                  script = await scriptResponse.text();
                } else {
                  console.error(
                    `Failed to fetch script for ${fullName} from ${spec.scriptUrl}`,
                  );
                  continue;
                }
              } else if (spec.script) {
                script = spec.script;
              } else {
                console.error(`No script source for worker ${fullName}`);
                continue;
              }

              // Create updated worker deployment
              const formData = new FormData();

              const metadata: any = {
                main_module: "index.js",
                compatibility_date: spec.compatibility_date || "2023-05-18",
              };

              if (spec.compatibility_date) {
                metadata.compatibility_date = spec.compatibility_date;
              }
              if (spec.compatibility_flags) {
                metadata.compatibility_flags = spec.compatibility_flags;
              }

              if (expectedBindings.length > 0) {
                metadata.bindings = expectedBindings;
              }

              formData.append("metadata", JSON.stringify(metadata));
              formData.append(
                "index.js",
                new Blob([script], { type: "application/javascript+module" }),
                "index.js",
              );

              const updateResponse = await fetch(
                `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${fullName}`,
                {
                  method: "PUT",
                  headers: {
                    Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
                  },
                  body: formData,
                },
              );

              if (updateResponse.ok) {
                console.log(
                  `Successfully updated bindings for worker ${fullName}`,
                );

                // Update status to reflect binding update
                const newStatus = {
                  ...status,
                  lastBindingUpdate: new Date().toISOString(),
                  bindingsUpdated: true,
                };

                const updateQuery = apiResource.namespace
                  ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
                  : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

                const updateParams = apiResource.namespace
                  ? [
                      JSON.stringify(newStatus),
                      apiResource.name,
                      apiResource.namespace,
                    ]
                  : [JSON.stringify(newStatus), apiResource.name];

                await env.DB.prepare(updateQuery)
                  .bind(...updateParams)
                  .run();
              } else {
                const error = await updateResponse.text();
                console.error(
                  `Failed to update bindings for worker ${fullName}:`,
                  error,
                );
              }
            }

            // Test the worker endpoint for health check
            const healthResponse = await fetch(`https://${customDomain}`, {
              method: "GET",
              headers: {
                "User-Agent": "Guber-Health-Check/1.0",
              },
            });

            const isHealthy = healthResponse.ok;
            const currentState = status.state;

            // Update status if health state changed
            if (
              (isHealthy && currentState === "Failed") ||
              (!isHealthy && currentState === "Ready")
            ) {
              const newStatus = {
                ...status,
                state: isHealthy ? "Ready" : "Failed",
                lastHealthCheck: new Date().toISOString(),
                healthCheckStatus: healthResponse.status,
                healthCheckError: isHealthy
                  ? undefined
                  : `HTTP ${healthResponse.status}: ${healthResponse.statusText}`,
              };

              if (!isHealthy) {
                try {
                  const errorText = await healthResponse.text();
                  if (errorText) {
                    newStatus.healthCheckError = `HTTP ${healthResponse.status}: ${errorText.substring(0, 500)}`;
                  }
                } catch (e) {
                  newStatus.healthCheckError = `HTTP ${healthResponse.status}: Failed to read response body - ${e.message}`;
                }
              }

              const updateQuery = apiResource.namespace
                ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
                : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

              const updateParams = apiResource.namespace
                ? [
                    JSON.stringify(newStatus),
                    apiResource.name,
                    apiResource.namespace,
                  ]
                : [JSON.stringify(newStatus), apiResource.name];

              await env.DB.prepare(updateQuery)
                .bind(...updateParams)
                .run();

              console.log(
                `Updated worker ${fullName} health status: ${currentState} -> ${newStatus.state}`,
              );
            }
          } catch (error) {
            console.error(`Error checking worker ${fullName}:`, error);

            // Update status to indicate check failed
            let status = {};
            try {
              status = apiResource.status ? JSON.parse(apiResource.status) : {};
            } catch (parseError) {
              console.error(
                `Failed to parse existing status for worker ${fullName}:`,
                parseError,
              );
              status = {};
            }

            const newStatus = {
              ...status,
              state: "Failed",
              lastHealthCheck: new Date().toISOString(),
              healthCheckError: `Worker check failed: ${error.message || String(error)}`,
            };

            const updateQuery = apiResource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = apiResource.namespace
              ? [
                  JSON.stringify(newStatus),
                  apiResource.name,
                  apiResource.namespace,
                ]
              : [JSON.stringify(newStatus), apiResource.name];

            try {
              await env.DB.prepare(updateQuery)
                .bind(...updateParams)
                .run();
            } catch (dbError) {
              console.error(
                `Failed to update status for worker ${fullName}:`,
                dbError,
              );
            }
          }
        }
      }

      console.log("Worker reconciliation completed");
    } catch (error) {
      console.error("Error during Worker reconciliation:", error);
    }
  }

  private async reconcilePendingWorkers(env: any) {
    console.log("Checking pending workers for dependency resolution...");

    const { results: pendingWorkers } = await env.DB.prepare(
      `
      SELECT * FROM resources 
      WHERE group_name='cf.guber.proc.io' 
      AND kind='Worker' 
      AND json_extract(status, '$.state') = 'Pending'
    `,
    ).all();

    for (const worker of pendingWorkers || []) {
      try {
        const spec = JSON.parse(worker.spec);
        const status = JSON.parse(worker.status);

        if (spec.dependencies && status.pendingDependencies) {
          let allDependenciesReady = true;
          const unresolvedDependencies = [];

          for (const dependency of spec.dependencies) {
            const depGroup = dependency.group || "cf.guber.proc.io";
            const depResource = await env.DB.prepare(
              "SELECT * FROM resources WHERE name=? AND kind=? AND group_name=? AND namespace IS NULL",
            )
              .bind(dependency.name, dependency.kind, depGroup)
              .first();

            if (!depResource || !depResource.status) {
              allDependenciesReady = false;
              unresolvedDependencies.push(dependency);
              continue;
            }

            const depStatus = JSON.parse(depResource.status);
            if (depStatus.state !== "Ready") {
              allDependenciesReady = false;
              unresolvedDependencies.push(dependency);
            }
          }

          if (allDependenciesReady) {
            console.log(
              `[Reconcile] All dependencies resolved for worker ${worker.name}, re-queuing for provisioning`,
            );

            // Queue for provisioning
            if (env.GUBER_BUS) {
              await env.GUBER_BUS.send({
                action: "create",
                resourceType: "worker",
                resourceName: worker.name,
                group: worker.group_name,
                kind: worker.kind,
                plural: worker.plural,
                namespace: worker.namespace,
                spec: spec,
              });
            }
          } else {
            console.log(
              `[Reconcile] Worker ${worker.name} still has unresolved dependencies:`,
              unresolvedDependencies.map((d) => `${d.kind}/${d.name}`),
            );

            // Update the dependency check timestamp
            const updatedStatus = {
              ...status,
              lastDependencyCheck: new Date().toISOString(),
              pendingDependencies: unresolvedDependencies,
            };

            await env.DB.prepare(
              "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
            )
              .bind(JSON.stringify(updatedStatus), worker.name)
              .run();
          }
        }
      } catch (error) {
        console.error(
          `[Reconcile] Error checking dependencies for worker ${worker.name}:`,
          error,
        );
      }
    }
  }

  private async reconcileD1Databases(env: any) {
    try {
      console.log("Starting D1 database reconciliation...");

      // Get all D1 resources from our API
      const { results: apiResources } = await env.DB.prepare(
        "SELECT * FROM resources WHERE group_name='cf.guber.proc.io' AND kind='D1'",
      ).all();

      // Get all D1 databases from Cloudflare
      const cloudflareResponse = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
          },
        },
      );

      if (!cloudflareResponse.ok) {
        console.error(
          "Failed to fetch D1 databases from Cloudflare:",
          await cloudflareResponse.text(),
        );
        return;
      }

      const cloudflareResult = await cloudflareResponse.json();
      const cloudflareDatabases = cloudflareResult.result || [];

      // Create maps for easier comparison
      const apiDatabaseMap = new Map();
      const cloudflareDatabaseMap = new Map();

      // Build API database map with full names
      for (const resource of apiResources || []) {
        const fullDatabaseName = this.buildFullDatabaseName(
          resource.name,
          resource.group_name,
          resource.plural,
          resource.namespace,
          env.GUBER_NAME,
        );
        apiDatabaseMap.set(fullDatabaseName, resource);
      }

      // Build Cloudflare database map
      for (const db of cloudflareDatabases) {
        cloudflareDatabaseMap.set(db.name, db);
      }

      console.log(
        `Found ${apiDatabaseMap.size} D1 resources in API and ${cloudflareDatabaseMap.size} databases in Cloudflare`,
      );

      // Find databases that exist in Cloudflare but not in our API (orphaned databases)
      const orphanedDatabases = [];
      for (const [dbName, cloudflareDb] of cloudflareDatabaseMap) {
        // Only consider databases that match our naming pattern
        if (
          dbName.includes("-") &&
          (dbName.includes("-d1s-cf-guber-proc-io") ||
            dbName.includes("-d1-cf-guber-proc-io"))
        ) {
          if (!apiDatabaseMap.has(dbName)) {
            orphanedDatabases.push(cloudflareDb);
          }
        }
      }

      // Find resources that exist in our API but not in Cloudflare (missing databases)
      const missingDatabases = [];
      for (const [fullName, apiResource] of apiDatabaseMap) {
        if (!cloudflareDatabaseMap.has(fullName)) {
          missingDatabases.push({ fullName, resource: apiResource });
        }
      }

      console.log(
        `Found ${orphanedDatabases.length} orphaned databases and ${missingDatabases.length} missing databases`,
      );

      // Delete orphaned databases from Cloudflare
      for (const orphanedDb of orphanedDatabases) {
        try {
          console.log(
            `Deleting orphaned database: ${orphanedDb.name} (ID: ${orphanedDb.uuid})`,
          );

          const deleteResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${orphanedDb.uuid}`,
            {
              method: "DELETE",
              headers: {
                Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
              },
            },
          );

          if (deleteResponse.ok) {
            console.log(
              `Successfully deleted orphaned database: ${orphanedDb.name}`,
            );
          } else {
            const error = await deleteResponse.text();
            console.error(
              `Failed to delete orphaned database ${orphanedDb.name}:`,
              error,
            );
          }
        } catch (error) {
          console.error(
            `Error deleting orphaned database ${orphanedDb.name}:`,
            error,
          );
        }
      }

      // Create missing databases in Cloudflare
      for (const { fullName, resource } of missingDatabases) {
        try {
          console.log(`Creating missing database: ${fullName}`);

          const spec = JSON.parse(resource.spec);
          const requestBody: any = { name: fullName };

          if (spec.location) {
            requestBody.primary_location_hint = spec.location;
          }

          const createResponse = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${env.CLOUDFLARE_API_TOKEN}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify(requestBody),
            },
          );

          if (createResponse.ok) {
            const result = await createResponse.json();
            const databaseId = result.result.uuid;

            // Update the resource status in our database
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = resource.namespace
              ? [
                  JSON.stringify({
                    state: "Ready",
                    database_id: databaseId,
                    createdAt: new Date().toISOString(),
                    endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                  resource.namespace,
                ]
              : [
                  JSON.stringify({
                    state: "Ready",
                    database_id: databaseId,
                    createdAt: new Date().toISOString(),
                    endpoint: `https://api.cloudflare.com/client/v4/accounts/${env.CLOUDFLARE_ACCOUNT_ID}/d1/database/${databaseId}`,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                ];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();

            console.log(
              `Successfully created missing database: ${fullName} with ID: ${databaseId}`,
            );
          } else {
            const error = await createResponse.text();
            console.error(
              `Failed to create missing database ${fullName}:`,
              error,
            );

            // Update status to failed
            const updateQuery = resource.namespace
              ? "UPDATE resources SET status=? WHERE name=? AND namespace=?"
              : "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL";

            const updateParams = resource.namespace
              ? [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                  resource.namespace,
                ]
              : [
                  JSON.stringify({
                    state: "Failed",
                    error: error,
                    reconciledAt: new Date().toISOString(),
                  }),
                  resource.name,
                ];

            await env.DB.prepare(updateQuery)
              .bind(...updateParams)
              .run();
          }
        } catch (error) {
          console.error(`Error creating missing database ${fullName}:`, error);
        }
      }

      console.log("D1 database reconciliation completed");
    } catch (error) {
      console.error("Error during D1 database reconciliation:", error);
    }
  }
}

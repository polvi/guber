import { Hono } from "hono";
import { v4 as uuid } from "uuid";
import type { Controller, ResourceContext } from "../config";

export default function github(): Controller {
  return new GitHubController();
}

class GitHubController implements Controller {

  async onResourceCreated(context: ResourceContext): Promise<void> {
    const { group, kind, name, spec, env } = context;

    // Only handle gh.guber.proc.io resources
    if (group !== "gh.guber.proc.io") return;

    // Queue for provisioning if it's a GitHub resource type
    if (
      kind === "ReleaseDeploy" &&
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

    // Only handle gh.guber.proc.io resources
    if (group !== "gh.guber.proc.io") return;

    // Queue for deletion if it's a GitHub resource type
    if (
      kind === "ReleaseDeploy" &&
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

          if (resourceType === "releasedeploy") {
            provisioningSuccessful = await this.provisionReleaseDeploy(
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
          if (resourceType === "releasedeploy") {
            await this.deleteReleaseDeploy(
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
      `Running GitHub resource reconciliation at ${new Date(event.scheduledTime).toISOString()}`,
    );
    await this.reconcileReleaseDeploys(env);
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
      WHERE group_name='gh.guber.proc.io' 
      AND kind='ReleaseDeploy'
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
              (dep.group || "gh.guber.proc.io") === resolvedGroup,
          );

          if (hasDependency) {
            console.log(
              `Found dependent resource ${resource.name}, checking if all dependencies are now ready`,
            );

            // Check if ALL dependencies are now ready
            let allDependenciesReady = true;
            const unresolvedDependencies = [];

            for (const dependency of spec.dependencies) {
              const depGroup = dependency.group || "gh.guber.proc.io";
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
                `✅ All dependencies resolved for ${resource.kind.toLowerCase()} ${resource.name}, re-queuing for provisioning`,
              );

              // Queue for provisioning
              if (env.GUBER_BUS) {
                await env.GUBER_BUS.send({
                  action: "create",
                  resourceType: resource.kind.toLowerCase(),
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
                `⏳ ${resource.kind} ${resource.name} still has unresolved dependencies:`,
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

  private async provisionReleaseDeploy(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
  ): Promise<boolean> {
    try {
      // Check dependencies first
      if (spec.dependencies && spec.dependencies.length > 0) {
        console.log(
          `Checking ${spec.dependencies.length} dependencies for release deploy ${resourceName}`,
        );

        for (const dependency of spec.dependencies) {
          const depGroup = dependency.group || "gh.guber.proc.io";
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

        console.log(`All dependencies satisfied for release deploy ${resourceName}`);
      }

      // Validate required fields
      if (!spec.repository) {
        throw new Error("ReleaseDeploy must have 'repository' specified");
      }

      if (!env.GITHUB_TOKEN) {
        throw new Error("GITHUB_TOKEN environment variable is required");
      }

      // Get the release tag - either specified or latest
      let releaseTag = spec.tag;
      let releaseData = null;

      if (!releaseTag) {
        console.log(`Fetching latest release for ${spec.repository}`);
        
        const latestReleaseResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/releases/latest`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${env.GITHUB_TOKEN}`,
              "Accept": "application/vnd.github.v3+json",
              "User-Agent": "Guber-GitHub-Controller/1.0",
            },
          },
        );

        if (!latestReleaseResponse.ok) {
          const errorResponse = await latestReleaseResponse.json();
          throw new Error(
            `Failed to fetch latest release: ${JSON.stringify(errorResponse)}`,
          );
        }

        releaseData = await latestReleaseResponse.json();
        releaseTag = releaseData.tag_name;
        console.log(`Using latest release tag: ${releaseTag}`);
      } else {
        // Fetch specific release data
        console.log(`Fetching release data for tag ${releaseTag}`);
        
        const releaseResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/releases/tags/${releaseTag}`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${env.GITHUB_TOKEN}`,
              "Accept": "application/vnd.github.v3+json",
              "User-Agent": "Guber-GitHub-Controller/1.0",
            },
          },
        );

        if (releaseResponse.ok) {
          releaseData = await releaseResponse.json();
        }
      }

      // Create GitHub deployment
      const deploymentPayload: any = {
        ref: releaseTag,
        environment: spec.environment || "production",
        description: spec.description || `Deploy ${releaseTag} to ${spec.environment || "production"}`,
        auto_merge: spec.autoMerge || false,
        required_contexts: spec.requiredContexts || [],
      };

      // Add payload if specified
      if (spec.payload) {
        deploymentPayload.payload = spec.payload;
      }

      console.log(
        `Creating GitHub deployment for ${spec.repository} tag ${releaseTag}`,
      );

      const deploymentResponse = await fetch(
        `https://api.github.com/repos/${spec.repository}/deployments`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${env.GITHUB_TOKEN}`,
            "Content-Type": "application/json",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Guber-GitHub-Controller/1.0",
          },
          body: JSON.stringify(deploymentPayload),
        },
      );

      if (!deploymentResponse.ok) {
        const errorResponse = await deploymentResponse.json();
        throw new Error(
          `Failed to create GitHub deployment: ${JSON.stringify(errorResponse)}`,
        );
      }

      const deploymentResult = await deploymentResponse.json();
      const deploymentId = deploymentResult.id;

      console.log(
        `GitHub deployment created successfully with ID: ${deploymentId}`,
      );

      // Create WorkerScriptVersion if requested
      let workerScriptVersionName = null;
      if (spec.createWorkerScriptVersion && spec.workerScriptVersionSpec) {
        try {
          workerScriptVersionName = await this.createWorkerScriptVersion(
            env,
            spec,
            releaseData,
            releaseTag,
            resourceName,
          );
        } catch (error) {
          console.error(
            `Failed to create WorkerScriptVersion for ${resourceName}:`,
            error,
          );
          // Don't fail the entire deployment if WorkerScriptVersion creation fails
        }
      }

      // Create deployment status if specified
      let statusId = null;
      if (spec.initialStatus) {
        const statusPayload = {
          state: spec.initialStatus.state || "pending",
          target_url: spec.initialStatus.targetUrl,
          log_url: spec.initialStatus.logUrl,
          description: spec.initialStatus.description || "Deployment started",
          environment: spec.environment || "production",
          environment_url: spec.initialStatus.environmentUrl,
        };

        const statusResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/deployments/${deploymentId}/statuses`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${env.GITHUB_TOKEN}`,
              "Content-Type": "application/json",
              "Accept": "application/vnd.github.v3+json",
              "User-Agent": "Guber-GitHub-Controller/1.0",
            },
            body: JSON.stringify(statusPayload),
          },
        );

        if (statusResponse.ok) {
          const statusResult = await statusResponse.json();
          statusId = statusResult.id;
          console.log(
            `GitHub deployment status created successfully with ID: ${statusId}`,
          );
        } else {
          const statusError = await statusResponse.json();
          console.error(
            `Failed to create deployment status: ${JSON.stringify(statusError)}`,
          );
        }
      }

      // Update the resource status in the database
      const statusUpdate: any = {
        state: "Ready",
        deployment_id: deploymentId,
        status_id: statusId,
        repository: spec.repository,
        tag: releaseTag,
        environment: spec.environment || "production",
        createdAt: new Date().toISOString(),
        endpoint: `https://api.github.com/repos/${spec.repository}/deployments/${deploymentId}`,
        url: `https://github.com/${spec.repository}/deployments`,
      };

      if (workerScriptVersionName) {
        statusUpdate.workerScriptVersionName = workerScriptVersionName;
      }

      await env.DB.prepare(
        "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
      )
        .bind(JSON.stringify(statusUpdate), resourceName)
        .run();

      console.log(
        `ReleaseDeploy ${resourceName} provisioned successfully`,
      );
      return true;
    } catch (error) {
      console.error(`Failed to provision ReleaseDeploy ${resourceName}:`, error);

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

  private async deleteReleaseDeploy(
    env: any,
    resourceName: string,
    group: string,
    kind: string,
    plural: string,
    namespace: string | null,
    spec: any,
    status?: any,
  ) {
    try {
      // Get deployment ID from the passed status
      const deploymentId = status?.deployment_id;

      if (deploymentId && spec.repository) {
        console.log(
          `Marking GitHub deployment ${resourceName} (ID: ${deploymentId}) as inactive`,
        );

        // GitHub doesn't allow deleting deployments, but we can mark them as inactive
        const statusPayload = {
          state: "inactive",
          description: "Deployment marked as inactive by Guber",
        };

        const statusResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/deployments/${deploymentId}/statuses`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${env.GITHUB_TOKEN}`,
              "Content-Type": "application/json",
              "Accept": "application/vnd.github.v3+json",
              "User-Agent": "Guber-GitHub-Controller/1.0",
            },
            body: JSON.stringify(statusPayload),
          },
        );

        if (statusResponse.ok) {
          console.log(
            `GitHub deployment ${resourceName} (ID: ${deploymentId}) marked as inactive`,
          );
        } else {
          const error = await statusResponse.text();
          console.error(
            `Failed to mark GitHub deployment ${resourceName} (ID: ${deploymentId}) as inactive:`,
            error,
          );
        }
      } else {
        console.log(
          `No deployment ID or repository found for ${resourceName}, skipping GitHub cleanup`,
        );
      }
    } catch (error) {
      console.error(`Error deleting ReleaseDeploy ${resourceName}:`, error);
    }
  }

  private async reconcileReleaseDeploys(env: any) {
    try {
      console.log("Starting ReleaseDeploy reconciliation...");

      // First, check pending release deploys for dependency resolution
      await this.reconcilePendingReleaseDeploys(env);

      // Get all ReleaseDeploy resources from our API
      const { results: apiResources } = await env.DB.prepare(
        "SELECT * FROM resources WHERE group_name='gh.guber.proc.io' AND kind='ReleaseDeploy'",
      ).all();

      console.log(
        `Found ${apiResources?.length || 0} ReleaseDeploy resources in API`,
      );

      // Check each deployment resource for health
      for (const resource of apiResources || []) {
        try {
          const spec = JSON.parse(resource.spec);
          let status = {};
          try {
            status = resource.status ? JSON.parse(resource.status) : {};
          } catch (statusParseError) {
            console.error(
              `Failed to parse status for release deploy ${resource.name}:`,
              statusParseError,
            );
            status = {};
          }

          // For ready deployments, verify they still exist in GitHub
          if (
            status.state === "Ready" &&
            status.deployment_id &&
            status.endpoint
          ) {
            try {
              const deploymentResponse = await fetch(status.endpoint, {
                method: "GET",
                headers: {
                  Authorization: `Bearer ${env.GITHUB_TOKEN}`,
                  "Accept": "application/vnd.github.v3+json",
                  "User-Agent": "Guber-GitHub-Controller/1.0",
                },
              });

              if (!deploymentResponse.ok) {
                console.log(
                  `GitHub deployment ${resource.name} (ID: ${status.deployment_id}) no longer exists`,
                );

                // Update status to indicate the deployment is missing
                const updatedStatus = {
                  ...status,
                  state: "Failed",
                  error: "Deployment no longer exists in GitHub",
                  lastHealthCheck: new Date().toISOString(),
                };

                await env.DB.prepare(
                  "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
                )
                  .bind(JSON.stringify(updatedStatus), resource.name)
                  .run();
              } else {
                // Update last health check timestamp
                const updatedStatus = {
                  ...status,
                  lastHealthCheck: new Date().toISOString(),
                };

                await env.DB.prepare(
                  "UPDATE resources SET status=? WHERE name=? AND namespace IS NULL",
                )
                  .bind(JSON.stringify(updatedStatus), resource.name)
                  .run();
              }
            } catch (error) {
              console.error(
                `Error checking GitHub deployment ${resource.name}:`,
                error,
              );
            }
          }
        } catch (error) {
          console.error(
            `Error processing release deploy ${resource.name}:`,
            error,
          );
        }
      }

      console.log("ReleaseDeploy reconciliation completed");
    } catch (error) {
      console.error("Error during ReleaseDeploy reconciliation:", error);
    }
  }

  private async createWorkerScriptVersion(
    env: any,
    spec: any,
    releaseData: any,
    releaseTag: string,
    releaseDeployName: string,
  ): Promise<string> {
    const workerScriptVersionSpec = spec.workerScriptVersionSpec;
    
    if (!workerScriptVersionSpec.workerName) {
      throw new Error("workerScriptVersionSpec.workerName is required when createWorkerScriptVersion is true");
    }

    // Generate a unique name for the WorkerScriptVersion
    const workerScriptVersionName = `${workerScriptVersionSpec.workerName}-${releaseTag.replace(/[^a-zA-Z0-9]/g, '-').toLowerCase()}`;

    // Get script content from release if not provided
    let scriptContent = workerScriptVersionSpec.script;
    
    if (!scriptContent && releaseData) {
      // Look for common script files in release assets
      const scriptAssets = releaseData.assets?.filter((asset: any) => 
        asset.name.endsWith('.js') || 
        asset.name.endsWith('.mjs') || 
        asset.name === 'worker.js' ||
        asset.name === 'index.js'
      );

      if (scriptAssets && scriptAssets.length > 0) {
        // Use the first matching asset
        const scriptAsset = scriptAssets[0];
        console.log(`Downloading script from release asset: ${scriptAsset.name}`);

        const scriptResponse = await fetch(scriptAsset.browser_download_url, {
          headers: {
            Authorization: `Bearer ${env.GITHUB_TOKEN}`,
            "User-Agent": "Guber-GitHub-Controller/1.0",
          },
        });

        if (scriptResponse.ok) {
          scriptContent = await scriptResponse.text();
        } else {
          console.warn(`Failed to download script asset ${scriptAsset.name}, will use empty script`);
          scriptContent = "// Script content could not be fetched from release";
        }
      } else {
        // Try to get the main branch content as fallback
        console.log("No script assets found in release, trying to fetch from repository");
        
        const repoContentResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/contents/worker.js`,
          {
            headers: {
              Authorization: `Bearer ${env.GITHUB_TOKEN}`,
              "Accept": "application/vnd.github.v3+json",
              "User-Agent": "Guber-GitHub-Controller/1.0",
            },
          },
        );

        if (repoContentResponse.ok) {
          const contentData = await repoContentResponse.json();
          if (contentData.content) {
            scriptContent = atob(contentData.content);
          }
        }

        if (!scriptContent) {
          scriptContent = "// Script content could not be fetched";
        }
      }
    }

    // Create the WorkerScriptVersion resource
    const workerScriptVersionResource = {
      name: workerScriptVersionName,
      namespace: null,
      group_name: "cf.guber.proc.io",
      kind: "WorkerScriptVersion",
      plural: "workerscriptversions",
      spec: JSON.stringify({
        workerName: workerScriptVersionSpec.workerName,
        script: scriptContent,
        metadata: {
          ...workerScriptVersionSpec.metadata,
          sourceRepository: spec.repository,
          sourceTag: releaseTag,
          createdBy: `ReleaseDeploy/${releaseDeployName}`,
          releaseUrl: releaseData?.html_url,
        },
      }),
      status: JSON.stringify({
        state: "Pending",
        message: `Created from ReleaseDeploy ${releaseDeployName}`,
      }),
    };

    // Insert the WorkerScriptVersion into the database
    await env.DB.prepare(
      `INSERT INTO resources (name, namespace, group_name, kind, plural, spec, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
    )
      .bind(
        workerScriptVersionResource.name,
        workerScriptVersionResource.namespace,
        workerScriptVersionResource.group_name,
        workerScriptVersionResource.kind,
        workerScriptVersionResource.plural,
        workerScriptVersionResource.spec,
        workerScriptVersionResource.status,
      )
      .run();

    console.log(
      `Created WorkerScriptVersion ${workerScriptVersionName} from ReleaseDeploy ${releaseDeployName}`,
    );

    // Queue the WorkerScriptVersion for provisioning by the Cloudflare controller
    if (env.GUBER_BUS) {
      await env.GUBER_BUS.send({
        action: "create",
        resourceType: "workerscriptversion",
        resourceName: workerScriptVersionName,
        group: "cf.guber.proc.io",
        kind: "WorkerScriptVersion",
        plural: "workerscriptversions",
        namespace: null,
        spec: JSON.parse(workerScriptVersionResource.spec),
      });
    }

    return workerScriptVersionName;
  }

  private async reconcilePendingReleaseDeploys(env: any) {
    console.log("Checking pending release deploys for dependency resolution...");

    const { results: pendingDeploys } = await env.DB.prepare(
      `
      SELECT * FROM resources 
      WHERE group_name='gh.guber.proc.io' 
      AND kind='ReleaseDeploy' 
      AND json_extract(status, '$.state') = 'Pending'
    `,
    ).all();

    for (const deploy of pendingDeploys || []) {
      try {
        const spec = JSON.parse(deploy.spec);
        const status = JSON.parse(deploy.status);

        if (spec.dependencies && status.pendingDependencies) {
          let allDependenciesReady = true;
          const unresolvedDependencies = [];

          for (const dependency of spec.dependencies) {
            const depGroup = dependency.group || "gh.guber.proc.io";
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
              `[Reconcile] All dependencies resolved for release deploy ${deploy.name}, re-queuing for provisioning`,
            );

            // Queue for provisioning
            if (env.GUBER_BUS) {
              await env.GUBER_BUS.send({
                action: "create",
                resourceType: "releasedeploy",
                resourceName: deploy.name,
                group: deploy.group_name,
                kind: deploy.kind,
                plural: deploy.plural,
                namespace: deploy.namespace,
                spec: spec,
              });
            }
          } else {
            console.log(
              `[Reconcile] Release deploy ${deploy.name} still has unresolved dependencies:`,
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
              .bind(JSON.stringify(updatedStatus), deploy.name)
              .run();
          }
        }
      } catch (error) {
        console.error(
          `[Reconcile] Error checking dependencies for release deploy ${deploy.name}:`,
          error,
        );
      }
    }
  }
}

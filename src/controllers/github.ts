import { Hono } from "hono";
import { v4 as uuid } from "uuid";
import type { Controller, ResourceContext } from "../config";
import type { WorkerScriptVersion, Worker, WorkerScriptDeployment } from "../client/gen/cloudflare/models";
import { 
  patchApisCfGuberProcIoV1WorkerscriptversionsName,
  patchApisCfGuberProcIoV1WorkersName,
  patchApisCfGuberProcIoV1WorkerscriptdeploymentsName
} from "../client/gen/cloudflare/default/default";
import { patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName } from "../client/gen/github/default/default";
import type { ReleaseDeploy } from "../client/gen/github/models";
import { setEnv } from "../client/custom-fetch";

export default function github(): Controller {
  return new GitHubController();
}

export class GitHubController implements Controller {
  async onResourceCreated(context: ResourceContext): Promise<void> {
    const { group, kind, name, spec, env } = context;

    // Set the environment for the generated client
    setEnv(env);

    // Only handle gh.guber.proc.io resources
    if (group !== "gh.guber.proc.io") return;

    // Queue for provisioning if it's a GitHub resource type
    if (kind === "ReleaseDeploy" && env.GUBER_BUS) {
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

    // Set the environment for the generated client
    setEnv(env);

    // Only handle gh.guber.proc.io resources
    if (group !== "gh.guber.proc.io") return;

    // Queue for deletion if it's a GitHub resource type
    if (kind === "ReleaseDeploy" && env.GUBER_BUS) {
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
    // Set the environment for the generated client
    setEnv(env);
    
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
    // Set the environment for the generated client
    setEnv(env);
    
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

              // Update status using the generated GitHub client
              const releaseDeployUpdate: ReleaseDeploy = {
                apiVersion: "gh.guber.proc.io/v1",
                kind: "ReleaseDeploy",
                metadata: {
                  name: resource.name,
                  namespace: resource.namespace || undefined,
                },
                status: updatedStatus,
              };

              await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
                "default",
                resource.name,
                releaseDeployUpdate,
              );
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
            const pendingStatus = {
              state: "Pending",
              message: `Waiting for dependency: ${depKind}/${depName}`,
              pendingDependencies: [dependency],
            };

            // Update status using the generated GitHub client
            const releaseDeployUpdate: ReleaseDeploy = {
              apiVersion: "gh.guber.proc.io/v1",
              kind: "ReleaseDeploy",
              metadata: {
                name: resourceName,
                namespace: namespace || undefined,
              },
              status: pendingStatus,
            };

            await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
              namespace || "default",
              resourceName,
              releaseDeployUpdate,
            );
            return false;
          }

          if (!depResource.status) {
            console.log(
              `Dependency ${depKind}/${depName} has no status, deferring provisioning`,
            );
            const pendingStatus = {
              state: "Pending",
              message: `Waiting for dependency to be provisioned: ${depKind}/${depName}`,
              pendingDependencies: [dependency],
            };

            // Update status using the generated GitHub client
            const releaseDeployUpdate: ReleaseDeploy = {
              apiVersion: "gh.guber.proc.io/v1",
              kind: "ReleaseDeploy",
              metadata: {
                name: resourceName,
                namespace: namespace || undefined,
              },
              status: pendingStatus,
            };

            await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
              namespace || "default",
              resourceName,
              releaseDeployUpdate,
            );
            return false;
          }

          const depStatus = JSON.parse(depResource.status);
          if (depStatus.state !== "Ready") {
            console.log(
              `Dependency ${depKind}/${depName} not ready (${depStatus.state}), deferring provisioning`,
            );
            const pendingStatus = {
              state: "Pending",
              message: `Waiting for dependency to be ready: ${depKind}/${depName} (current state: ${depStatus.state})`,
              pendingDependencies: [dependency],
            };

            // Update status using the generated GitHub client
            const releaseDeployUpdate: ReleaseDeploy = {
              apiVersion: "gh.guber.proc.io/v1",
              kind: "ReleaseDeploy",
              metadata: {
                name: resourceName,
                namespace: namespace || undefined,
              },
              status: pendingStatus,
            };

            await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
              namespace || "default",
              resourceName,
              releaseDeployUpdate,
            );
            return false;
          }
        }

        console.log(
          `All dependencies satisfied for release deploy ${resourceName}`,
        );
      }

      // Validate required fields
      if (!spec.repository) {
        throw new Error("ReleaseDeploy must have 'repository' specified");
      }

      // Get the release tag - either specified or latest
      let releaseTag = spec.tag;
      let releaseData = null;

      if (!releaseTag) {
        console.log(
          `🔍 Fetching latest release for repository: ${spec.repository}`,
        );

        const headers: Record<string, string> = {
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "Guber-GitHub-Controller/1.0",
        };

        if (env.GITHUB_TOKEN) {
          headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
        }

        const latestReleaseResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/releases/latest`,
          {
            method: "GET",
            headers,
          },
        );

        if (!latestReleaseResponse.ok) {
          const errorResponse = await latestReleaseResponse.json();

          // If unauthorized and no token provided, throw specific error
          if (latestReleaseResponse.status === 401 && !env.GITHUB_TOKEN) {
            throw new Error(
              "GITHUB_TOKEN environment variable is required for private repositories or when rate limited",
            );
          }

          console.error(
            `❌ Failed to fetch latest release for ${spec.repository}:`,
            errorResponse,
          );
          throw new Error(
            `Failed to fetch latest release: ${JSON.stringify(errorResponse)}`,
          );
        }

        releaseData = await latestReleaseResponse.json();
        releaseTag = releaseData.tag_name;
        console.log(
          `✅ Found latest release for ${spec.repository}: ${releaseTag} (published: ${releaseData.published_at})`,
        );

        if (releaseData.prerelease) {
          console.log(
            `⚠️  Note: Release ${releaseTag} is marked as prerelease`,
          );
        }
        if (releaseData.draft) {
          console.log(`⚠️  Note: Release ${releaseTag} is marked as draft`);
        }
      } else {
        // Fetch specific release data
        console.log(
          `🔍 Fetching release data for specific tag: ${releaseTag} from ${spec.repository}`,
        );

        const headers: Record<string, string> = {
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "Guber-GitHub-Controller/1.0",
        };

        if (env.GITHUB_TOKEN) {
          headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
        }

        const releaseResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/releases/tags/${releaseTag}`,
          {
            method: "GET",
            headers,
          },
        );

        if (releaseResponse.ok) {
          releaseData = await releaseResponse.json();
          console.log(
            `✅ Found release data for ${releaseTag}: ${releaseData.name || releaseTag} (published: ${releaseData.published_at})`,
          );

          if (releaseData.prerelease) {
            console.log(
              `⚠️  Note: Release ${releaseTag} is marked as prerelease`,
            );
          }
          if (releaseData.draft) {
            console.log(`⚠️  Note: Release ${releaseTag} is marked as draft`);
          }
        } else {
          // If unauthorized and no token provided, throw specific error
          if (releaseResponse.status === 401 && !env.GITHUB_TOKEN) {
            throw new Error(
              "GITHUB_TOKEN environment variable is required for private repositories or when rate limited",
            );
          }

          console.warn(
            `⚠️  Could not fetch release data for tag ${releaseTag} from ${spec.repository}`,
          );
        }
      }

      // Create GitHub deployment (only if token is available)
      let deploymentId = null;
      let deploymentUrl = null;

      if (env.GITHUB_TOKEN) {
        const deploymentPayload: any = {
          ref: releaseTag,
          environment: spec.environment || "production",
          description:
            spec.description ||
            `Deploy ${releaseTag} to ${spec.environment || "production"}`,
          auto_merge: spec.autoMerge || false,
          required_contexts: spec.requiredContexts || [],
        };

        // Add payload if specified
        if (spec.payload) {
          deploymentPayload.payload = spec.payload;
        }

        console.log(
          `🚀 Creating GitHub deployment for ${spec.repository} tag ${releaseTag} to environment: ${spec.environment || "production"}`,
        );

        if (releaseData) {
          console.log(
            `📦 Release details: ${releaseData.name || releaseTag} - ${releaseData.body ? releaseData.body.substring(0, 100) + "..." : "No description"}`,
          );
          if (releaseData.assets && releaseData.assets.length > 0) {
            console.log(
              `📎 Release has ${releaseData.assets.length} assets: ${releaseData.assets.map((a) => a.name).join(", ")}`,
            );
          }
        }

        const headers: Record<string, string> = {
          "Content-Type": "application/json",
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "Guber-GitHub-Controller/1.0",
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        };

        const deploymentResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/deployments`,
          {
            method: "POST",
            headers,
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
        deploymentId = deploymentResult.id;
        deploymentUrl = `https://github.com/${spec.repository}/deployments`;

        console.log(
          `✅ GitHub deployment created successfully with ID: ${deploymentId} for ${spec.repository}@${releaseTag}`,
        );
        console.log(`🔗 Deployment URL: ${deploymentUrl}`);
      } else {
        console.log(
          `⚠️  Skipping GitHub deployment creation for ${spec.repository}@${releaseTag} - GITHUB_TOKEN not provided`,
        );
        console.log(
          `📦 Release details: ${releaseData?.name || releaseTag} - ${releaseData?.body ? releaseData.body.substring(0, 100) + "..." : "No description"}`,
        );
        if (releaseData?.assets && releaseData.assets.length > 0) {
          console.log(
            `📎 Release has ${releaseData.assets.length} assets: ${releaseData.assets.map((a) => a.name).join(", ")}`,
          );
        }
      }

      // Create Cloudflare resources if requested
      let workerName = null;
      let workerScriptVersionName = null;
      let workerScriptDeploymentName = null;

      if (spec.createCloudflareResources) {
        try {
          // Create Worker first
          workerName = await this.createWorker(env, spec, resourceName, releaseTag);
          
          // Create WorkerScriptVersion
          workerScriptVersionName = await this.createWorkerScriptVersion(
            env,
            spec,
            releaseData,
            releaseTag,
            resourceName,
            workerName,
          );
          
          // Create WorkerScriptDeployment
          workerScriptDeploymentName = await this.createWorkerScriptDeployment(
            env,
            spec,
            resourceName,
            releaseTag,
            workerName,
            workerScriptVersionName,
          );
        } catch (error) {
          console.error(
            `Failed to create Cloudflare resources for ${resourceName}:`,
            error,
          );
          // Don't fail the entire deployment if Cloudflare resource creation fails
        }
      }

      // Create deployment status if specified and deployment was created
      let statusId = null;
      if (spec.initialStatus && deploymentId && env.GITHUB_TOKEN) {
        const statusPayload = {
          state: spec.initialStatus.state || "pending",
          target_url: spec.initialStatus.targetUrl,
          log_url: spec.initialStatus.logUrl,
          description: spec.initialStatus.description || "Deployment started",
          environment: spec.environment || "production",
          environment_url: spec.initialStatus.environmentUrl,
        };

        const headers: Record<string, string> = {
          "Content-Type": "application/json",
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "Guber-GitHub-Controller/1.0",
          Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        };

        const statusResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/deployments/${deploymentId}/statuses`,
          {
            method: "POST",
            headers,
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

      // Update the resource status via HTTP API
      const statusUpdate: any = {
        state: "Ready",
        repository: spec.repository,
        tag: releaseTag,
        environment: spec.environment || "production",
        createdAt: new Date().toISOString(),
        releaseUrl: releaseData?.html_url,
      };

      if (deploymentId) {
        statusUpdate.deployment_id = deploymentId;
        statusUpdate.endpoint = `https://api.github.com/repos/${spec.repository}/deployments/${deploymentId}`;
        statusUpdate.url = deploymentUrl;
      }

      if (statusId) {
        statusUpdate.status_id = statusId;
      }

      if (workerName) {
        statusUpdate.workerName = workerName;
      }

      if (workerScriptVersionName) {
        statusUpdate.workerScriptVersionName = workerScriptVersionName;
      }

      if (workerScriptDeploymentName) {
        statusUpdate.workerScriptDeploymentName = workerScriptDeploymentName;
      }

      if (!env.GITHUB_TOKEN) {
        statusUpdate.note =
          "GitHub deployment not created - GITHUB_TOKEN not provided";
      }

      // Update status using the generated GitHub client
      const releaseDeployUpdate: ReleaseDeploy = {
        apiVersion: "gh.guber.proc.io/v1",
        kind: "ReleaseDeploy",
        metadata: {
          name: resourceName,
          namespace: namespace || undefined,
        },
        status: statusUpdate,
      };

      await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
        namespace || "default",
        resourceName,
        releaseDeployUpdate,
      );

      console.log(`ReleaseDeploy ${resourceName} provisioned successfully`);
      return true;
    } catch (error) {
      console.error(
        `Failed to provision ReleaseDeploy ${resourceName}:`,
        error,
      );

      // Update status to failed via HTTP API
      const failedStatus = {
        state: "Failed",
        error: error.message || String(error),
      };

      // Update status using the generated GitHub client
      const releaseDeployUpdate: ReleaseDeploy = {
        apiVersion: "gh.guber.proc.io/v1",
        kind: "ReleaseDeploy",
        metadata: {
          name: resourceName,
          namespace: namespace || undefined,
        },
        status: failedStatus,
      };

      await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
        namespace || "default",
        resourceName,
        releaseDeployUpdate,
      );

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
      // Clean up associated Cloudflare resources if they were created
      if (status && spec.createCloudflareResources) {
        await this.cleanupCloudflareResources(env, resourceName, status);
      }
    } catch (error) {
      console.error(`Error deleting ReleaseDeploy ${resourceName}:`, error);
    }
  }

  private async cleanupCloudflareResources(
    env: any,
    releaseDeployName: string,
    status: any,
  ) {
    console.log(
      `🧹 Cleaning up Cloudflare resources for ReleaseDeploy ${releaseDeployName}`,
    );

    // Delete WorkerScriptDeployment first (if it exists)
    if (status.workerScriptDeploymentName) {
      try {
        // Use the generated client to delete via API
        const response = await env.GUBER_API.fetch(
          new Request(
            `http://fake/apis/cf.guber.proc.io/v1/workerscriptdeployments/${status.workerScriptDeploymentName}`,
            {
              method: "DELETE",
              headers: {
                "Content-Type": "application/json",
              },
            },
          ),
        );

        if (response.ok) {
          console.log(
            `✅ Deleted WorkerScriptDeployment ${status.workerScriptDeploymentName}`,
          );
        } else {
          const errorText = await response.text();
          console.error(
            `❌ Failed to delete WorkerScriptDeployment ${status.workerScriptDeploymentName}: ${response.status} ${errorText}`,
          );
        }
      } catch (error) {
        console.error(
          `❌ Failed to delete WorkerScriptDeployment ${status.workerScriptDeploymentName}:`,
          error,
        );
      }
    }

    // Delete WorkerScriptVersion (if it exists)
    if (status.workerScriptVersionName) {
      try {
        // Use the generated client to delete via API
        const response = await env.GUBER_API.fetch(
          new Request(
            `http://fake/apis/cf.guber.proc.io/v1/workerscriptversions/${status.workerScriptVersionName}`,
            {
              method: "DELETE",
              headers: {
                "Content-Type": "application/json",
              },
            },
          ),
        );

        if (response.ok) {
          console.log(
            `✅ Deleted WorkerScriptVersion ${status.workerScriptVersionName}`,
          );
        } else {
          const errorText = await response.text();
          console.error(
            `❌ Failed to delete WorkerScriptVersion ${status.workerScriptVersionName}: ${response.status} ${errorText}`,
          );
        }
      } catch (error) {
        console.error(
          `❌ Failed to delete WorkerScriptVersion ${status.workerScriptVersionName}:`,
          error,
        );
      }
    }

    // Delete Worker (if it exists)
    if (status.workerName) {
      try {
        // Use the generated client to delete via API
        const response = await env.GUBER_API.fetch(
          new Request(
            `http://fake/apis/cf.guber.proc.io/v1/workers/${status.workerName}`,
            {
              method: "DELETE",
              headers: {
                "Content-Type": "application/json",
              },
            },
          ),
        );

        if (response.ok) {
          console.log(`✅ Deleted Worker ${status.workerName}`);
        } else {
          const errorText = await response.text();
          console.error(
            `❌ Failed to delete Worker ${status.workerName}: ${response.status} ${errorText}`,
          );
        }
      } catch (error) {
        console.error(
          `❌ Failed to delete Worker ${status.workerName}:`,
          error,
        );
      }
    }

    console.log(
      `🧹 Finished cleaning up Cloudflare resources for ReleaseDeploy ${releaseDeployName}`,
    );
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
              const headers: Record<string, string> = {
                Accept: "application/vnd.github.v3+json",
                "User-Agent": "Guber-GitHub-Controller/1.0",
              };

              if (env.GITHUB_TOKEN) {
                headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
              }

              const deploymentResponse = await fetch(status.endpoint, {
                method: "GET",
                headers,
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

                // Update status using the generated GitHub client
                const releaseDeployUpdate: ReleaseDeploy = {
                  apiVersion: "gh.guber.proc.io/v1",
                  kind: "ReleaseDeploy",
                  metadata: {
                    name: resource.name,
                    namespace: resource.namespace || undefined,
                  },
                  status: updatedStatus,
                };

                await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
                  "default",
                  resource.name,
                  releaseDeployUpdate,
                );
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

  private async createWorker(
    env: any,
    spec: any,
    releaseDeployName: string,
    releaseTag: string,
  ): Promise<string> {
    const workerName = spec.workerName || `${releaseDeployName}-worker`;

    // Create the Worker resource
    const workerResource: Worker = {
      apiVersion: "cf.guber.proc.io/v1",
      kind: "Worker",
      metadata: {
        name: workerName,
        namespace: undefined,
        labels: {
          "guber.proc.io/created-by": "ReleaseDeploy",
          "guber.proc.io/source-repository": spec.repository.replace("/", "-"),
          "guber.proc.io/source-tag": releaseTag,
        },
        annotations: {
          "guber.proc.io/source-repository": spec.repository,
          "guber.proc.io/source-tag": releaseTag,
          "guber.proc.io/created-by": `ReleaseDeploy/${releaseDeployName}`,
        },
      },
      spec: {
        name: workerName,
        ...spec.workerSpec,
      },
    };

    // Create the resource in the database
    await env.DB.prepare(
      "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
      .bind(
        uuid(),
        "cf.guber.proc.io",
        "v1",
        "Worker",
        "workers",
        workerName,
        JSON.stringify(workerResource.spec),
        null,
      )
      .run();

    console.log(
      `✅ Created Worker ${workerName} from ReleaseDeploy ${releaseDeployName}`,
    );

    return workerName;
  }

  private async createWorkerScriptVersion(
    env: any,
    spec: any,
    releaseData: any,
    releaseTag: string,
    releaseDeployName: string,
    workerName: string,
  ): Promise<string> {
    // Generate a unique name for the WorkerScriptVersion
    const workerScriptVersionName = `${workerName}-${releaseTag.replace(/[^a-zA-Z0-9]/g, "-").toLowerCase()}`;

    // Get script content from release if not provided
    let scriptContent = spec.workerScriptVersionSpec?.script;

    if (!scriptContent && releaseData) {
      // Look for common script files in release assets
      const scriptAssets = releaseData.assets?.filter(
        (asset: any) =>
          asset.name.endsWith(".js") ||
          asset.name.endsWith(".mjs") ||
          asset.name === "worker.js" ||
          asset.name === "index.js",
      );

      if (scriptAssets && scriptAssets.length > 0) {
        // Use the first matching asset
        const scriptAsset = scriptAssets[0];
        console.log(
          `📥 Downloading script from release asset: ${scriptAsset.name} (${scriptAsset.size} bytes)`,
        );

        const headers: Record<string, string> = {
          "User-Agent": "Guber-GitHub-Controller/1.0",
        };

        if (env.GITHUB_TOKEN) {
          headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
        }

        const scriptResponse = await fetch(scriptAsset.browser_download_url, {
          headers,
        });

        if (scriptResponse.ok) {
          scriptContent = await scriptResponse.text();
          console.log(
            `✅ Successfully downloaded script content (${scriptContent.length} characters)`,
          );
        } else {
          console.warn(
            `❌ Failed to download script asset ${scriptAsset.name}, will use empty script`,
          );
          scriptContent = "// Script content could not be fetched from release";
        }
      } else {
        // Try to get the main branch content as fallback
        console.log(
          `⚠️  No script assets found in release ${releaseTag}, trying to fetch from repository main branch`,
        );

        const headers: Record<string, string> = {
          Accept: "application/vnd.github.v3+json",
          "User-Agent": "Guber-GitHub-Controller/1.0",
        };

        if (env.GITHUB_TOKEN) {
          headers.Authorization = `Bearer ${env.GITHUB_TOKEN}`;
        }

        const repoContentResponse = await fetch(
          `https://api.github.com/repos/${spec.repository}/contents/worker.js`,
          {
            headers,
          },
        );

        if (repoContentResponse.ok) {
          const contentData = await repoContentResponse.json();
          if (contentData.content) {
            scriptContent = atob(contentData.content);
            console.log(
              `✅ Successfully fetched worker.js from repository main branch (${scriptContent.length} characters)`,
            );
          }
        } else {
          console.warn(
            `❌ Could not fetch worker.js from repository main branch`,
          );
        }

        if (!scriptContent) {
          console.warn(
            `⚠️  No script content available for WorkerScriptVersion, using placeholder`,
          );
          scriptContent = "// Script content could not be fetched";
        }
      }
    }

    // Create the WorkerScriptVersion resource via HTTP API
    const workerScriptVersionResource: WorkerScriptVersion = {
      apiVersion: "cf.guber.proc.io/v1",
      kind: "WorkerScriptVersion",
      metadata: {
        name: workerScriptVersionName,
        namespace: undefined,
        labels: {
          "guber.proc.io/created-by": "ReleaseDeploy",
          "guber.proc.io/source-repository": spec.repository.replace("/", "-"),
          "guber.proc.io/source-tag": releaseTag,
        },
        annotations: {
          "guber.proc.io/source-repository": spec.repository,
          "guber.proc.io/source-tag": releaseTag,
          "guber.proc.io/created-by": `ReleaseDeploy/${releaseDeployName}`,
          "guber.proc.io/release-url": releaseData?.html_url || "",
        },
      },
      spec: {
        workerName: workerName,
        script: scriptContent,
        metadata: {
          ...spec.workerScriptVersionSpec?.metadata,
          sourceRepository: spec.repository,
          sourceTag: releaseTag,
          createdBy: `ReleaseDeploy/${releaseDeployName}`,
          releaseUrl: releaseData?.html_url,
        },
      },
    };

    // Create the resource via the generated client
    try {
      // First create the resource in the database
      await env.DB.prepare(
        "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      )
        .bind(
          uuid(),
          "cf.guber.proc.io",
          "v1",
          "WorkerScriptVersion",
          "workerscriptversions",
          workerScriptVersionName,
          JSON.stringify(workerScriptVersionResource.spec),
          null,
        )
        .run();

      console.log(
        `✅ Created WorkerScriptVersion ${workerScriptVersionName} from ReleaseDeploy ${releaseDeployName}`,
      );
      console.log(
        `📝 WorkerScriptVersion details: worker=${workerName}, source=${spec.repository}@${releaseTag}`,
      );
    } catch (error) {
      console.error(`Failed to create WorkerScriptVersion:`, error);
      throw error;
    }

    return workerScriptVersionName;
  }

  private async createWorkerScriptDeployment(
    env: any,
    spec: any,
    releaseDeployName: string,
    releaseTag: string,
    workerName: string,
    workerScriptVersionName: string,
  ): Promise<string> {
    // Generate a unique name for the WorkerScriptDeployment
    const workerScriptDeploymentName = `${workerName}-deployment-${releaseTag.replace(/[^a-zA-Z0-9]/g, "-").toLowerCase()}`;

    // Create the WorkerScriptDeployment resource
    const workerScriptDeploymentResource: WorkerScriptDeployment = {
      apiVersion: "cf.guber.proc.io/v1",
      kind: "WorkerScriptDeployment",
      metadata: {
        name: workerScriptDeploymentName,
        namespace: undefined,
        labels: {
          "guber.proc.io/created-by": "ReleaseDeploy",
          "guber.proc.io/source-repository": spec.repository.replace("/", "-"),
          "guber.proc.io/source-tag": releaseTag,
          "guber.proc.io/worker": workerName,
        },
        annotations: {
          "guber.proc.io/source-repository": spec.repository,
          "guber.proc.io/source-tag": releaseTag,
          "guber.proc.io/created-by": `ReleaseDeploy/${releaseDeployName}`,
          "guber.proc.io/worker": workerName,
          "guber.proc.io/worker-script-version": workerScriptVersionName,
        },
      },
      spec: {
        workerName: workerName,
        workerScriptVersionName: workerScriptVersionName,
        environment: spec.environment || "production",
        ...spec.workerScriptDeploymentSpec,
      },
    };

    // Create the resource in the database
    await env.DB.prepare(
      "INSERT INTO resources (id, group_name, version, kind, plural, name, spec, namespace) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
      .bind(
        uuid(),
        "cf.guber.proc.io",
        "v1",
        "WorkerScriptDeployment",
        "workerscriptdeployments",
        workerScriptDeploymentName,
        JSON.stringify(workerScriptDeploymentResource.spec),
        null,
      )
      .run();

    console.log(
      `✅ Created WorkerScriptDeployment ${workerScriptDeploymentName} from ReleaseDeploy ${releaseDeployName}`,
    );
    console.log(
      `📝 WorkerScriptDeployment details: worker=${workerName}, version=${workerScriptVersionName}, environment=${spec.environment || "production"}`,
    );

    return workerScriptDeploymentName;
  }

  private async reconcilePendingReleaseDeploys(env: any) {
    console.log(
      "Checking pending release deploys for dependency resolution...",
    );

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

            // Update status using the generated GitHub client
            const releaseDeployUpdate: ReleaseDeploy = {
              apiVersion: "gh.guber.proc.io/v1",
              kind: "ReleaseDeploy",
              metadata: {
                name: deploy.name,
                namespace: deploy.namespace || undefined,
              },
              status: updatedStatus,
            };

            await patchApisGhGuberProcIoV1NamespacesNamespaceReleasedeploysName(
              "default",
              deploy.name,
              releaseDeployUpdate,
            );
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

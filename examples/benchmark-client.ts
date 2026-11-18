import assert from 'node:assert/strict';
import { $ } from 'bun';
import { z } from 'zod';
import { DEFAULT_RESOURCES, GkeSandboxes, KubernetesApi } from '../src/index.ts';

export const pods = '/api/v1/namespaces/sandboxes/pods';
const shape = { cpu: '4', memory: '8Gi' };
const podResources = z.object({
  spec: z.object({
    containers: z.tuple([
      z.object({
        resources: z.object({
          requests: z.record(z.string(), z.string()),
          limits: z.record(z.string(), z.string()),
        }),
      }),
    ]),
  }),
});

// Verify the effective resource requests and limits on the pod returned by the API.
class BenchmarkApi extends KubernetesApi {
  override async request(
    method: Parameters<KubernetesApi['request']>[0],
    path: string,
    body?: unknown,
  ) {
    const result = await super.request(method, path, body);
    if (method === 'POST' && path === pods) {
      const actual = podResources.parse(result).spec.containers[0].resources;
      for (const resource of [actual.requests, actual.limits]) {
        assert.equal(resource.cpu, shape.cpu);
        assert.equal(resource.memory, shape.memory);
      }
    }
    return result;
  }
}

export async function benchmarkClient(endpoint: string, controller: string | undefined) {
  let authAt = 0;
  let token = '';
  async function accessToken() {
    if (Date.now() >= authAt) {
      if (controller) {
        token = (
          await $`gcloud auth print-access-token --impersonate-service-account=${controller}`
            .quiet()
            .text()
        ).trim();
      } else {
        const response = await fetch(
          'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token',
          {
            headers: { 'Metadata-Flavor': 'Google' },
          },
        );
        if (!response.ok)
          throw new Error(`VM service account token request failed: ${response.status}`);
        token = z.object({ access_token: z.string() }).parse(await response.json()).access_token;
      }
      authAt = Date.now() + 45 * 60_000;
    }
    return token;
  }
  const client = new BenchmarkApi(endpoint, accessToken);
  const engine = new GkeSandboxes(client, {
    namespace: 'sandboxes',
    serviceAccount: 'sandbox',
    image: 'docker.io/library/debian:bookworm-slim',
    idleTimeoutMs: 1,
    resources: {
      requests: { ...DEFAULT_RESOURCES.requests, ...shape },
      limits: { ...DEFAULT_RESOURCES.limits, ...shape },
    },
  });
  await accessToken();
  return { client, engine };
}

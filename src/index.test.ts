import { expect, mock, test } from 'bun:test';
import { z } from 'zod';
import { GkeSandboxes, KubernetesApi } from './index.ts';

const image = 'example.test/workload@sha256:' + '1'.repeat(64);
const resources = {
  requests: { cpu: '100m', memory: '850Mi', 'ephemeral-storage': '4Gi' },
  limits: { cpu: '4', memory: '8Gi', 'ephemeral-storage': '40Gi' },
};

// Serve the pod creation response at the same HTTP boundary the public client uses.
test('a caller-supplied image and ID work with the standalone snapshot and network policies', async () => {
  const manifests = await Bun.file(
    new URL('../infra/resources.yaml.tftpl', import.meta.url),
  ).text();
  const resourceSchema = z.object({
    kind: z.string(),
    metadata: z.object({ name: z.string() }),
    spec: z.unknown().optional(),
  });
  const documents = manifests
    .split(/^---$/m)
    .map((doc) => resourceSchema.parse(Bun.YAML.parse(doc)));
  const selector = z.object({ matchLabels: z.record(z.string(), z.string()) });
  const policy = z
    .object({
      selector,
      snapshotGroupingRules: z.object({
        groupByLabelValue: z.object({ labels: z.array(z.string()) }),
      }),
    })
    .parse(documents.find((resource) => resource.kind === 'PodSnapshotPolicy')?.spec);
  const network = z
    .object({ podSelector: selector })
    .parse(
      documents.find(
        (resource) => resource.kind === 'NetworkPolicy' && resource.metadata.name === 'sandboxes',
      )?.spec,
    );
  let podSpec: unknown;
  let podAnnotations: unknown;
  const server = Bun.serve({
    port: 0,
    async fetch(request) {
      expect(request.headers.get('authorization')).toBe('Bearer test');
      const { pathname, searchParams } = new URL(request.url);
      if (request.method === 'POST' && pathname === '/api/v1/namespaces/sandboxes/pods') {
        const manifest = (await request.json()) as {
          metadata: Record<string, unknown>;
          spec: unknown;
        };
        podSpec = manifest.spec;
        podAnnotations = manifest.metadata.annotations;
        return Response.json({
          metadata: {
            ...manifest.metadata,
            uid: 'uid',
            resourceVersion: '1',
            creationTimestamp: new Date().toISOString(),
          },
          status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
        });
      }
      if (request.method === 'GET' && searchParams.has('labelSelector'))
        return Response.json({ items: [] });
      // A new sandbox records that its id is taken.
      if (
        request.method === 'POST' &&
        pathname === '/apis/coordination.k8s.io/v1/namespaces/sandboxes/leases'
      )
        return Response.json(await request.json());
      return new Response('Unexpected request', { status: 400 });
    },
  });
  try {
    const engine = new GkeSandboxes(new KubernetesApi(server.url.href, async () => 'test'), {
      namespace: 'sandboxes',
      serviceAccount: 'sandbox',
      image,
      resources,
    });
    const { sandbox, startState, labels } = await engine.acquire(
      'sample-42',
      AbortSignal.timeout(1000),
    );
    expect(sandbox.id).toBe('sample-42');
    expect(startState).toBe('missing');
    expect(labels).toMatchObject(policy.selector.matchLabels);
    expect(labels).toMatchObject(network.podSelector.matchLabels);
    expect(Object.keys(labels).sort()).toEqual(
      policy.snapshotGroupingRules.groupByLabelValue.labels.sort(),
    );
    // The memory watchdog starts in its own shell before the pod settles into its wait. Kubernetes
    // turns `$$` in a command into `$`, so the watchdog's own pid is written doubled.
    const [, , script] = (podSpec as { containers: [{ command: string[] }] }).containers[0].command;
    expect(script).toContain('gke-sandboxes-memory-watchdog');
    expect(script).toContain('[ "$pid" -ne $$$$ ]');
    expect(script).not.toMatch(/-ne \$\$ \]/);
    expect(podSpec).toMatchObject({
      runtimeClassName: 'gvisor',
      automountServiceAccountToken: false,
      serviceAccountName: 'sandbox',
      containers: [{ image, resources }],
      volumes: [{ name: 'scratch', emptyDir: {} }],
    });
    expect(podAnnotations).toMatchObject({
      'dev.gvisor.flag.overlay2': `root:self,size=${20 * 2 ** 30}`,
    });
  } finally {
    await server.stop(true);
  }
});

test('invalid workspace IDs cannot expand a delete selector', async () => {
  const engine = new GkeSandboxes(
    new KubernetesApi('https://example.invalid', async () => {
      throw new Error('unexpected request');
    }),
    {
      namespace: 'sandboxes',
      serviceAccount: 'sandbox',
      image,
    },
  );
  await expect(engine.delete('x,y')).rejects.toThrow(/pattern/);
  await expect(engine.deleteMatching({})).rejects.toThrow('selector is required');
});

test('the disk must be a size in bytes below the ephemeral-storage limit', () => {
  const build = (disk: string, limit: string) => () =>
    new GkeSandboxes(new KubernetesApi('https://example.invalid', async () => 'test'), {
      namespace: 'sandboxes',
      serviceAccount: 'sandbox',
      image,
      disk,
      resources: {
        requests: resources.requests,
        limits: { ...resources.limits, 'ephemeral-storage': limit },
      },
    });
  expect(build('1.5Gi', '2G')).not.toThrow();
  expect(build('20Gi', '20Gi')).toThrow('must be below the ephemeral-storage limit (20Gi)');
  expect(build('20GB', '40Gi')).toThrow('Unsupported disk size: 20GB');
});

test('exec returns output and the exit code when GKE sends status and leaves the socket open', async () => {
  const socket = {
    close: mock(() => {}),
    onmessage: null as null | ((event: { data: ArrayBuffer }) => void),
  };
  const original = Object.getOwnPropertyDescriptor(globalThis, 'WebSocket')!;
  Object.defineProperty(globalThis, 'WebSocket', {
    configurable: true,
    value: function () {
      queueMicrotask(() => {
        for (const [channel, payload] of [
          [1, 'one'],
          [2, 'two'],
          [
            3,
            JSON.stringify({
              status: 'Failure',
              details: { causes: [{ reason: 'ExitCode', message: '7' }] },
            }),
          ],
        ] as const) {
          const bytes = new Uint8Array([channel, ...new TextEncoder().encode(payload)]);
          socket.onmessage?.({ data: bytes.buffer });
        }
      });
      return socket;
    },
  });
  try {
    const api = new KubernetesApi('https://example.invalid', async () => 'test');
    expect(await api.exec('/pods/test', ['sh'], new Uint8Array(), 1)).toEqual({
      output: 'onetwo',
      exitCode: 7,
      truncated: false,
    });
    expect(socket.close).toHaveBeenCalledTimes(1);
    // A caller sees every piece as it arrives, and keeps no more than it asked for.
    const seen: string[] = [];
    expect(
      await api.exec('/pods/test', ['sh'], new Uint8Array(), 1, {
        maxOutputBytes: 4,
        onOutput: (chunk, stream) => seen.push(`${stream}:${new TextDecoder().decode(chunk)}`),
      }),
    ).toEqual({ output: 'onet', exitCode: 7, truncated: true });
    expect(seen).toEqual(['stdout:one', 'stderr:two']);
  } finally {
    Object.defineProperty(globalThis, 'WebSocket', original);
  }
});

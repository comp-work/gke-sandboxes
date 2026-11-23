import { expect, mock, test } from 'bun:test';
import { z } from 'zod';
import { GkeSandboxes, KubernetesApi, KubernetesApiError } from './index.ts';

const image = 'example.test/workload@sha256:' + '1'.repeat(64);
const resources = {
  requests: { cpu: '100m', memory: '850Mi', 'ephemeral-storage': '4Gi' },
  limits: { cpu: '4', memory: '8Gi', 'ephemeral-storage': '20Gi' },
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
    const podLimits = { ...resources.limits, 'ephemeral-storage': String(40 * 2 ** 30) };
    expect(podSpec).toMatchObject({
      runtimeClassName: 'gvisor',
      automountServiceAccountToken: false,
      serviceAccountName: 'sandbox',
      // The kubelet's limit is twice the 20 GiB root filesystem, so a full sandbox keeps /tmp.
      containers: [{ image, resources: { requests: resources.requests, limits: podLimits } }],
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

test('a disk limit that cannot be given to gVisor in bytes is refused', () => {
  const limits = { ...resources.limits, 'ephemeral-storage': '1.5Gi' };
  expect(
    () =>
      new GkeSandboxes(new KubernetesApi('https://example.invalid', async () => 'test'), {
        namespace: 'sandboxes',
        serviceAccount: 'sandbox',
        image,
        resources: { requests: resources.requests, limits },
      }),
  ).toThrow('Unsupported ephemeral-storage limit: 1.5Gi');
});

test('warm claims avoid local races and keep retained handles valid across pod replacements', async () => {
  type Manifest = {
    metadata: { name: string; labels: Record<string, string>; annotations: Record<string, string> };
  };
  type Stored = Manifest & {
    metadata: { uid: string; resourceVersion: string; creationTimestamp: string };
    status: { phase: string; conditions: { type: string; status: string }[] };
  };
  const pods = new Map<string, Stored>();
  let conflicts = 0;
  let readyLists = 0;
  let commandLookups = 0;
  const claimed = Promise.withResolvers<void>();
  let waitForClaimOnDelete = false;
  const exec = mock(async (path: string) => ({ exitCode: 0, output: path, truncated: false }));
  const api = new (class extends KubernetesApi {
    override exec(path: string) {
      return exec(path);
    }
    override async request(
      method: Parameters<KubernetesApi['request']>[0],
      path: string,
      body?: unknown,
    ) {
      if (method === 'POST') {
        const { metadata } = body as Manifest;
        const pod = {
          metadata: {
            ...metadata,
            uid: metadata.name,
            resourceVersion: '1',
            creationTimestamp: new Date().toISOString(),
          },
          status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }] },
        };
        pods.set(metadata.name, structuredClone(pod));
        return structuredClone(pod);
      }
      if (method === 'GET') {
        const selector = new URL(path, 'https://unused.invalid').searchParams.get('labelSelector')!;
        if (!selector.includes('!energy-sandbox')) {
          const id = /(?:^|,)energy-sandbox=([^,]+)/.exec(selector)![1];
          commandLookups++;
          return {
            items: [...pods.values()].filter((p) => p.metadata.labels['energy-sandbox'] === id),
          };
        }
        readyLists++;
        return {
          items: structuredClone(
            [...pods.values()].filter((p) => 'getenergy.com/warm-pool' in p.metadata.labels),
          ),
        };
      }
      const id = path.split('/').at(-1)!;
      const pod = pods.get(id)!;
      if (method === 'PATCH') {
        const { metadata } = body as Manifest & { metadata: { resourceVersion: string } };
        if (pod.metadata.resourceVersion === metadata.resourceVersion) {
          for (const [key, value] of Object.entries(metadata.labels)) {
            if (value === null) delete pod.metadata.labels[key];
            else pod.metadata.labels[key] = value;
          }
          pod.metadata.resourceVersion = '2';
          if (waitForClaimOnDelete) claimed.resolve();
          return structuredClone(pod);
        }
      } else if (method === 'DELETE') {
        const { preconditions } = body as {
          preconditions: { uid: string; resourceVersion: string };
        };
        expect(preconditions.uid).toBe(id);
        if (waitForClaimOnDelete) await claimed.promise;
        if (preconditions.resourceVersion === pod.metadata.resourceVersion) {
          pods.delete(id);
          return {};
        }
      } else throw new Error('Unexpected request');
      conflicts++;
      throw new KubernetesApiError(409, 'Changed since list');
    }
  })('https://unused.invalid', async () => 'unused');
  const options = { namespace: 'sandboxes', serviceAccount: 'sandbox', image };
  const a = new GkeSandboxes(api, options).warmPool('agents');
  const b = new GkeSandboxes(api, options).warmPool('agents');
  await a.reconcile(2);
  const signal = AbortSignal.timeout(3000);
  const beforeLists = readyLists;
  const [one, two] = await Promise.all([
    a.create(signal, { owner: 'a' }),
    a.create(signal, { owner: 'b' }),
  ]);
  expect(one.fromPool && two.fromPool).toBe(true);
  expect(one.sandbox.id).not.toBe(two.sandbox.id);
  expect(readyLists - beforeLists).toBe(1);
  expect(conflicts).toBe(0);
  expect(pods.get(two.sandbox.id)?.metadata.labels.owner).toBe('b');
  const direct = await one.sandbox.process.executeCommand('true');
  expect(direct.result.endsWith('/' + one.podName)).toBe(true);
  expect(commandLookups).toBe(0);
  await new Promise<void>((resolve) => setImmediate(resolve));
  for (const item of [one, two]) {
    const pod = pods.get(item.podName)!;
    pods.delete(item.podName);
    pod.metadata.name += '-restored';
    pods.set(pod.metadata.name, pod);
    expect((await item.sandbox.process.executeCommand('true')).result.endsWith('-restored')).toBe(
      true,
    );
  }
  expect(commandLookups).toBe(2);

  await a.reconcile(2);
  const independent = await Promise.all([a.create(signal), b.create(signal)]);
  expect(independent.every((item) => item.fromPool)).toBe(true);
  expect(independent[0].sandbox.id).not.toBe(independent[1].sandbox.id);
  await a.reconcile(1);
  const priorConflicts = conflicts;
  waitForClaimOnDelete = true;
  const [three] = await Promise.all([b.create(signal), a.reconcile(0)]);
  expect(pods.has(three.sandbox.id)).toBe(true);
  expect(conflicts).toBe(priorConflicts + 1);
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

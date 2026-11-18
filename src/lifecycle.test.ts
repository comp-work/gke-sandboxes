import { afterAll, expect, test } from 'bun:test';
import { GkeSandboxes, KubernetesApi, KubernetesApiError, sandboxState } from './index.ts';

// The sleep, wake and recovery rules, against a stand-in for the Kubernetes API. Ported from the
// tests of Energy's dispatcher, which this library was extracted from.

type Resource = {
  metadata: {
    name: string;
    uid: string;
    resourceVersion: string;
    creationTimestamp: string;
    deletionTimestamp?: string;
    labels: Record<string, string>;
    annotations: Record<string, string>;
  };
  status: Record<string, unknown>;
};

const ID = 'session-42';
const signal = new AbortController().signal;
const running = () => ({
  phase: 'Running',
  podIP: '10.0.0.7',
  conditions: [{ type: 'Ready', status: 'True' }],
});
const stored = (snapshot: string) => ({
  conditions: [{ type: 'Ready', status: 'True' }],
  storageStatus: {
    gcs: { observedGCSPath: `gs://snapshots/kept/${snapshot}` },
  },
});
type Watch = {
  kind: string;
  name: string;
  send: (type: string, object: unknown) => void;
};
const servers: { stop: (closeActiveConnections: boolean) => Promise<void> }[] = [];
afterAll(() => Promise.all(servers.map((server) => server.stop(true))));

// The slice of the Kubernetes API the provider uses. Each pod keeps the files written through
// exec; a snapshot copies them and a pod created with the restore annotation gets them back.
// Requests are answered directly. A watch is the client's own, served over HTTP, and is sent each
// change made through a request or `change`.
const cluster = (options: { drainStatus: 0 | 3; restores: boolean }) => {
  // Deleted pods stay listed as shutting down until a test lets them go.
  let lingering = false;
  // A pod stays pending and a snapshot unfinished until a test moves them on.
  let slow = false;
  // The next watch starts from a version the server no longer remembers.
  let forgotten = false;
  const counts = { reads: 0, watches: 0 };
  const watches = new Map<Watch, () => void>();
  const kinds: Record<string, Map<string, Resource>> = {
    pods: new Map(),
    podsnapshots: new Map(),
    podsnapshotmanualtriggers: new Map(),
  };
  const files = new Map<string, Set<string>>();
  // What each pod was created with.
  const specs = new Map<string, unknown>();
  let version = 0;
  const announce = (kind: string, type: 'ADDED' | 'MODIFIED' | 'DELETED', resource: Resource) => {
    for (const watch of watches.keys())
      if (watch.kind === kind && watch.name === resource.metadata.name) watch.send(type, resource);
  };
  const server = Bun.serve({
    port: 0,
    fetch: ({ url }) => {
      const { pathname, searchParams } = new URL(url);
      if (searchParams.get('watch') !== '1') return new Response(null, { status: 400 });
      const kind = pathname.split('/').at(-1)!;
      const name = searchParams.get('fieldSelector')!.replace('metadata.name=', '');
      const from = searchParams.get('resourceVersion');
      const encoder = new TextEncoder();
      let watch: Watch;
      return new Response(
        new ReadableStream<Uint8Array>({
          start: (controller) => {
            const send: Watch['send'] = (type, object) =>
              controller.enqueue(encoder.encode(`${JSON.stringify({ type, object })}\n`));
            watch = { kind, name, send };
            counts.watches += 1;
            if (forgotten) {
              forgotten = false;
              send('ERROR', {
                kind: 'Status',
                code: 410,
                message: 'too old resource version',
              });
              return controller.close();
            }
            watches.set(watch, () => controller.close());
            // Whatever happened since the version the client read.
            const resource = kinds[kind]!.get(name);
            if (resource && resource.metadata.resourceVersion !== from)
              send(from ? 'MODIFIED' : 'ADDED', resource);
            if (!resource && from) send('DELETED', { metadata: { name } });
          },
          cancel: () => {
            watches.delete(watch);
          },
        }),
      );
    },
  });
  servers.push(server);
  const matches = (resource: Resource, query: string) =>
    decodeURIComponent(new URLSearchParams(query).get('labelSelector') ?? '')
      .split(',')
      .filter(Boolean)
      // `key=value` selects by value, and a bare `key` selects every resource that has the label.
      .every((pair) =>
        pair.includes('=')
          ? resource.metadata.labels[pair.split('=')[0]!] === pair.split('=')[1]
          : pair in resource.metadata.labels,
      );
  const request: KubernetesApi['request'] = async (method, path, body) => {
    const [location, query = ''] = path.split('?');
    const [, kind = '', name] = /\/(pods|podsnapshots|podsnapshotmanualtriggers)(?:\/(.+))?$/.exec(
      location!,
    )!;
    const resources = kinds[kind]!;
    // A response is what the resource was when it was read.
    if (method === 'GET' && !name)
      return structuredClone({
        items: [...resources.values()].filter((resource) => matches(resource, query)),
      });
    if (method === 'GET') {
      counts.reads += 1;
      return structuredClone(resources.get(name!) ?? null);
    }
    if (method === 'DELETE') {
      for (const resource of name ? [resources.get(name)] : [...resources.values()])
        if (resource && matches(resource, query)) {
          if (kind === 'pods' && lingering) {
            resource.metadata.deletionTimestamp = 'now';
            resource.metadata.resourceVersion = String(++version);
            announce(kind, 'MODIFIED', resource);
          } else {
            resources.delete(resource.metadata.name);
            announce(kind, 'DELETED', resource);
          }
        }
      return {};
    }
    const input = body as {
      metadata: Partial<Resource['metadata']> & { name: string };
      spec?: { targetPod: string; containers?: { command: string[] }[] };
    };
    if (method === 'PATCH') {
      const resource = resources.get(name!);
      if (!resource) return null;
      if (Array.isArray(body)) {
        const operations = body as {
          op: 'test' | 'replace';
          path: string;
          value: string;
        }[];
        const target = ({ path }: { path: string }) => {
          const [, , group, field] = path.split('/');
          const values = resource.metadata[group as 'labels' | 'annotations'];
          return { values, field: field!.replaceAll('~1', '/') };
        };
        if (
          operations.some((operation) => {
            const { values, field } = target(operation);
            return operation.op === 'test' && values[field] !== operation.value;
          })
        )
          throw new KubernetesApiError(422, 'Test failed');
        for (const operation of operations) {
          const { values, field } = target(operation);
          values[field] = operation.value;
        }
        resource.metadata.resourceVersion = String(++version);
        announce(kind, 'MODIFIED', resource);
        return resource;
      }
      Object.assign(resource.metadata.labels, input.metadata.labels);
      for (const [annotation, value] of Object.entries(input.metadata.annotations ?? {}))
        if (value === null) delete resource.metadata.annotations[annotation];
        else resource.metadata.annotations[annotation] = value;
      resource.metadata.resourceVersion = String(++version);
      announce(kind, 'MODIFIED', resource);
      return resource;
    }
    const resource: Resource = {
      metadata: {
        ...input.metadata,
        labels: input.metadata.labels ?? {},
        annotations: input.metadata.annotations ?? {},
        uid: crypto.randomUUID(),
        resourceVersion: String(++version),
        creationTimestamp: new Date(version).toISOString(),
      },
      status: {},
    };
    resources.set(resource.metadata.name, resource);
    if (kind === 'pods') {
      specs.set(resource.metadata.name, input.spec);
      resource.status = slow ? { phase: 'Pending' } : running();
      const snapshot = resource.metadata.annotations['podsnapshot.gke.io/ps-name'];
      // A sandbox's first pod writes its identity file as it starts.
      const [, , script, sandboxId] = input.spec!.containers![0]!.command;
      const written = /^printf %s "\$0" > (\S+) \|\| exit 1$/m.exec(script!);
      files.set(
        resource.metadata.name,
        new Set(
          options.restores && snapshot
            ? files.get(snapshot)
            : written && !snapshot
              ? [`${written[1]}=${sandboxId}`]
              : [],
        ),
      );
    }
    if (kind === 'podsnapshotmanualtriggers') {
      const snapshot = `snapshot-${version}`;
      await request('POST', location!.replace(kind, 'podsnapshots'), {
        metadata: {
          name: snapshot,
          labels: {
            ...kinds.pods!.get(input.spec!.targetPod)!.metadata.labels,
          },
        },
      });
      files.set(snapshot, new Set(files.get(input.spec!.targetPod)));
      if (!slow) {
        kinds.podsnapshots!.get(snapshot)!.status = stored(snapshot);
        resource.status = { snapshotCreated: { name: snapshot } };
      }
    }
    announce(kind, 'ADDED', resource);
    return resource;
  };
  const exec: KubernetesApi['exec'] = async (podPath, _command, stdin) => {
    const pod = files.get(podPath.split('/').at(-1)!);
    if (!pod) throw new Error('Sandbox command connection failed.');
    // A command run for a caller starts by changing directory; the library's own checks do not.
    const script = new TextDecoder().decode(stdin).replace(/^cd .*\n/, '');
    if (script.includes('drain-idle'))
      return { exitCode: options.drainStatus, output: '', truncated: false };
    const written = /^printf %s '(.+)' > '(.+)'$/.exec(script);
    if (written) pod.add(`${written[2]}=${written[1]}`);
    const compared = /^test "\$\(cat '(.+)' 2>\/dev\/null\)" = '(.+)'$/.exec(script);
    if (compared)
      return {
        exitCode: pod.has(`${compared[1]}=${compared[2]}`) ? 0 : 1,
        output: '',
        truncated: false,
      };
    return { exitCode: 0, output: '', truncated: false };
  };
  // The real client, so that its watches are the ones tested.
  const client = (answers: Partial<Pick<KubernetesApi, 'request' | 'exec'>>) =>
    Object.assign(new KubernetesApi(server.url.href, async () => 'token'), {
      request,
      exec,
      ...answers,
    });
  // A backend's client. Its service is asked whether it still has work before a sandbox sleeps.
  const backend = (api: Partial<Pick<KubernetesApi, 'request' | 'exec'>> = {}) =>
    new GkeSandboxes(client(api), {
      namespace: 'sandboxes',
      image: 'registry/sandbox:1',
      serviceAccount: 'sandbox',
      drain: async (sandbox) => (await sandbox.process.executeCommand('drain-idle')).exitCode !== 3,
    });
  const sandboxes = backend();
  // A sandbox's pod has its own name, which changes with every wake.
  const pod = (sandboxId: string) =>
    [...kinds.pods!.values()].find(
      ({ metadata }) =>
        metadata.labels['energy-sandbox'] === sandboxId && !metadata.deletionTimestamp,
    )!;
  const filesOf = (sandboxId: string) => files.get(pod(sandboxId).metadata.name)!;
  const awake = () =>
    [...kinds.pods!.values()].map(({ metadata }) => metadata.labels['energy-sandbox']);
  // Lets the sweep treat the sandbox as long unused.
  const age = (sandboxId: string) => {
    pod(sandboxId).metadata.annotations['getenergy.com/acquired-ms'] = '0';
  };
  const sleep = async (sandboxId: string) => {
    age(sandboxId);
    await sandboxes.sleepUnused();
  };
  const linger = () => {
    lingering = true;
  };
  const specOf = (sandboxId: string) => specs.get(pod(sandboxId).metadata.name);
  // What the kubelet or GKE's snapshot controller does to a resource.
  const change = (resource: Resource, status: Resource['status']) => {
    resource.status = status;
    resource.metadata.resourceVersion = String(++version);
    const kind = Object.keys(kinds).find(
      (kind) => kinds[kind]!.get(resource.metadata.name) === resource,
    )!;
    announce(kind, 'MODIFIED', resource);
  };
  // Resolves once the provider has opened this many watches.
  const watching = async (count: number) => {
    while (counts.watches < count) await Bun.sleep(1);
  };
  // The server ends every open watch, as it may at any time.
  const endWatches = () => {
    for (const end of watches.values()) end();
    watches.clear();
  };
  return {
    sandboxes,
    backend,
    change,
    counts,
    watching,
    endWatches,
    slowly: () => {
      slow = true;
    },
    forget: () => {
      forgotten = true;
    },
    request,
    exec,
    kinds,
    pod,
    filesOf,
    specOf,
    awake,
    age,
    sleep,
    linger,
  };
};

const stateOf = async (sandboxes: GkeSandboxes, id = ID) => {
  const found = await sandboxes.find(id);
  return found && sandboxState(found);
};

test('sleeps an idle sandbox into a snapshot and wakes the same sandbox from it', async () => {
  const { sandboxes, kinds, filesOf, awake, sleep } = cluster({
    drainStatus: 0,
    restores: true,
  });
  const created = await sandboxes.acquire(ID, signal);
  expect(created.startState).toBe('missing');
  filesOf(ID).add('/home/user/login=kept');

  // A sandbox that was just acquired is not idle.
  await sandboxes.sleepUnused();
  expect(awake()).toEqual([ID]);
  await sleep(ID);
  expect(kinds.pods!.size).toBe(0);
  expect(kinds.podsnapshots!.size).toBe(1);
  expect(kinds.podsnapshotmanualtriggers!.size).toBe(0);
  expect(await stateOf(sandboxes)).toBe('stopped');

  const woken = await sandboxes.acquire(ID, signal);
  expect([woken.sandbox.id, woken.startState]).toEqual([ID, 'stopped']);
  expect(woken.podName).not.toBe(created.podName);
  expect(filesOf(ID).has('/home/user/login=kept')).toBe(true);
});

test('a sandbox sleeps once nothing has used it for five minutes', async () => {
  const { sandboxes, pod, awake } = cluster({ drainStatus: 0, restores: true });
  await sandboxes.acquire(ID, signal);
  const lastUsed = (minutesAgo: number) => {
    pod(ID).metadata.annotations['getenergy.com/acquired-ms'] = String(
      Date.now() - minutesAgo * 60_000,
    );
  };
  lastUsed(4.5);
  await sandboxes.sleepUnused();
  expect(awake()).toEqual([ID]);
  lastUsed(5.5);
  await sandboxes.sleepUnused();
  expect(awake()).toEqual([]);
});

test('a sandbox reported as in use stays awake', async () => {
  const { sandboxes, age, awake } = cluster({ drainStatus: 0, restores: true });
  const created = await sandboxes.acquire(ID, signal);
  age(ID);
  await created.touch();
  await sandboxes.sleepUnused();
  expect(awake()).toEqual([ID]);
});

test('leaves a sandbox running while drain says it has work', async () => {
  const { sandboxes, kinds, sleep } = cluster({
    drainStatus: 3,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  await sleep(ID);
  expect(kinds.podsnapshots!.size).toBe(0);
  expect(await stateOf(sandboxes)).toBe('started');
  // The pod is usable again at once, not only when an abandoned sleep would have expired.
  expect((await sandboxes.acquire(ID, signal)).startState).toBe('started');
});

test('refuses a woken pod that GKE started without its snapshot', async () => {
  const { sandboxes, kinds, sleep } = cluster({
    drainStatus: 0,
    restores: false,
  });
  await sandboxes.acquire(ID, signal);
  await sleep(ID);

  await expect(sandboxes.acquire(ID, signal)).rejects.toThrow('was not restored from its snapshot');
  // The empty pod is gone and the snapshot is kept for the next wake.
  expect(kinds.pods!.size).toBe(0);
  expect(kinds.podsnapshots!.size).toBe(1);
}, 15_000);

test('a wake does not wait for the slept pod to finish shutting down', async () => {
  const { sandboxes, kinds, filesOf, sleep, linger } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  filesOf(ID).add('/home/user/login=kept');
  linger();
  await sleep(ID);
  expect(kinds.pods!.size).toBe(1);

  const woken = await sandboxes.acquire(ID, signal);
  expect(woken.startState).toBe('stopped');
  expect(kinds.pods!.size).toBe(2);
  expect(filesOf(ID).has('/home/user/login=kept')).toBe(true);
  expect((await woken.sandbox.process.executeCommand('true')).exitCode).toBe(0);
});

test('a pod that dies comes back with everything up to its last checkpoint', async () => {
  const { sandboxes, kinds, pod, filesOf } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  filesOf(ID).add('/home/user/before=checkpoint');
  await sandboxes.checkpoint(ID);
  // The sandbox keeps running after a checkpoint.
  expect(await stateOf(sandboxes)).toBe('started');
  filesOf(ID).add('/home/user/after=checkpoint');
  await sandboxes.checkpoint(ID);
  expect(kinds.podsnapshots!.size).toBe(1);
  filesOf(ID).add('/home/user/never=saved');

  kinds.pods!.delete(pod(ID).metadata.name);
  const recovered = await sandboxes.acquire(ID, signal);
  expect(recovered.sandbox.id).toBe(ID);
  expect([...filesOf(ID)].toSorted()).toEqual([
    '/home/user/after=checkpoint',
    '/home/user/before=checkpoint',
    '/var/tmp/.energy-sandbox-id=' + ID,
  ]);
});

test('a caller outlasts a sleep that was abandoned', async () => {
  const { sandboxes, pod, counts } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  // Whatever began this sleep died, so nothing ends it or clears its mark. The pod is free again
  // once the mark is older than any snapshot may take, which is six minutes. This mark is a fifth
  // of a second short of that.
  const { name } = pod(ID).metadata;
  pod(ID).metadata.annotations['getenergy.com/sleeping-ms'] = String(Date.now() - 6 * 60_000 + 200);
  await sandboxes.acquire(ID, signal);
  // Nothing changed on the pod, so the caller waited on its watch until the mark expired.
  expect(counts.watches).toBe(1);
  // The same pod, with everything on it: it was neither slept nor replaced.
  expect(pod(ID).metadata.name).toBe(name);
  expect(await stateOf(sandboxes)).toBe('started');
});

test('the sweep puts idle sandboxes to sleep together, not one after another', async () => {
  const { backend, exec, kinds, age } = cluster({
    drainStatus: 0,
    restores: true,
  });
  // Each sandbox takes a while to answer the drain, as a real one does to snapshot.
  let draining = 0;
  let together = 0;
  const sandboxes = backend({
    exec: async (...call) => {
      if (!new TextDecoder().decode(call[2]).includes('drain-idle')) return exec(...call);
      together = Math.max(together, ++draining);
      await Bun.sleep(50);
      draining -= 1;
      return exec(...call);
    },
  });
  for (let index = 0; index < 4; index += 1) {
    await sandboxes.acquire(`session-${index}`, signal);
    age(`session-${index}`);
  }
  await sandboxes.sleepUnused();
  expect(kinds.pods!.size).toBe(0);
  expect(kinds.podsnapshots!.size).toBe(4);
  expect(together).toBe(4);
});

test('the first command after an acquire runs in its pod without looking it up', async () => {
  const { backend, request, exec, sleep } = cluster({ drainStatus: 0, restores: true });
  let lookups = 0;
  const ran: string[] = [];
  const sandboxes = backend({
    request: async (method, path, body) => {
      if (method === 'GET' && path.includes('/pods?')) lookups += 1;
      return request(method, path, body);
    },
    exec: async (...call) => {
      ran.push(call[0].split('/').at(-1)!);
      return exec(...call);
    },
  });
  for (const startState of ['missing', 'started', 'stopped']) {
    const acquired = await sandboxes.acquire(ID, signal);
    expect(acquired.startState).toBe(startState);
    lookups = 0;
    ran.length = 0;
    await acquired.sandbox.process.executeCommand('true');
    expect([lookups, ran]).toEqual([0, [acquired.podName]]);
    // A handle kept past the handoff finds the sandbox's pod again, which may have changed.
    await new Promise<void>((resolve) => setImmediate(resolve));
    await acquired.sandbox.process.executeCommand('true');
    expect([lookups, ran]).toEqual([1, [acquired.podName, acquired.podName]]);
    if (startState === 'started') await sleep(ID);
  }
});

test('a new pod is kept when the listing does not have it yet', async () => {
  const { backend, request, pod } = cluster({ drainStatus: 0, restores: true });
  // A listing read just after a pod was created can be older than the pod.
  let created = false;
  const sandboxes = backend({
    request: async (method, path, body) => {
      if (method === 'GET' && created && path.includes('energy-sandbox%3D')) {
        created = false;
        return { items: [] };
      }
      if (method === 'POST' && path.endsWith('/pods')) created = true;
      return request(method, path, body);
    },
  });
  await sandboxes.acquire(ID, signal);
  expect(pod(ID).metadata.name).toStartWith(`${ID}-`);
  expect(await stateOf(sandboxes)).toBe('started');
});

test('a caller is told that its pod has started, and does not keep asking', async () => {
  const { sandboxes, kinds, change, counts, watching, slowly } = cluster({
    drainStatus: 0,
    restores: true,
  });
  slowly();
  let acquired = false;
  const acquiring = sandboxes.acquire(ID, signal).then((sandbox) => {
    acquired = true;
    return sandbox;
  });
  await watching(1);
  const [pod] = [...kinds.pods!.values()];
  // A pod reports running a moment before its container accepts commands.
  change(pod!, {
    phase: 'Running',
    conditions: [{ type: 'Ready', status: 'False' }],
  });
  await Bun.sleep(50);
  expect(acquired).toBe(false);
  counts.reads = 0;
  change(pod!, running());
  expect((await acquiring).startState).toBe('missing');
  // One watch, and the pod was not read again by name once it was open.
  expect(counts).toEqual({ reads: 0, watches: 1 });
});

test('a watch that the server ends before the pod has started is taken up again', async () => {
  const { sandboxes, kinds, change, counts, watching, endWatches, slowly } = cluster({
    drainStatus: 0,
    restores: true,
  });
  slowly();
  const acquiring = sandboxes.acquire(ID, signal);
  await watching(1);
  const [pod] = [...kinds.pods!.values()];
  endWatches();
  // The client reads the pod again and watches from there.
  await watching(2);
  change(pod!, { phase: 'Running', conditions: [] });
  // A change that no watch reports, because none is open, is found by that read.
  endWatches();
  change(pod!, running());
  expect((await acquiring).sandbox.id).toBe(ID);
  expect(counts.watches).toBe(2);
});

test('a watch from a version the server has forgotten starts from the current one', async () => {
  const { sandboxes, kinds, change, counts, watching, slowly, forget } = cluster({
    drainStatus: 0,
    restores: true,
  });
  slowly();
  forget();
  const acquiring = sandboxes.acquire(ID, signal);
  await watching(2);
  change([...kinds.pods!.values()][0]!, running());
  expect((await acquiring).startState).toBe('missing');
  expect(counts.watches).toBe(2);
});

test('a caller that aborts stops waiting for its pod', async () => {
  const { sandboxes, watching, slowly } = cluster({
    drainStatus: 0,
    restores: true,
  });
  slowly();
  const aborting = new AbortController();
  const acquiring = sandboxes.acquire(ID, aborting.signal);
  await watching(1);
  aborting.abort(new Error('The run was cancelled.'));
  await expect(acquiring).rejects.toThrow('The run was cancelled.');
});

test('a pod that is deleted while it starts fails the caller', async () => {
  const { sandboxes, kinds, request, watching, slowly } = cluster({
    drainStatus: 0,
    restores: true,
  });
  slowly();
  const acquiring = sandboxes.acquire(ID, signal);
  await watching(1);
  await request('DELETE', `/api/v1/namespaces/sandboxes/pods/${[...kinds.pods!.keys()][0]}`);
  await expect(acquiring).rejects.toThrow('failed to start');
});

test('a caller goes on as soon as the checkpoint that held its pod ends', async () => {
  const { sandboxes, pod, request, watching } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  const { metadata } = pod(ID);
  metadata.annotations['getenergy.com/sleeping-ms'] = String(Date.now());
  const acquiring = sandboxes.acquire(ID, signal);
  await watching(1);
  // A checkpoint ends by clearing its mark.
  await request('PATCH', `/api/v1/namespaces/sandboxes/pods/${metadata.name}`, {
    metadata: { annotations: { 'getenergy.com/sleeping-ms': '0' } },
  });
  expect((await acquiring).startState).toBe('stopped');
  expect(pod(ID).metadata.name).toBe(metadata.name);
});

test('a sleep waits for GKE to name the snapshot and then to store it', async () => {
  const { sandboxes, kinds, change, counts, watching, slowly, age } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  slowly();
  age(ID);
  const sleeping = sandboxes.sleepUnused();
  await watching(1);
  const [trigger] = [...kinds.podsnapshotmanualtriggers!.values()];
  const [snapshot] = [...kinds.podsnapshots!.values()];
  change(trigger!, { snapshotCreated: { name: snapshot!.metadata.name } });
  await watching(2);
  // The pod is kept until its files are stored.
  expect(kinds.pods!.size).toBe(1);
  counts.reads = 0;
  change(snapshot!, stored(snapshot!.metadata.name));
  await sleeping;
  expect(kinds.pods!.size).toBe(0);
  expect(await stateOf(sandboxes)).toBe('stopped');
  expect(counts).toEqual({ reads: 0, watches: 2 });
});

test('a snapshot that GKE could not take leaves the sandbox running', async () => {
  const { sandboxes, kinds, change, watching, slowly, age } = cluster({
    drainStatus: 0,
    restores: true,
  });
  await sandboxes.acquire(ID, signal);
  slowly();
  age(ID);
  const sleeping = sandboxes.sleepUnused();
  await watching(1);
  change([...kinds.podsnapshotmanualtriggers!.values()][0]!, {
    conditions: [{ type: 'Triggered', status: 'False', message: 'no room' }],
  });
  await sleeping;
  expect(kinds.podsnapshotmanualtriggers!.size).toBe(0);
  expect(await stateOf(sandboxes)).toBe('started');
});

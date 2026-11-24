import { afterAll, beforeAll, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CommandServerUnreachableError, directExec } from './command-channel.ts';
import { COMMAND_SERVER } from './command-server-binary.ts';
import { GkeSandboxes, KubernetesApi } from './index.ts';

// The command server itself, built for this machine: the same source the package ships for
// Linux, so the client is tested against what answers it in a sandbox.
const SOURCE = join(import.meta.dir, '../command-server/command-server.c');
const TOKEN = 'a'.repeat(64);
let directory: string;
let server: ReturnType<typeof Bun.spawn>;
let port: number;
const address = () => ({ host: '127.0.0.1', port, token: TOKEN });
const sh = (script: string) => ['sh', '-c', script];
const run = (script: string, stdin = '', options = {}) =>
  directExec(address(), sh(script), new TextEncoder().encode(stdin), 30, options, true);

// Starts a command server on a port nothing else holds.
const serve = (token: string) => {
  const probe = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { data() {} } });
  const free = probe.port;
  probe.stop(true);
  const process = Bun.spawn([join(directory, 'command-server')], {
    env: {
      PATH: Bun.env.PATH,
      GKE_SANDBOXES_COMMAND_TOKEN: token,
      GKE_SANDBOXES_COMMAND_PORT: String(free),
    },
    stdout: 'ignore',
    stderr: 'inherit',
  });
  return { port: free, process };
};

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), 'command-server-test-'));
  const compiled = Bun.spawnSync([
    'cc',
    '-Wall',
    '-Wextra',
    '-Werror',
    '-o',
    join(directory, 'command-server'),
    SOURCE,
  ]);
  if (compiled.exitCode !== 0) throw new Error(compiled.stderr.toString());
  ({ port, process: server } = serve(TOKEN));
});
afterAll(async () => {
  server.kill();
  await rm(directory, { recursive: true });
});

test('the shipped command server was built from the current source', async () => {
  expect(COMMAND_SERVER.sourceSha256).toBe(
    createHash('sha256')
      .update(await readFile(SOURCE))
      .digest('hex'),
  );
});

test('runs a command with its stdin and reports its output and exit code', async () => {
  const streams: string[] = [];
  const result = await run('cat; echo problem >&2; exit 7', 'from stdin\n', {
    onOutput: (_chunk: Uint8Array, stream: string) => streams.push(stream),
  });
  expect(result.exitCode).toBe(7);
  expect(result.output).toContain('from stdin\n');
  expect(result.output).toContain('problem\n');
  expect(new Set(streams)).toEqual(new Set(['stdout', 'stderr']));
});

test('a command ended by a signal exits with 128 plus the signal', async () => {
  expect((await run('kill -KILL $$')).exitCode).toBe(137);
});

test('a program that does not exist is an ordinary failed command', async () => {
  const result = await directExec(address(), ['no-such-program'], new Uint8Array(), 30, {}, true);
  expect(result).toMatchObject({ exitCode: 127, output: expect.stringContaining('Could not run') });
});

test('carries a large file in and out unchanged', async () => {
  const bytes = crypto.getRandomValues(new Uint8Array(65536));
  const file = new Uint8Array(6 * 1024 * 1024);
  for (let offset = 0; offset < file.byteLength; offset += bytes.byteLength)
    file.set(bytes, offset);
  const path = join(directory, 'uploaded');
  const uploaded = await directExec(
    address(),
    ['sh', '-c', 'cat > "$0"', path],
    file,
    30,
    {},
    true,
  );
  expect(uploaded.exitCode).toBe(0);
  expect(Buffer.compare(await readFile(path), file)).toBe(0);
  const received: Uint8Array[] = [];
  const downloaded = await directExec(
    address(),
    ['cat', path],
    new Uint8Array(),
    30,
    {
      maxOutputBytes: 0,
      onOutput: (chunk) => received.push(chunk.slice()),
    },
    true,
  );
  expect(downloaded).toMatchObject({ exitCode: 0, output: '', truncated: true });
  expect(Buffer.compare(Buffer.concat(received), file)).toBe(0);
});

test('a command that prints while it is still being fed does not stall', async () => {
  // `cat` echoes far more than a pipe holds before its input ends.
  const input = 'x'.repeat(4 * 1024 * 1024);
  const result = await run('cat', input);
  expect(result.output.length).toBe(input.length);
});

test('commands do not see the token', async () => {
  expect((await run('env')).output).not.toContain('GKE_SANDBOXES');
});

test('refuses a caller without the token, and runs nothing for it', async () => {
  const marker = join(directory, 'ran');
  const attempt = directExec(
    { ...address(), token: 'b'.repeat(64) },
    sh(`touch ${marker}`),
    new Uint8Array(),
    30,
    {},
    true,
  );
  expect(attempt).rejects.toBeInstanceOf(CommandServerUnreachableError);
  await attempt.catch(() => undefined);
  expect(await Bun.file(marker).exists()).toBe(false);
});

test('an address nothing listens on is unreachable', async () => {
  const probe = Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { data() {} } });
  const unused = probe.port;
  probe.stop(true);
  expect(
    directExec({ ...address(), port: unused }, sh('true'), new Uint8Array(), 30, {}, false),
  ).rejects.toBeInstanceOf(CommandServerUnreachableError);
});

test('the caller stops waiting when its time is up', async () => {
  expect(directExec(address(), sh('sleep 30'), new Uint8Array(), 0.2, {}, true)).rejects.toThrow(
    'timed out',
  );
});

// A cluster with one pod, whose address is this machine: enough to see what the library asks the
// cluster for and that its commands then go to the command server.
const cluster = (podAnnotations: (created: Record<string, string>) => Record<string, string>) => {
  const requests: { method: string; path: string; body: any }[] = [];
  const pods = new Map<string, any>();
  let execs = 0;
  const api = Object.assign(new KubernetesApi('http://cluster.invalid', async () => 'token'), {
    request: async (method: string, path: string, body?: any) => {
      requests.push({ method, path, body });
      if (path.includes('/podsnapshots')) return { items: [] };
      if (path.endsWith('/configmaps')) return {};
      if (method === 'GET') return { items: [...pods.values()] };
      if (method === 'PATCH') return pods.get(path.split('/').at(-1)!);
      const pod = {
        metadata: {
          ...body.metadata,
          annotations: podAnnotations(body.metadata.annotations),
          uid: crypto.randomUUID(),
          resourceVersion: '1',
          creationTimestamp: new Date().toISOString(),
        },
        spec: body.spec,
        status: {
          phase: 'Running',
          podIP: '127.0.0.1',
          conditions: [{ type: 'Ready', status: 'True' }],
        },
      };
      pods.set(pod.metadata.name, pod);
      return pod;
    },
    exec: async () => {
      execs++;
      return { exitCode: 0, output: 'through the cluster\n', truncated: false };
    },
  });
  return { api, requests, pods, execs: () => execs };
};
const options = { namespace: 'sandboxes', image: 'registry/sandbox:1', serviceAccount: 'sandbox' };
const signal = new AbortController().signal;

test('a sandbox started with direct commands is reached without the cluster', async () => {
  // The pod's server has the token the library generated; this machine's has TOKEN.
  const { api, requests, pods, execs } = cluster((created) => ({
    ...created,
    'getenergy.com/command-token': TOKEN,
  }));
  const sandboxes = new GkeSandboxes(api, { ...options, directCommands: { port } });
  const { sandbox } = await sandboxes.acquire('session-42', signal);

  const published = requests.find(({ path }) => path.endsWith('/configmaps'))!;
  expect(Object.keys(published.body.binaryData)).toEqual([
    'command-server-x86_64',
    'command-server-aarch64',
  ]);
  const [pod] = [...pods.values()];
  const generated = requests.find(
    ({ method, path }) => method === 'POST' && path.endsWith('/pods'),
  )!.body.metadata.annotations;
  expect(generated['getenergy.com/command-token']).toMatch(/^[0-9a-f]{64}$/);
  expect(generated['getenergy.com/command-port']).toBe(String(port));
  expect(pod.spec.volumes).toContainEqual({
    name: 'command-server',
    configMap: { name: published.body.metadata.name, defaultMode: 0o555 },
  });
  expect(pod.spec.containers[0].command[2]).toContain(
    '/opt/gke-sandboxes/command-server-$(uname -m)',
  );

  const local = join(directory, 'local');
  const remote = join(directory, 'remote');
  await writeFile(local, 'payload');
  await sandbox.fs.uploadFile(local, remote);
  await sandbox.fs.downloadFile(remote, `${local}.back`);
  expect(await readFile(`${local}.back`, 'utf8')).toBe('payload');
  const reads = requests.length;
  expect(
    await sandbox.process.executeCommand('pwd; echo $GREETING', directory, { GREETING: 'hi' }),
  ).toEqual({ exitCode: 0, result: `${directory}\nhi\n`, truncated: false });
  // Nothing was asked of the cluster for the later commands, and nothing was run through it.
  expect([requests.length - reads, execs()]).toEqual([0, 0]);
});

test('a handle whose pod is gone finds the pod the sandbox has now', async () => {
  const first = serve('d'.repeat(64));
  const { api, pods } = cluster((created) => ({
    ...created,
    'getenergy.com/command-port': String(first.port),
    'getenergy.com/command-token': 'd'.repeat(64),
  }));
  const sandboxes = new GkeSandboxes(api, { ...options, directCommands: { port } });
  const { sandbox } = await sandboxes.acquire('session-42', signal);
  expect((await sandbox.process.executeCommand('echo first')).result).toBe('first\n');

  // The sandbox slept and woke: its first pod is gone and its new one has another token.
  first.process.kill();
  await first.process.exited;
  Object.assign([...pods.values()][0].metadata.annotations, {
    'getenergy.com/command-port': String(port),
    'getenergy.com/command-token': TOKEN,
  });
  expect((await sandbox.process.executeCommand('echo second')).result).toBe('second\n');
});

test('a sandbox started without a command server is still reached through the cluster', async () => {
  const { api, execs } = cluster(() => ({ 'getenergy.com/sleeping-ms': '0' }));
  const sandboxes = new GkeSandboxes(api, { ...options, directCommands: { port } });
  const { sandbox } = await sandboxes.acquire('session-42', signal);
  expect((await sandbox.process.executeCommand('true')).result).toBe('through the cluster\n');
  expect(execs()).toBe(1);
});

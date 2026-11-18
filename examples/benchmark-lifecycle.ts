import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { $ } from 'bun';
import { z } from 'zod';
import type { SandboxEnvironment } from '../src/index.ts';
import { benchmarkClient, pods } from './benchmark-client.ts';

// Run against the quickstart cluster. Stdout is one JSON record per attempt.
const { values } = parseArgs({
  options: {
    endpoint: { type: 'string' },
    controller: { type: 'string' },
    starts: { type: 'string', default: '100' },
    snapshots: { type: 'string', default: '30' },
  },
});
const args = z
  .object({
    endpoint: z.url(),
    controller: z.email(),
    starts: z.coerce.number().int().positive(),
    snapshots: z.coerce.number().int().nonnegative(),
  })
  .parse(values);
assert.ok(args.snapshots <= args.starts);

const { client, engine } = await benchmarkClient(args.endpoint, args.controller);
const fixture =
  'https://raw.githubusercontent.com/opencoredev/sandbox-sdk/56e60d6f42a9b11efb487b5364efc325831e976a/bench/fixtures/vite-react';
const files = [
  'index.html',
  'package-lock.json',
  'package.json',
  'src.jsx',
  'test.mjs',
  'vite.config.js',
];
const hashes: string[] = [];
const writeCommands = ['mkdir -p /var/tmp/bench-app', 'cd /var/tmp/bench-app'];
let fixtureBytes = 0;
for (const file of files) {
  const r = await fetch(`${fixture}/${file}`);
  if (!r.ok) throw Error(`Could not fetch ${file}: HTTP ${r.status}`);
  const bytes = Buffer.from(await r.arrayBuffer());
  fixtureBytes += bytes.length;
  hashes.push(`${createHash('sha256').update(bytes).digest('hex')}  ${file}`);
  writeCommands.push(`printf %s '${bytes.toString('base64')}' | base64 -d > '${file}'`);
}
const checksum = `cd /var/tmp/bench-app && sha256sum ${files.join(' ')}`;
async function run(box: SandboxEnvironment, command: string) {
  const result = await box.process.executeCommand(command);
  assert.equal(result.exitCode, 0, result.result);
  return result.result.trim();
}
const integerMs = (start: number) => Math.round(performance.now() - start);
for (let i = 0; i < args.starts; i++) {
  const id = `bench-${randomUUID()}`;
  const signal = AbortSignal.timeout(10 * 60_000);
  const full =
    Math.floor(((i + 1) * args.snapshots) / args.starts) >
    Math.floor((i * args.snapshots) / args.starts);
  const sample: Record<string, unknown> = { attempt: i + 1, snapshotTrial: full };
  try {
    const start = performance.now();
    const fresh = await engine.acquire(id, signal);
    await run(fresh.sandbox, 'true');
    sample.freshMs = integerMs(start);
    assert.equal(fresh.startState, 'missing');
    if (full) {
      await run(fresh.sandbox, writeCommands.join(' && '));
      assert.equal(await run(fresh.sandbox, checksum), hashes.join('\n'));
      sample.fixtureBytes = fixtureBytes;
      const checkpoint = performance.now();
      await engine.checkpoint(id);
      sample.saveMs = integerMs(checkpoint);
      const checkpointFiles = (await engine.inventory()).find(
        (item) => item.sandboxId === id,
      )?.snapshots;
      assert.equal(checkpointFiles?.length, 1);
      const checkpointPath = checkpointFiles![0]!.path;
      const sleep = performance.now();
      await engine.sleep(id);
      assert.ok(
        await client.watch(
          pods,
          fresh.podName,
          (pod) => (pod === null ? true : undefined),
          Date.now() + 120_000,
          signal,
        ),
      );
      sample.sleepMs = integerMs(sleep);
      const saved = (await engine.inventory()).find((item) => item.sandboxId === id);
      // The previous checkpoint can still be finalizing deletion after sleep returns.
      const sleepSnapshots = saved?.snapshots.filter(
        (snapshot) => snapshot.path !== checkpointPath,
      );
      assert.equal(sleepSnapshots?.length, 1);
      const path = sleepSnapshots![0]!.path;
      const usage = await $`gcloud storage du --summarize ${path}`.quiet().text();
      const bytes = Number(usage.match(/^\s*(\d+)\s/m)?.[1]);
      assert.ok(Number.isSafeInteger(bytes) && bytes > 0, `Unrecognized snapshot size: ${usage}`);
      sample.storedBytes = bytes;
      const wake = performance.now();
      const restored = await engine.acquire(id, signal);
      await run(restored.sandbox, 'true');
      sample.restoreMs = integerMs(wake);
      assert.equal(restored.startState, 'stopped');
      assert.notEqual(restored.podName, fresh.podName);
      assert.equal(await run(restored.sandbox, checksum), hashes.join('\n'));
    }
    console.log(JSON.stringify({ ...sample, ok: true }));
  } catch (error) {
    console.log(JSON.stringify({ ...sample, ok: false, error: String(error) }));
    throw error;
  } finally {
    await engine.delete(id);
  }
}

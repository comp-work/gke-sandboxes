import assert from 'node:assert/strict';
import { parseArgs } from 'node:util';
import { $ } from 'bun';
import { z } from 'zod';
import { GkeSandboxes, KubernetesApi } from '../src/index.ts';

const { values } = parseArgs({
  options: {
    endpoint: { type: 'string' },
    controller: { type: 'string' },
    concurrency: { type: 'string', default: '1' },
    rounds: { type: 'string', default: '1' },
  },
});
const args = z
  .object({
    endpoint: z.url({ protocol: /^https$/ }),
    controller: z.email(),
    concurrency: z.coerce.number().int().positive(),
    rounds: z.coerce.number().int().positive(),
  })
  .parse(values);

// A run can span a token's lifetime. Share one refresh across concurrent operations.
let refreshAt = 0;
let token: Promise<string>;
const client = new KubernetesApi(args.endpoint, () => {
  if (Date.now() >= refreshAt) {
    refreshAt = Date.now() + 45 * 60_000;
    token = $`gcloud auth print-access-token --impersonate-service-account=${args.controller}`
      .quiet()
      .text()
      .then((text) => text.trim())
      .catch((error) => {
        refreshAt = 0;
        throw error;
      });
  }
  return token;
});
const idleTimeoutMs = 1000;
const engine = new GkeSandboxes(client, {
  namespace: 'sandboxes',
  serviceAccount: 'sandbox',
  image: 'docker.io/library/debian:bookworm-slim',
  idleTimeoutMs,
});

async function cycle() {
  const id = `verify-${crypto.randomUUID()}`;
  const path = '/var/tmp/workspace-proof';
  const signal = AbortSignal.timeout(10 * 60_000);
  try {
    const before = performance.now();
    const first = await engine.acquire(id, signal);
    const created = performance.now();
    assert.equal(first.startState, 'missing');
    const result = await first.sandbox.process.executeCommand(`printf %s '${id}' > ${path}`);
    assert.equal(result.exitCode, 0, result.result);
    await engine.checkpoint(id);
    await Bun.sleep(idleTimeoutMs);
    const beforeSleep = performance.now();
    await engine.sleep(id);
    // A terminating pod can still be listed after DELETE returns.
    const gone = await client.watch(
      '/api/v1/namespaces/sandboxes/pods',
      first.podName,
      (pod) => (pod === null ? true : undefined),
      Date.now() + 120_000,
      signal,
    );
    assert.ok(gone, 'Old pod still present');
    assert.equal((await engine.find(id))?.snapshots.length, 1);
    const beforeWake = performance.now();
    const restored = await engine.acquire(id, signal);
    const woke = performance.now();
    assert.equal(restored.startState, 'stopped');
    assert.notEqual(restored.podName, first.podName);
    const file = await restored.sandbox.process.executeCommand(`cat ${path}`);
    assert.equal(file.exitCode, 0, file.result);
    assert.equal(file.result, id);
    return {
      createMs: created - before,
      sleepAndRemoveMs: beforeWake - beforeSleep,
      wakeMs: woke - beforeWake,
    };
  } finally {
    await engine.delete(id);
  }
}

const samples: Awaited<ReturnType<typeof cycle>>[] = [];
for (let round = 0; round < args.rounds; round++) {
  const results = await Promise.allSettled(Array.from({ length: args.concurrency }, cycle));
  const failures = results.filter((result) => result.status === 'rejected');
  for (const result of results) if (result.status === 'fulfilled') samples.push(result.value);
  if (failures.length)
    throw new AggregateError(
      failures.map((result) => result.reason),
      `${failures.length} failed cycles`,
    );
}
console.log(
  JSON.stringify({ concurrency: args.concurrency, rounds: args.rounds, samples }, null, 2),
);

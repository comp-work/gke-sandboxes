import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { z } from 'zod';
import { benchmarkClient, pods } from './benchmark-client.ts';

import { newTrace, observeStartup, traceRequest } from './benchmark-startup-trace.ts';

const { values } = parseArgs({
  options: {
    endpoint: { type: 'string' },
    controller: { type: 'string' },
    profile: { type: 'boolean', default: false },
    baselineSize: { type: 'string', default: '0' },
    poolSize: { type: 'string', default: '1' },
    batches: { type: 'string', default: '20' },
    intervalMs: { type: 'string', default: '6000' },
  },
});
const args = z
  .object({
    endpoint: z.url(),
    controller: z.email().optional(),
    profile: z.boolean(),
    batches: z.coerce
      .number()
      .int()
      .min(2)
      .refine((n) => n % 2 === 0),
    baselineSize: z.coerce.number().int().nonnegative(),
    poolSize: z.coerce.number().int().positive(),
    intervalMs: z.coerce.number().int().positive(),
  })
  .parse(values);
const { client, engine } = await benchmarkClient(args.endpoint, args.controller);
if (args.profile) observeStartup(client);
const runId = randomUUID();
const labels = { 'benchmark-run': runId };
const runQuery = new URLSearchParams({ labelSelector: `benchmark-run=${runId}` });
const podList = z.object({
  items: z.array(
    z.object({
      metadata: z.object({ name: z.string(), deletionTimestamp: z.string().optional() }),
      status: z.object({
        conditions: z.array(z.object({ type: z.string(), status: z.string() })).optional(),
      }),
    }),
  ),
});
const assigned = new Set<string>();

async function block(blockNumber: number, poolSize: number) {
  const name = `bench-${runId}`;
  const pool = engine.warmPool(name);
  const stop = new AbortController();
  const errors: unknown[] = [];
  const pending: Promise<void>[] = [];
  const maintaining = poolSize
    ? pool.maintain(poolSize, stop.signal).catch((error) => {
        errors.push(error);
        stop.abort(error);
      })
    : Promise.resolve();
  const readyQuery = new URLSearchParams({ labelSelector: `getenergy.com/warm-pool=${name}` });
  async function waitForSpares() {
    const timeout = AbortSignal.timeout(300_000);
    while (true) {
      timeout.throwIfAborted();
      stop.signal.throwIfAborted();
      const idle = podList.parse(await client.request('GET', `${pods}?${readyQuery}`));
      if (
        idle.items.filter(
          (p) =>
            !p.metadata.deletionTimestamp &&
            p.status.conditions?.some((c) => c.type === 'Ready' && c.status === 'True'),
        ).length === poolSize
      )
        return;
      await Bun.sleep(250);
    }
  }
  async function request(batch: number, slot: number, target: number) {
    const begin = performance.now();
    const signal = AbortSignal.timeout(300_000);
    const firstCommand = async () => {
      const item = poolSize
        ? await pool.create(signal, labels)
        : { ...(await engine.acquire(randomUUID(), signal, labels)), fromPool: false };
      const allocationMs = performance.now() - begin;
      try {
        const result = await item.sandbox.process.executeCommand('true');
        return { item, result, allocationMs, elapsedMs: Math.round(performance.now() - begin) };
      } catch (error) {
        await client.request('DELETE', `${pods}/${item.podName}`);
        throw error;
      }
    };
    const trace = newTrace();
    const { item, result, allocationMs, elapsedMs } = args.profile
      ? await traceRequest(trace, firstCommand)
      : await firstCommand();
    const { podName, sandbox } = item;
    try {
      assert.equal(result.exitCode, 0, result.result);
      assert.ok(!assigned.has(sandbox.id), 'Sandbox handed out twice');
      assigned.add(sandbox.id);
      // Verify the pod identity outside the first-command timer.
      const proof = await sandbox.process.executeCommand('cat /var/tmp/.energy-sandbox-id');
      assert.equal(proof.exitCode, 0);
      assert.equal(proof.result.trim(), sandbox.id);
      console.log(
        JSON.stringify({
          block: blockNumber,
          poolSize,
          batch,
          slot,
          requestsInBatch: batch % 2 === 0 ? 1 : 2,
          dispatchLagMs: Math.round(begin - target),
          route: item.fromPool ? 'hit' : poolSize ? 'miss' : 'on-demand',
          elapsedMs,
          ...(args.profile && item.fromPool ? { ...trace, allocationMs } : {}),
        }),
      );
    } finally {
      await client.request('DELETE', `${pods}/${podName}`);
    }
  }
  try {
    if (poolSize) await waitForSpares();
    const opening = performance.now();
    for (let batch = 0; batch < args.batches; batch++) {
      const target = opening + batch * args.intervalMs;
      await Bun.sleep(Math.max(0, target - performance.now()));
      for (let slot = 0; slot < (batch % 2 === 0 ? 1 : 2); slot++)
        pending.push(
          request(batch, slot, target).catch((err) => {
            errors.push(err);
          }),
        );
    }
  } finally {
    await Promise.all(pending);
    stop.abort();
    await maintaining;
    await client.request('DELETE', `${pods}?${readyQuery}`);
    await client.request('DELETE', `${pods}?${runQuery}`);
    const remaining = podList.parse(await client.request('GET', `${pods}?${runQuery}`));
    await Promise.all(
      remaining.items.map((pod) =>
        client
          .watch(
            pods,
            pod.metadata.name,
            (found) => (found === null ? true : undefined),
            Date.now() + 60_000,
            null,
          )
          .then((gone) => assert.ok(gone, 'Pod still present after cleanup')),
      ),
    );
  }
  if (errors.length) throw new AggregateError(errors, 'Incomplete benchmark block');
}
try {
  // Reverse the order for the second pair to reduce the effect of client or cluster drift.
  const sizes = args.profile
    ? [args.poolSize]
    : [args.baselineSize, args.poolSize, args.poolSize, args.baselineSize];
  for (const [i, size] of sizes.entries()) await block(i + 1, size);
} finally {
  await client.request('DELETE', `${pods}?${runQuery}`);
}

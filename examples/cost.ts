import { z } from 'zod';

const filename = Bun.argv[2];
if (!filename) throw new Error('Pass a JSON file with workload and price assumptions.');
const price = z.number().nonnegative();
const input = z
  .object({
    hours: z.number().positive(),
    averageAwake: price,
    sandboxesPerNode: z.number().int().positive(),
    minimumNodes: z.number().int().nonnegative(),
    spareNodes: z.number().int().nonnegative(),
    nodeAndDiskPerHour: price,
    clusterAndSystemPerMonth: price,
    snapshotStoragePerMonth: price,
    networkAndLoggingPerMonth: price,
    managedPerSandboxHour: price,
    managedExtrasPerMonth: price,
  })
  .strict()
  .parse(await Bun.file(filename).json());

function estimate(averageAwake: number) {
  const nodes = Math.max(
    input.minimumNodes,
    Math.ceil(averageAwake / input.sandboxesPerNode) + input.spareNodes,
  );
  const sandboxHours = averageAwake * input.hours;
  const gke =
    nodes * input.nodeAndDiskPerHour * input.hours +
    input.clusterAndSystemPerMonth +
    input.snapshotStoragePerMonth +
    input.networkAndLoggingPerMonth;
  const managed = sandboxHours * input.managedPerSandboxHour + input.managedExtrasPerMonth;
  return {
    averageAwake,
    nodes,
    sandboxHours,
    gkeUSD: Math.round(gke),
    managedUSD: Math.round(managed),
  };
}
console.table(
  [...new Set([0, Math.ceil(input.averageAwake / 10), input.averageAwake])].map(estimate),
);

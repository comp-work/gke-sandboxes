# Use the TypeScript client

Install the client in your backend. It runs on Node.js 22 or later and on Bun.

```sh
npm install @getenergy/gke-sandboxes
```

Your backend chooses a sandbox ID after checking the user’s access. Calling `acquire` with that ID creates a sandbox, returns the running one, or wakes it from its snapshot.

```ts
import { GkeSandboxes, KubernetesApi } from '@getenergy/gke-sandboxes';

const sandboxes = new GkeSandboxes(new KubernetesApi(clusterUrl, getGoogleAccessToken), {
  namespace: 'sandboxes',
  serviceAccount: 'sandbox',
  image: 'docker.io/library/debian:bookworm-slim',
});

const { sandbox } = await sandboxes.acquire('session-42', AbortSignal.timeout(300_000));
await sandbox.fs.uploadFile('./input.txt', '/var/tmp/input.txt');
const { exitCode, result } = await sandbox.process.executeCommand('wc -l /var/tmp/input.txt');
if (exitCode !== 0) throw new Error(result);
await sandboxes.checkpoint(sandbox.id);
```

- **Schedule checkpoints.** Call `await sandboxes.checkpointDue({ intervalMs: 600_000, concurrency: 4 })` from your backend scheduler. This checkpoints running sandboxes whose latest ready snapshot is at least ten minutes old, using sandbox age for the first checkpoint. Each checkpoint briefly pauses its sandbox. The result gives `attempted`, `succeeded`, and `skipped` counts for due candidates and a `failed` list with sandbox IDs and errors.
- **Keep active work running.** Call the returned `touch()` periodically while an agent is busy. A sandbox nothing has used for five minutes is put to sleep by the sweeper the quickstart deploys, which calls `sleepUnused()` once a minute. If jobs run in the background, supply `drain(sandbox)` to check for work before sleep. Returning `false` keeps it awake. `drain` is code in your backend, so call `sleepUnused()` there on a schedule and deploy with `TF_VAR_sweeper_enabled=false`. Explicit `sleep(id)` checks the same conditions.
- **Manage saved files.** Use `find(id)` to inspect a sandbox without waking it. Call `delete(id)` when its files can be discarded and other callers have finished with the ID.
- **Bring your tools.** Use a stable image with POSIX shell utilities and writable `/var/tmp`. Pin the image digest in production. To share a read-only set of tools across sandboxes, pass `runtime: { image, mountPath }`. Use `boot(labels)` to start a service once the filesystem is ready.
- **Run commands.** `executeCommand(command, cwd, env, timeoutSeconds, options)` returns the exit code and the combined output. When the time is up, the command and every process it started are ended in the sandbox. Pass `onOutput(chunk, stream)` to read output as it arrives. The result keeps the first 32 MiB and sets `truncated` beyond that; change it with `maxOutputBytes`.
- **Move files.** `sandbox.fs.uploadFile(local, remote)` and `sandbox.fs.downloadFile(remote, local)` copy a file in either direction, binary-safe.
- **Survive a runaway program.** A sandbox that reaches its memory limit is killed whole, and loses what it wrote since its last snapshot. Each sandbox therefore watches its own memory and ends its largest processes first. The ended command sees the reason on its error output. Pass `memoryWatchdog: { protect: ['my-service'] }` to name processes it must never end, or `memoryWatchdog: false` to turn it off. Each time it acts it prints a JSON line with `"event":"sandbox_memory_pressure"` to the container’s log, which GKE sends to Cloud Logging with the pod’s name and labels.
- **Look across sandboxes.** `list(labels)` returns every sandbox with those labels, awake or asleep. `inventory()` adds where each one’s snapshots are stored, for metering. `deleteMatching(labels)` deletes every sandbox with those labels. Labels you pass to `acquire` as a third argument are set when a sandbox is created.
- **Reach a port.** `endpoint(sandbox, port)` returns the sandbox’s private address. The quickstart blocks incoming traffic to sandboxes, so allow your caller in a network policy first, as the preview example does.
- **Size the sandbox.** Pass `resources: { requests, limits }` to set what a sandbox reserves on its node and the most it may use. The requests decide how many sandboxes fit on a machine. Pass `disk` to set the size of the sandbox’s root filesystem, which must be below the `ephemeral-storage` limit.

## Keep sandboxes ready for new sessions

Run one pool maintainer in your backend. This example keeps two unused sandboxes ready so two agents can start at once. Give every backend replica the same pool name, namespace, image and resource settings.

```ts
const pool = sandboxes.warmPool('agents');

// In one long-running worker. Abort the signal on shutdown.
await pool.maintain(2, shutdownSignal);
```

Create one pool object per backend process and reuse it across requests. Concurrent calls to `pool.create` on that object choose different ready pods. Across processes, Kubernetes decides which caller gets each pod. If the pool is empty, the SDK creates a sandbox on demand. Store the returned ID with its owner. When the user comes back, call `sandboxes.acquire(id, signal)` to reopen it.

```ts
const { sandbox, fromPool, touch } = await pool.create(signal);
```

The maintainer checks capacity once a second. It picks up unused pods after a worker restart. To remove spare capacity, stop the maintainer and call `await pool.reconcile(0)`. Assigned sandboxes keep running. Pools support prebuilt images. Use `acquire` directly if your startup command depends on caller labels through `boot(labels)`.

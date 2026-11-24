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
  onPodLost(labels, phase, reason, hasSnapshot, recoveryPoint) {
    console.warn('sandbox pod lost', { labels, phase, reason, hasSnapshot, ...recoveryPoint });
  },
});

const { sandbox } = await sandboxes.acquire('session-42', AbortSignal.timeout(300_000));
await sandbox.fs.uploadFile('./input.txt', '/var/tmp/input.txt');
const { exitCode, result } = await sandbox.process.executeCommand('wc -l /var/tmp/input.txt');
if (exitCode !== 0) throw new Error(result);
await sandboxes.checkpoint(sandbox.id);
```

- **Keep active work running.** Call the returned `touch()` periodically while an agent is busy. Each call renews a Kubernetes Lease named after the sandbox’s pod and never writes to the pod, so frequent calls stay cheap on a busy cluster. A sandbox nothing has used for five minutes is put to sleep by the sweeper the quickstart deploys, which calls `sleepUnused()` once a minute. If jobs run in the background, supply `drain(sandbox)` to check for work before sleep. Returning `false` keeps it awake. `drain` is code in your backend, so call `sleepUnused()` there on a schedule and deploy with `TF_VAR_sweeper_enabled=false`. Explicit `sleep(id)` checks the same conditions; `sleep(id, { now: true })` sleeps at once however recently the sandbox was used, which pauses it. `acquire(id)` resumes it.
- **Manage saved files.** Use `find(id)` to inspect a sandbox without waking it. Call `delete(id)` when its files can be discarded and other callers have finished with the ID.
- **Bring your tools.** Use a stable image with POSIX shell utilities and writable `/var/tmp`. Pin the image digest in production. To share a read-only set of tools across sandboxes, pass `runtime: { image, mountPath }`. Use `boot(labels)` to start a service once the filesystem is ready.
- **Run commands.** `executeCommand(command, cwd, env, timeoutSeconds, options)` returns the exit code and the combined output. When the time is up, the command and every process it started are ended in the sandbox. Pass `onOutput(chunk, stream)` to read output as it arrives. The result keeps the first 32 MiB and sets `truncated` beyond that; change it with `maxOutputBytes`.
- **Skip the cluster for commands.** By default each command and file transfer is an exec call through the Kubernetes API. Pass `directCommands: { port: 7391 }` and every sandbox started from then on runs a small command server on that port, which the client connects to directly. In one cluster a command took 30 ms this way against 220 ms through the API, and a 50 MiB upload 0.7 s against 2.3 s; direct commands also do not queue behind other API calls when many sandboxes are busy. See [what it needs](#run-commands-without-the-kubernetes-api) before turning it on.
- **Move files.** `sandbox.fs.uploadFile(local, remote)` and `sandbox.fs.downloadFile(remote, local)` copy a file in either direction, binary-safe.
- **Survive a runaway program.** A sandbox that reaches its memory limit is killed whole, and loses what it wrote since its last snapshot. Each sandbox therefore watches its own memory and ends its largest processes first. The ended command sees the reason on its error output. Pass `memoryWatchdog: { protect: ['my-service'] }` to name processes it must never end, or `memoryWatchdog: false` to turn it off. Each time it acts it prints a JSON line with `"event":"sandbox_memory_pressure"` to the container’s log, which GKE sends to Cloud Logging with the pod’s name and labels.
- **Report pod loss.** When `acquire` finds a failed pod, `onPodLost` reports the selected Ready snapshot’s creation time (`recoveryPointAt`) and age in milliseconds at detection (`recoveryPointAgeMs`). Both fields are omitted without a usable timestamp. The SDK cannot tell which writes were lost. For restore outcome and duration, use the `wake` event from `onOperation`.
- **Snapshot and fork.** `snapshot(id)` snapshots a running sandbox and returns the snapshot's name. The sandbox runs on, and no later checkpoint or sleep removes the snapshot; `deleteSnapshot(name)` does. Pass it as `from` to start new sandboxes with those files, as many as you like: `acquire('fork-1', signal, labels, { from: name })`. A fork is a sandbox of its own, with its own id, labels and snapshots.
- **Look across sandboxes.** `list(labels)` returns every sandbox with those labels, awake or asleep. `inventory()` adds where each one’s snapshots are stored, for metering. `deleteMatching(labels)` deletes every sandbox with those labels. Labels you pass to `acquire` as a third argument are set when a sandbox is created.
- **Reach a port.** `endpoint(sandbox, port)` returns the sandbox’s private address. The quickstart blocks incoming traffic to sandboxes, so allow your caller in a network policy first, as the preview example does.
- **Size the sandbox.** Pass `resources: { requests, limits }` to set what a sandbox reserves on its node and the most it may use. The requests decide how many sandboxes fit on a machine. Pass `disk` to set the size of the sandbox’s root filesystem, which must be below the `ephemeral-storage` limit.
- **Keep memory across a sleep.** Snapshots keep files. Deploy with `TF_VAR_snapshot_scope=whole-pod` to keep memory and running processes too, so a woken sandbox carries on where it stopped. That needs machines newer than E2, such as [C4D](deploy.md#deploy-on-c4d), and each snapshot also stores the sandbox's memory.
- **Grant capabilities.** `capabilities: ['NET_ADMIN', 'SYS_ADMIN']` adds Linux capabilities to the sandbox's container. They apply inside gVisor's kernel, not the node's. The quickstart's namespace refuses most of them; deploy with `TF_VAR_pod_security_level=privileged` to allow them. See [run Docker in a sandbox](#run-docker-in-a-sandbox).

## Keep sandboxes ready for new sessions

Run one pool maintainer in your backend. This example keeps two unused sandboxes ready so two agents can start at once. Give every backend replica the same pool name, namespace, image and resource settings.

A pool hands out a sandbox by creating a Lease named after its pod. GKE's admission webhooks check every pod write but not Leases, so hand-outs stay fast when many arrive at once. With 100 at the same moment, a hand-out and a first command took 0.8–1.2 s at the median and 1.1–1.4 s at the 95th percentile in four of five runs, against 17 s for new pods; one run stalled at 13 s. The pod is labelled as its sandbox right afterwards, off the caller's path.

```ts
const pool = sandboxes.warmPool('agents');

// In one long-running worker. Abort the signal on shutdown.
await pool.maintain(2, shutdownSignal);
```

Create one pool object per backend process and reuse it across requests. To give a sandbox your own id, call `pool.acquire(id, signal, labels)`. A new id gets a pod from the pool; an existing one is woken or returned as `sandboxes.acquire` would. Callers that ask for the same new id at the same moment, from any replica, get one sandbox. Use the same pool for an id every time.

```ts
const { sandbox, fromPool, touch } = await pool.acquire(`session-${sessionId}`, signal, { team });
```

`pool.create(signal, labels)` hands out a sandbox with a generated id instead; store it with its owner. If the pool is empty, either call starts a new pod.

A pool's pods start before their labels are known, so `boot(labels)` runs when a pod is handed out instead, along with recording the sandbox's id in it. That is one command in the sandbox, about 30 ms with `directCommands` and about 200 ms through the Kubernetes API.

The maintainer checks capacity once a second. It picks up unused pods after a worker restart, and labels a handed-out pod whose caller stopped before it could. To remove spare capacity, stop the maintainer and call `await pool.reconcile(0)`. Handed-out sandboxes keep running.

## Run commands without the Kubernetes API

`directCommands: { port }` needs three things that the default does not:

- **A route to the pods.** The backend must be able to open a connection to a pod's address, so it runs in the cluster or on the cluster's VPC. A backend that only has the cluster's API endpoint cannot use this option.
- **A network rule.** The quickstart blocks incoming traffic to sandboxes. Admit your backend on the port, and nothing else:

  ```yaml
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata:
    namespace: sandboxes
    name: sandbox-commands
  spec:
    podSelector:
      matchLabels:
        app.kubernetes.io/managed-by: gke-sandboxes
    policyTypes: [Ingress]
    ingress:
      - from:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: my-backend
        ports:
          - protocol: TCP
            port: 7391
  ```

- **Permission to create ConfigMaps** in the sandbox namespace, which the Terraform module grants. The command server is a 30 KB static program inside this package. The client publishes it as a ConfigMap, one per package version, and each sandbox mounts it read-only, so it works with any image.

Each sandbox has its own random token, stored in the pod's annotations, so a caller needs read access to the pod to use it. The client and the server each show the other half of the token before a command is sent. A sandbox therefore cannot run commands in its neighbour, and a pod that takes over a reused address is never told what the previous one was asked to run. The connection itself is not encrypted: it relies on your cluster network being private.

Setting the option changes only sandboxes started afterwards. One that is already running keeps taking commands through the API until it next sleeps and wakes, and a client without the option can still reach every sandbox through the API. A handle remembers the server it last reached, so later commands ask the cluster nothing. If the sandbox has since moved to another pod, the handle finds it again; a `CommandServerUnreachableError` means no command ran.

## Run Docker in a sandbox

Docker runs inside a gVisor sandbox as root with two capabilities. Deploy with `TF_VAR_pod_security_level=privileged` and create the client with `capabilities: ['NET_ADMIN', 'SYS_ADMIN']`. In the image, or once in the sandbox as root:

```sh
apt-get install -y docker.io iptables
update-alternatives --set iptables /usr/sbin/iptables-legacy
update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
mkdir -p /etc/docker && echo '{"iptables":false,"ip6tables":false}' > /etc/docker/daemon.json
dockerd >/var/log/dockerd.log 2>&1 &
docker run --rm --network host hello-world
```

`docker run`, `docker build` and published ports work. Containers reach the internet only with `--network host`: gVisor has no NAT table, so Docker's default bridge has no way out, and containers cannot reach each other by name. Images are stored with the `vfs` driver, which copies each layer in full.

import { createHash, randomBytes, randomInt, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { createWriteStream } from 'node:fs';
import { readFile, rm } from 'node:fs/promises';
import { finished } from 'node:stream/promises';
import { z } from 'zod';
import {
  CommandServerUnreachableError,
  directExec,
  type CommandServerAddress,
} from './command-channel.js';
import { COMMAND_SERVER } from './command-server-binary.js';
import { KubernetesApiError, type ExecOptions, type KubernetesApi } from './kubernetes-api.js';
import { MEMORY_WATCHDOG } from './memory-watchdog.js';
export { CommandServerUnreachableError } from './command-channel.js';
export { KubernetesApi, KubernetesApiError, type ExecOptions } from './kubernetes-api.js';

export type SandboxEnvironment = {
  id: string;
  state: 'started' | 'stopped';
  process: {
    executeCommand(
      command: string,
      cwd?: string,
      env?: Record<string, string>,
      timeout?: number,
      options?: ExecOptions,
    ): Promise<{ exitCode: number; result: string; truncated: boolean }>;
  };
  fs: {
    uploadFile(local: string, remote: string, timeout?: number): Promise<void>;
    downloadFile(remote: string, local: string, timeout?: number): Promise<void>;
  };
};
type Labels = Record<string, string>;
export type SandboxEvent = {
  operation: 'create' | 'wake' | 'sleep' | 'checkpoint';
  outcome: 'succeeded' | 'aborted' | 'failed';
  labels: Labels;
  pod?: string;
  podIp?: string;
  durationMs: number;
  error: unknown;
};
type ResourceList = { cpu: string; memory: string; 'ephemeral-storage': string };
export type SandboxResources = { requests: ResourceList; limits: ResourceList };
// Sandboxes are mostly idle, so nodes are packed by the request, not by the limit. The kubelet
// counts a sandbox's root filesystem, scratch /tmp and logs against the disk limit and evicts a
// pod that exceeds it, so the limit is what one sandbox may take of its node's disk.
export const DEFAULT_RESOURCES: SandboxResources = {
  requests: { cpu: '250m', memory: '1Gi', 'ephemeral-storage': '4Gi' },
  limits: { cpu: '2', memory: '4Gi', 'ephemeral-storage': '40Gi' },
};
export const DEFAULT_DISK = '20Gi';
export type GkeSandboxOptions = {
  namespace: string;
  image: string;
  serviceAccount: string;
  managerLabels?: Labels;
  idleTimeoutMs?: number;
  runtime?: { image: string; mountPath: string };
  /** What a sandbox reserves on its node and may use at most. Defaults to `DEFAULT_RESOURCES`. */
  resources?: SandboxResources;
  /**
   * The size of a sandbox's root filesystem, `DEFAULT_DISK` unless given. `df` reports it and a
   * write past it fails. It must be below the `ephemeral-storage` limit: the difference is what a
   * full sandbox may still hold in /tmp before it is evicted. A sleeping sandbox holding more than
   * this does not wake.
   */
  disk?: string;
  /**
   * A sandbox that reaches its memory limit is killed whole. By default each sandbox watches its
   * own memory and ends its largest processes first. `protect` lists command-line fragments of
   * processes it must never end, such as a service started by `boot`. Pass `false` to turn it off.
   */
  memoryWatchdog?: false | { protect?: string[] };
  boot?: (labels: Labels) => { shell: string; args: string[] };
  /**
   * Runs commands and moves files over a connection straight to the sandbox, on this port,
   * instead of through the Kubernetes API. A command starts several times sooner and does not
   * queue behind other API calls on a busy cluster. The caller must be able to reach pod
   * addresses: on the cluster's network, and allowed in on this port by the sandboxes' network
   * policy. Each sandbox started with this set runs a small command server that only accepts
   * its own random token. Sandboxes started without it are still reached through the API.
   */
  directCommands?: { port: number };
  drain?: (sandbox: SandboxEnvironment) => Promise<boolean>;
  onOperation?: (event: SandboxEvent) => void;
  /** The recovery point is empty if the selected snapshot has no usable timestamp. */
  onPodLost?: (
    labels: Labels,
    phase: string,
    reason: string,
    hasSnapshot: boolean,
    recoveryPoint: { recoveryPointAt?: string; recoveryPointAgeMs?: number },
  ) => void;
};
const SANDBOX_LABEL = 'energy-sandbox';
const CREATED_LABEL = 'energy-created-ms';
const POOL_LABEL = 'getenergy.com/warm-pool';
const TEMPLATE_LABEL = 'getenergy.com/warm-template';
const ACQUIRED_ANNOTATION = 'getenergy.com/acquired-ms';
const SLEEPING_ANNOTATION = 'getenergy.com/sleeping-ms';
// JSON Patch paths; `~1` is a slash inside a key.
const ACQUIRED_PATH = '/metadata/annotations/getenergy.com~1acquired-ms';
const SLEEPING_PATH = '/metadata/annotations/getenergy.com~1sleeping-ms';
const RESTORED_ANNOTATION = 'getenergy.com/restore-verified';
const RESTORE_ANNOTATION = 'podsnapshot.gke.io/ps-name';
// What a pod's command server listens on and accepts. Only the pod's own record holds the token,
// so a caller that reads the pod can reach it and a neighbouring pod cannot.
const COMMAND_PORT_ANNOTATION = 'getenergy.com/command-port';
const COMMAND_TOKEN_ANNOTATION = 'getenergy.com/command-token';
const COMMAND_SERVER_MOUNT = '/opt/gke-sandboxes';
// One ConfigMap per build of the server, so pods of an earlier release keep theirs.
const COMMAND_SERVER_CONFIG = `gke-sandboxes-command-server-${COMMAND_SERVER.sha256.slice(0, 12)}`;
// GKE Dataplane V2 runs Cilium's bandwidth manager, which paces a pod's traffic out of its node to
// this rate, about a 16 Gbps node's share for one of 64 sandboxes. No sandbox crowds out its
// neighbours, and one turned against a host can send it no more than this.
const EGRESS_BANDWIDTH = '250M';
const QUANTITY_UNITS: Record<string, number> = {
  Ki: 2 ** 10,
  Mi: 2 ** 20,
  Gi: 2 ** 30,
  Ti: 2 ** 40,
  k: 1e3,
  M: 1e6,
  G: 1e9,
  T: 1e12,
};
// gVisor takes a size in bytes, not a Kubernetes quantity.
const quantityBytes = (quantity: string) => {
  const [, amount, unit] = /^(\d+(?:\.\d+)?)(Ki|Mi|Gi|Ti|k|M|G|T)?$/.exec(quantity) ?? [];
  if (!amount) throw new Error(`Unsupported disk size: ${quantity}`);
  return Math.floor(Number(amount) * (unit ? QUANTITY_UNITS[unit]! : 1));
};
// Written by a sandbox's first pod as it starts. A woken pod without it was not restored from its
// snapshot.
const SANDBOX_ID_FILE = '/var/tmp/.energy-sandbox-id';
const BOOT_LOG = '/var/tmp/.energy-boot.log';

const START_SECONDS = 300;
const SNAPSHOT_SECONDS = 300;
const SLEEP_ABANDONED_MS = SNAPSHOT_SECONDS * 1000 + 60_000;
const RESTORE_CHECKS = 25;
const RESTORE_CHECK_MS = 100;

// Model commands leave orphans behind; the pod's first process reaps them.
const POD_LOOP = 'trap "exit 0" TERM; while :; do sleep 3600 & wait $!; done';
const bootScript = (shell: string) => `(
  attempt=0
  until [ "$(cat ${SANDBOX_ID_FILE} 2>/dev/null)" = "$0" ]; do
    [ "$attempt" -lt 600 ] || exit 0
    sleep 0.05
    attempt=$((attempt + 1))
  done
  ${shell}
) </dev/null >${BOOT_LOG} 2>&1 &
`;
// A sandbox's first pod records whose it is before anything else can happen to it: a dispatch
// that created the pod may be aborted before it could run a command in it.
const podCommand = (
  sandboxId: string,
  first: boolean,
  boot: { shell: string; args: string[] } | null,
  watchdog: { protect: string[] } | null,
  commandServer: boolean,
) => [
  'sh',
  '-c',
  `${first ? `printf %s "$0" > ${SANDBOX_ID_FILE} || exit 1\n` : ''}${
    // Restarted if it ever ends: without it the sandbox takes no commands. The programs the
    // sandbox starts do not inherit the token, though anything in the sandbox can still find it:
    // it lets a caller into this sandbox only.
    commandServer
      ? `(while :; do "${COMMAND_SERVER_MOUNT}/command-server-$(uname -m)"; sleep 1; done) </dev/null &\nunset GKE_SANDBOXES_COMMAND_TOKEN GKE_SANDBOXES_COMMAND_PORT\n`
      : ''
  }${
    // Its own shell, so that its $$ is the watchdog and not the pod's first process.
    watchdog
      ? // Kubernetes reads `$$` in a command as an escaped `$`, so the script's own are doubled.
        `"$(command -v bash || echo sh)" -c ${quote(MEMORY_WATCHDOG.replaceAll('$$', () => '$$$$'))} gke-sandboxes-memory-watchdog ${watchdog.protect.map(quote).join(' ')} </dev/null &\n`
      : ''
  }${boot ? bootScript(boot.shell) : ''}${POD_LOOP}`,
  sandboxId,
  ...(boot ? boot.args : []),
];
// The command arrives on stdin so its size never meets a URL limit, and runs with stdin closed.
// Closing the connection does not stop the command, so a timer in the sandbox does: when the time
// is up it ends the command and every process descended from it, found through /proc so that no
// tool beyond the shell is needed. The caller's own timer rejects at the same moment.
const EXEC_WRAPPER = `script=$(cat)
sh -c "$script" </dev/null &
child=$!
(
  trap 'kill "$nap" 2>/dev/null; exit 0' TERM
  sleep "$1" &
  nap=$!
  wait "$nap"
  pids=$child
  grew=1
  while [ -n "$grew" ]; do
    grew=
    for stat in /proc/[0-9]*/stat; do
      read -r line < "$stat" 2>/dev/null || continue
      pid=\${stat#/proc/}
      pid=\${pid%/stat}
      rest=\${line##*) }
      rest=\${rest#* }
      case " $pids " in *" $pid "*) continue ;; esac
      case " $pids " in *" \${rest%% *} "*) pids="$pids $pid"; grew=1 ;; esac
    done
  done
  kill -TERM $pids 2>/dev/null
  sleep 2
  kill -KILL $pids 2>/dev/null
) >/dev/null 2>&1 &
timer=$!
wait "$child"
code=$?
kill "$timer" 2>/dev/null
exit "$code"`;
const execCommand = (timeoutSeconds: number) => [
  'sh',
  '-c',
  EXEC_WRAPPER,
  'gke-sandboxes-exec',
  String(timeoutSeconds),
];

const MetadataSchema = z.object({
  name: z.string(),
  uid: z.string(),
  resourceVersion: z.string(),
  creationTimestamp: z.string(),
  deletionTimestamp: z.string().optional(),
  labels: z.record(z.string(), z.string()).default({}),
  annotations: z.record(z.string(), z.string()).default({}),
});
const PodSchema = z.object({
  metadata: MetadataSchema,
  status: z.object({
    phase: z.string(),
    podIP: z.string().optional(),
    conditions: z.array(z.object({ type: z.string(), status: z.string() })).default([]),
    // Why a pod that is no longer running ended: `Evicted` for the pod, `OOMKilled` for its
    // container.
    reason: z.string().optional(),
    containerStatuses: z
      .array(
        z.object({
          state: z
            .object({ terminated: z.object({ reason: z.string().optional() }).optional() })
            .optional(),
        }),
      )
      .default([]),
  }),
});
const ConditionsSchema = z
  .object({ conditions: z.array(z.object({ type: z.string(), status: z.string() })).default([]) })
  .default({ conditions: [] });
const SnapshotSchema = z.object({ metadata: MetadataSchema, status: ConditionsSchema });
// Where GKE wrote a snapshot's files, which is what a sleeping sandbox occupies.
const StoredSnapshotSchema = z.object({
  metadata: MetadataSchema,
  status: z.object({
    storageStatus: z.object({ gcs: z.object({ observedGCSPath: z.string().startsWith('gs://') }) }),
  }),
});
// A sandbox's keep-alive. Reports of use renew it instead of writing to the pod: GKE checks every
// pod write with an admission webhook, which is several times slower and queues under load.
const LeaseSchema = z.object({
  metadata: z.object({ name: z.string() }),
  spec: z.object({ renewTime: z.string().optional() }).default({}),
});
const renewedMs = (lease: unknown) =>
  lease ? Date.parse(LeaseSchema.parse(lease).spec.renewTime ?? '') || 0 : 0;
const TriggerSchema = z.object({
  status: z
    .object({
      conditions: z
        .array(z.object({ type: z.string(), status: z.string(), message: z.string().optional() }))
        .default([]),
      snapshotCreated: z.object({ name: z.string() }).optional(),
    })
    .default({ conditions: [] }),
});
const listOf = <T extends z.ZodType>(item: T) => z.object({ items: z.array(item) });

type Pod = z.infer<typeof PodSchema>;
export type SandboxRecord = {
  name: string;
  labels: Record<string, string>;
  createdMs: number;
  pod: Pod | null;
  /** Ready snapshots, newest first. */
  snapshots: string[];
  /** When the newest snapshot was taken. */
  sleptMs: number | null;
};

const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const ready = (conditions: { type: string; status: string }[]) =>
  conditions.some(({ type, status }) => type === 'Ready' && status === 'True');
export const terminating = (pod: Pod) => !!pod.metadata.deletionTimestamp;
// A sleep or a checkpoint is working on the pod. One that has not finished by now was abandoned.
const snapshotting = (pod: Pod) =>
  Date.now() - Number(pod.metadata.annotations[SLEEPING_ANNOTATION]) < SLEEP_ABANDONED_MS;
const sleeping = (pod: Pod) => terminating(pod) || snapshotting(pod);
// A sleep or a checkpoint still holds the pod, and will either delete it or free it.
const held = (pod: Pod) => !terminating(pod) && snapshotting(pod);
export const sandboxState = ({ pod }: SandboxRecord) =>
  pod?.status.phase === 'Running' && !sleeping(pod) ? 'started' : 'stopped';
// Orders a sandbox's pods so that the one to use comes first: not shutting down, then oldest.
const oldestLiveFirst = (left: Pod, right: Pod) =>
  Number(terminating(left)) - Number(terminating(right)) ||
  left.metadata.creationTimestamp.localeCompare(right.metadata.creationTimestamp) ||
  left.metadata.name.localeCompare(right.metadata.name);
// A pod reports running a moment before its container accepts commands.
const starting = (pod: Pod) =>
  pod.status.phase === 'Pending' ||
  (pod.status.phase === 'Running' && !ready(pod.status.conditions));

export class SandboxInUseError extends Error {
  constructor(name: string) {
    super(`Sandbox ${name} is in use and cannot sleep yet.`);
    this.name = 'SandboxInUseError';
  }
}

const sandboxIdSchema = z
  .string()
  .max(56)
  .regex(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/);
// Only equality selectors are needed. Kubernetes validates the syntax of the label itself.
const labelSchema = z.record(
  z.string().regex(/^[A-Za-z0-9._/-]+$/),
  z.string().regex(/^[A-Za-z0-9._-]*$/),
);

export class GkeSandboxes {
  private readonly pods: string;
  private readonly leases: string;
  private readonly triggers: string;
  private readonly snapshotsPath: string;
  private readonly manager: Labels;
  private readonly idleTimeoutMs: number;
  private readonly diskBytes: number;
  private readonly configMaps: string;
  private commandServer: Promise<void> | undefined;

  constructor(
    private readonly kubernetes: KubernetesApi,
    private readonly options: GkeSandboxOptions,
  ) {
    const namespace = z
      .string()
      .max(63)
      .regex(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/)
      .parse(options.namespace);
    this.manager = labelSchema.parse(
      options.managerLabels ?? { 'app.kubernetes.io/managed-by': 'gke-sandboxes' },
    );
    if (
      [SANDBOX_LABEL, CREATED_LABEL, POOL_LABEL, TEMPLATE_LABEL].some((key) => key in this.manager)
    )
      throw new Error('A manager selector cannot use reserved workspace labels.');
    this.idleTimeoutMs = options.idleTimeoutMs ?? 300_000;
    const disk = options.disk ?? DEFAULT_DISK;
    const limit = (options.resources ?? DEFAULT_RESOURCES).limits['ephemeral-storage'];
    this.diskBytes = quantityBytes(disk);
    if (this.diskBytes >= quantityBytes(limit))
      throw new Error(
        `The disk (${disk}) must be below the ephemeral-storage limit (${limit}), which also covers /tmp and logs.`,
      );
    if (
      !Object.keys(this.manager).length ||
      !Number.isFinite(this.idleTimeoutMs) ||
      this.idleTimeoutMs <= 0
    )
      throw new Error('A manager label and a positive idle timeout are required.');
    this.pods = `/api/v1/namespaces/${namespace}/pods`;
    this.leases = `/apis/coordination.k8s.io/v1/namespaces/${namespace}/leases`;
    this.configMaps = `/api/v1/namespaces/${namespace}/configmaps`;
    if (options.directCommands)
      z.number().int().min(1).max(65535).parse(options.directCommands.port);
    const api = `/apis/podsnapshot.gke.io/v1/namespaces/${namespace}`;
    this.triggers = `${api}/podsnapshotmanualtriggers`;
    this.snapshotsPath = `${api}/podsnapshots`;
  }

  private selector(labels: Labels) {
    const pairs = Object.entries(labelSchema.parse({ ...labels, ...this.manager })).map(
      ([key, value]) => `${key}=${value}`,
    );
    return encodeURIComponent(pairs.join(','));
  }

  async acquire(id: string, signal: AbortSignal, initialLabels: Labels = {}) {
    while (true) {
      signal.throwIfAborted();
      const existing = await this.find(id);
      const createdMs = Date.now();
      const record: SandboxRecord = existing ?? {
        name: id,
        labels: {},
        createdMs,
        pod: null,
        snapshots: [],
        sleptMs: null,
      };
      const startState = existing ? sandboxState(existing) : 'missing';
      const { pod, fresh } = await this.start(record, signal, {
        ...initialLabels,
        ...this.manager,
        [SANDBOX_LABEL]: id,
        [CREATED_LABEL]: String(record.createdMs),
      });
      if (!fresh && !(await this.markAcquired(pod))) continue;
      return {
        sandbox: this.makeEnvironment(id, 'started', pod),
        startState,
        labels: pod.metadata.labels,
        podName: pod.metadata.name,
        createdMs: Number(pod.metadata.labels[CREATED_LABEL]),
        touch: () => this.touch(id),
      };
    }
  }

  /** Creates single-use sandboxes with generated IDs. Reopen a saved ID with acquire(). */
  warmPool(name: string) {
    sandboxIdSchema.parse(name);
    if (this.options.boot)
      throw new Error('Warm pools require an image that can start before caller labels are known.');
    const poolLabels = { ...this.manager, [POOL_LABEL]: name };
    const spare = (id: string) => this.manifest({ name: id, labels: poolLabels }, id, undefined);
    const template = createHash('sha256')
      .update(JSON.stringify(spare('template').spec))
      .digest('hex')
      .slice(0, 32);
    const list = async (labels: Labels) =>
      listOf(PodSchema)
        .parse(
          await this.kubernetes.request(
            'GET',
            `${this.pods}?labelSelector=${this.selector(labels)},!${SANDBOX_LABEL}`,
          ),
        )
        .items.filter((pod) => !terminating(pod));
    const requestedLabels = { ...poolLabels, [TEMPLATE_LABEL]: template };

    // One maintainer per named pool. Claims can come from any replica. A versioned delete
    // prevents a resize or image rollout from deleting a pod another replica just handed out.
    const reconcile = async (size: number) => {
      z.number().int().nonnegative().parse(size);
      const idle = await list(poolLabels);
      const usable = idle
        .filter(
          (pod) =>
            pod.metadata.labels[TEMPLATE_LABEL] === template &&
            ['Pending', 'Running'].includes(pod.status.phase),
        )
        .sort(oldestLiveFirst);
      const keep = new Set(usable.slice(0, size).map((pod) => pod.metadata.uid));
      await Promise.all([
        ...idle
          .filter((pod) => !keep.has(pod.metadata.uid))
          .map(async ({ metadata }) => {
            try {
              await this.kubernetes.request('DELETE', `${this.pods}/${metadata.name}`, {
                preconditions: { uid: metadata.uid, resourceVersion: metadata.resourceVersion },
              });
            } catch (error) {
              if (!(error instanceof KubernetesApiError && error.status === 409)) throw error;
            }
          }),
        ...Array.from({ length: Math.max(0, size - keep.size) }, () => {
          const pod = spare(randomUUID());
          pod.metadata.labels = requestedLabels;
          return this.createPod(pod);
        }),
      ]);
    };

    // Share one list among concurrent callers. Keep each chosen pod version reserved until
    // the API stops listing it. That also covers a list issued while a claim is in flight.
    let reading: Promise<Pod[]> | undefined;
    const reserved = new Set<string>();
    const podVersion = ({ metadata }: Pod) => metadata.uid + '/' + metadata.resourceVersion;
    const available = async () => {
      const pods = (await list(requestedLabels)).filter(
        (pod) => pod.status.phase === 'Running' && ready(pod.status.conditions),
      );
      const versions = new Set(pods.map(podVersion));
      for (const version of reserved) if (!versions.has(version)) reserved.delete(version);
      // Independent backend replicas can see the same list. Vary their first choice;
      // the versioned Kubernetes patch still decides exclusive ownership.
      const offset = pods.length ? randomInt(pods.length) : 0;
      return pods.slice(offset).concat(pods.slice(0, offset));
    };

    const create = async (signal: AbortSignal, labels: Labels = {}) => {
      labelSchema.parse(labels);
      for (const key of [
        SANDBOX_LABEL,
        CREATED_LABEL,
        POOL_LABEL,
        TEMPLATE_LABEL,
        ...Object.keys(this.manager),
      ])
        if (key in labels) throw new Error(`Reserved pool label: ${key}`);
      const startedAt = Date.now();
      signal.throwIfAborted();
      reading ??= available().finally(() => {
        reading = undefined;
      });
      const pods = await reading;
      while (pods.length) {
        signal.throwIfAborted();
        const pod = pods.pop()!;
        const version = podVersion(pod);
        if (reserved.has(version)) continue;
        reserved.add(version);
        const id = pod.metadata.name;
        const now = String(Date.now());
        try {
          const claimed = await this.kubernetes.request('PATCH', `${this.pods}/${id}`, {
            metadata: {
              resourceVersion: pod.metadata.resourceVersion,
              labels: {
                ...labels,
                [SANDBOX_LABEL]: id,
                [CREATED_LABEL]: now,
                [POOL_LABEL]: null,
                [TEMPLATE_LABEL]: null,
              },
              annotations: { [ACQUIRED_ANNOTATION]: now },
            },
          });
          if (!claimed) continue;
          this.report('create', PodSchema.parse(claimed).metadata.labels, pod, startedAt, null);
          return {
            sandbox: this.makeEnvironment(id, 'started', pod),
            podName: id,
            fromPool: true,
            touch: () => this.touch(id),
          };
        } catch (error) {
          if (!(error instanceof KubernetesApiError && error.status === 409)) {
            reserved.delete(version);
            throw error;
          }
        }
      }
      const { sandbox, podName, touch } = await this.acquire(randomUUID(), signal, labels);
      return { sandbox, podName, touch, fromPool: false };
    };

    return {
      create,
      reconcile,
      async maintain(size: number, signal: AbortSignal) {
        while (!signal.aborted) {
          await reconcile(size);
          try {
            await delay(1000, undefined, { signal });
          } catch (error) {
            if (!signal.aborted) throw error;
          }
        }
      },
    };
  }

  /** Open a previously listed workspace without extending its idle period on a read. */
  async open(record: SandboxRecord, signal: AbortSignal, freshLabels: Labels) {
    signal.throwIfAborted();
    await this.start(record, signal, { ...record.labels, ...freshLabels });
    return this.environment(record.name, 'started');
  }

  async labelPod(podName: string, labels: Labels) {
    if (
      !(await this.kubernetes.request('PATCH', `${this.pods}/${podName}`, { metadata: { labels } }))
    )
      throw new Error(`Pod ${podName} is not running.`);
  }

  async endpoint(sandbox: { id: string }, port: number) {
    const pod = await this.livePod(sandbox.id);
    if (!pod?.status.podIP || pod.status.phase !== 'Running' || sleeping(pod))
      throw new Error(`Sandbox ${sandbox.id} is not running.`);
    return { url: `http://${pod.status.podIP}:${port}`, headers: {} };
  }

  // Pod addresses are reused, so a remembered address may now belong to another sandbox.
  async endpointIsCurrent(sandboxId: string, endpoint: { url: string }) {
    const pod = await this.livePod(sandboxId);
    return (
      !!pod?.status.podIP &&
      pod.status.phase === 'Running' &&
      !sleeping(pod) &&
      new URL(endpoint.url).hostname === pod.status.podIP
    );
  }

  /**
   * Snapshots a running sandbox and lets it run on, so a pod that dies later loses only what was
   * written since. The sandbox is paused while the snapshot is taken.
   */
  async checkpoint(sandboxId: string) {
    const pod = await this.livePod(sandboxId);
    if (!pod || pod.status.phase !== 'Running' || sleeping(pod)) return;
    const claimed = await this.claim(pod, [
      { op: 'test', path: SLEEPING_PATH, value: pod.metadata.annotations[SLEEPING_ANNOTATION]! },
      { op: 'replace', path: SLEEPING_PATH, value: String(Date.now()) },
    ]);
    if (!claimed) return;
    const startedAt = Date.now();
    try {
      await this.dropOlderSnapshots(sandboxId, await this.snapshot(pod));
      this.report('checkpoint', pod.metadata.labels, pod, startedAt, null);
    } catch (error) {
      this.report('checkpoint', pod.metadata.labels, pod, startedAt, error);
      throw error;
    } finally {
      await this.kubernetes.request('PATCH', `${this.pods}/${pod.metadata.name}`, {
        metadata: { annotations: { [SLEEPING_ANNOTATION]: '0' } },
      });
    }
  }

  async sleepUnused() {
    const idleBefore = Date.now() - this.idleTimeoutMs;
    const [sandboxes, leases] = await Promise.all([
      this.list({}),
      this.kubernetes.request('GET', `${this.leases}?labelSelector=${this.selector({})}`),
    ]);
    const renewed = new Map(
      listOf(LeaseSchema)
        .parse(leases)
        .items.map((lease) => [lease.metadata.name, renewedMs(lease)]),
    );
    // All at once: a sleep waits on its snapshot, and one request sweeps the whole cluster, so
    // sleeping in turn reaches only the first few of many idle sandboxes before its deadline.
    await Promise.all(
      sandboxes.map(async ({ pod }) => {
        if (!pod || sleeping(pod) || pod.status.phase !== 'Running') return;
        if (Number(pod.metadata.annotations[ACQUIRED_ANNOTATION]) > idleBefore) return;
        if ((renewed.get(pod.metadata.name) ?? 0) > idleBefore) return;
        // One sandbox that cannot sleep must not keep the rest awake; the next sweep retries it.
        // A failed sleep has been reported, and one in use is no failure.
        await this.sleepPod(pod).catch(() => undefined);
      }),
    );
  }

  /**
   * Every sandbox that exists, awake or asleep, with whose it is and where its snapshots are
   * stored. Usage is metered by pod and by stored snapshot, and neither names an owner.
   */
  async inventory() {
    const query = `?labelSelector=${this.selector({})},${SANDBOX_LABEL}`;
    const [pods, snapshots] = await Promise.all([
      this.kubernetes.request('GET', `${this.pods}${query}`),
      this.kubernetes.request('GET', `${this.snapshotsPath}${query}`),
    ]);
    const sandboxes = new Map<
      string,
      {
        sandboxId: string;
        labels: Labels;
        state: 'running' | 'stopped';
        snapshots: { path: string; createdAt: string }[];
      }
    >();
    const sandbox = ({ labels }: z.infer<typeof MetadataSchema>) => {
      const sandboxId = labels[SANDBOX_LABEL]!;
      const found = sandboxes.get(sandboxId) ?? {
        sandboxId,
        labels,
        state: 'stopped' as const,
        snapshots: [],
      };
      sandboxes.set(sandboxId, found);
      return found;
    };
    for (const pod of listOf(PodSchema).parse(pods).items) {
      const found = sandbox(pod.metadata);
      // A pod that is going to sleep no longer accepts work.
      if (pod.status.phase === 'Running' && !sleeping(pod)) found.state = 'running';
    }
    // A snapshot still being written has no stored files yet.
    for (const item of listOf(z.unknown()).parse(snapshots).items) {
      const stored = StoredSnapshotSchema.safeParse(item);
      if (stored.success)
        sandbox(stored.data.metadata).snapshots.push({
          path: stored.data.status.storageStatus.gcs.observedGCSPath,
          createdAt: stored.data.metadata.creationTimestamp,
        });
    }
    return [...sandboxes.values()];
  }

  async deleteMatching(match: Labels) {
    if (!Object.keys(match).some((key) => !(key in this.manager)))
      throw new Error('A deletion selector is required.');
    const labels = this.selector(match);
    await Promise.all(
      [this.pods, this.snapshotsPath, this.triggers].map((path) =>
        this.kubernetes.request('DELETE', `${path}?labelSelector=${labels}`),
      ),
    );
  }

  async find(id: string) {
    sandboxIdSchema.parse(id);
    return (await this.list({ [SANDBOX_LABEL]: id }))[0] ?? null;
  }

  /** Every sandbox with these labels, awake or asleep, newest first. */
  async list(labels: Record<string, string>) {
    const query = `?labelSelector=${this.selector(labels)},${SANDBOX_LABEL}`;
    const [pods, snapshots] = await Promise.all([
      this.kubernetes.request('GET', `${this.pods}${query}`),
      this.kubernetes.request('GET', `${this.snapshotsPath}${query}`),
    ]);
    const sandboxes = new Map<string, SandboxRecord>();
    const sandbox = (metadata: z.infer<typeof MetadataSchema>) => {
      const name = metadata.labels[SANDBOX_LABEL]!;
      const found = sandboxes.get(name) ?? {
        name,
        labels: metadata.labels,
        createdMs: Number(metadata.labels[CREATED_LABEL]),
        pod: null,
        snapshots: [],
        sleptMs: null,
      };
      sandboxes.set(name, found);
      return found;
    };
    // A woken sandbox gets a new pod while its previous one may still be shutting down.
    for (const pod of listOf(PodSchema).parse(pods).items.sort(oldestLiveFirst).reverse())
      sandbox(pod.metadata).pod = pod;
    for (const snapshot of listOf(SnapshotSchema)
      .parse(snapshots)
      .items.filter(({ status }) => ready(status.conditions))
      .sort((left, right) =>
        right.metadata.creationTimestamp.localeCompare(left.metadata.creationTimestamp),
      )) {
      const found = sandbox(snapshot.metadata);
      // A sandbox recreated under the same id must not wake from its predecessor's snapshot.
      if (snapshot.metadata.labels[CREATED_LABEL] !== found.labels[CREATED_LABEL]) continue;
      found.sleptMs ??= Date.parse(snapshot.metadata.creationTimestamp);
      found.snapshots.push(snapshot.metadata.name);
    }
    return [...sandboxes.values()].sort((left, right) => right.createdMs - left.createdMs);
  }

  private async livePods(sandboxId: string) {
    return listOf(PodSchema).parse(
      await this.kubernetes.request(
        'GET',
        `${this.pods}?labelSelector=${this.selector({ [SANDBOX_LABEL]: sandboxId })}`,
      ),
    ).items;
  }

  /** The sandbox's pod to use: the one that is not shutting down, or null. */
  private async livePod(sandboxId: string) {
    return (await this.livePods(sandboxId)).sort(oldestLiveFirst)[0] ?? null;
  }

  /** Returns the sandbox's running pod, waking or creating it first. */
  private async start(sandbox: SandboxRecord, signal: AbortSignal, initialLabels: Labels) {
    const id = sandbox.name;
    let pod = sandbox.pod;
    let snapshot = sandbox.snapshots[0];
    // A sleep ends with the pod shutting down, and a checkpoint with the pod free again. One that
    // was abandoned ends when its mark expires, so the wait lasts until then and needs no deadline
    // of its own: a shorter one would fail the dispatch moments before the pod is usable.
    while (pod && held(pod)) {
      await this.kubernetes.watch(
        this.pods,
        pod.metadata.name,
        (found) => (found && held(PodSchema.parse(found)) ? undefined : true),
        Number(pod.metadata.annotations[SLEEPING_ANNOTATION]) + SLEEP_ABANDONED_MS,
        signal,
      );
      const [current] = await this.list({ [SANDBOX_LABEL]: id });
      pod = current?.pod ?? null;
      snapshot = current?.snapshots[0];
      if (current) sandbox.labels = current.labels;
    }
    const startedAt = Date.now();
    let created = false;
    while (true) {
      signal.throwIfAborted();
      if (pod && (terminating(pod) || !['Running', 'Pending'].includes(pod.status.phase))) {
        // The pod's last snapshot may be newer than the listing. One of another creation under
        // this id belongs to a sandbox that was replaced, not to this one.
        const [listed] = await this.list({ [SANDBOX_LABEL]: id });
        const current =
          listed?.labels[CREATED_LABEL] === sandbox.labels[CREATED_LABEL] ? listed : undefined;
        snapshot = current?.snapshots[0];
        if (!terminating(pod)) {
          // Everything written since that snapshot is gone with the pod. The report comes before
          // the pod is deleted, so a start that fails here reports the loss when it is retried.
          const reason =
            pod.status.containerStatuses.find(({ state }) => state?.terminated?.reason)?.state
              ?.terminated?.reason ??
            pod.status.reason ??
            pod.status.phase;
          // `sleptMs` is when the newest snapshot, the one this start restores, was taken.
          const takenMs = snapshot ? current?.sleptMs : null;
          this.options.onPodLost?.(
            sandbox.labels,
            pod.status.phase,
            reason,
            !!snapshot,
            takenMs != null && Number.isFinite(takenMs)
              ? {
                  recoveryPointAt: new Date(takenMs).toISOString(),
                  recoveryPointAgeMs: Math.max(0, Date.now() - takenMs),
                }
              : {},
          );
          await this.kubernetes.request('DELETE', `${this.pods}/${pod.metadata.name}`);
        }
        pod = null;
      }
      if (pod) break;
      // A fresh filesystem must also get the caller's initial labels.
      if (!snapshot) sandbox.labels = initialLabels;
      // Named after the snapshot it starts from. Kubernetes creates a name once, so callers that
      // start the sandbox at the same moment get one pod between them. And a wake never waits
      // for the pod it replaces to finish shutting down, which started from an earlier snapshot.
      const name = `${id}-${createHash('sha256')
        .update(snapshot ?? '')
        .digest('hex')
        .slice(0, 6)}`;
      try {
        pod = PodSchema.parse(await this.createPod(this.manifest(sandbox, name, snapshot)));
        created = true;
        break;
      } catch (error) {
        if (!(error instanceof KubernetesApiError && error.status === 409)) throw error;
      }
      // The name is taken. By a pod another caller just created, which is this caller's too. Or by
      // one that started from the same snapshot and died or was deleted: Kubernetes removes a
      // dead pod at once, and one still shutting down has to go before its name is free.
      const taken = await this.kubernetes.watch(
        this.pods,
        name,
        (found) => {
          const current = found ? PodSchema.parse(found) : null;
          return current && terminating(current) ? undefined : current;
        },
        startedAt + START_SECONDS * 1000,
        signal,
      );
      if (taken === undefined) throw new Error(`Sandbox ${id} did not finish starting.`);
      pod = taken;
    }
    // Only the caller whose pod it is reports the start; one that waits for another's does not.
    const operation = created ? (snapshot ? 'wake' : 'create') : null;
    const createdAt = Date.now();
    try {
      if (starting(pod)) {
        // The pod as it is once it has started or can no longer start, or null once it is gone.
        const started = await this.kubernetes.watch(
          this.pods,
          pod.metadata.name,
          (found) => {
            const current = found ? PodSchema.parse(found) : null;
            return current && starting(current) ? undefined : current;
          },
          startedAt + START_SECONDS * 1000,
          signal,
        );
        if (started === undefined) throw new Error(`Sandbox ${id} did not finish starting.`);
        pod = started;
      }
      if (pod?.status.phase !== 'Running') throw new Error(`Sandbox ${id} failed to start.`);
      // A second caller can see a restored pod before its creator has checked the files.
      if (
        pod.metadata.annotations[RESTORE_ANNOTATION] &&
        !pod.metadata.annotations[RESTORED_ANNOTATION]
      )
        await this.confirmRestored(pod, id);
    } catch (error) {
      if (operation) this.report(operation, sandbox.labels, pod, createdAt, error, signal);
      throw error;
    }
    if (operation) this.report(operation, sandbox.labels, pod, createdAt, null);
    return { pod, fresh: created };
  }

  private report(
    operation: 'create' | 'wake' | 'sleep' | 'checkpoint',
    labels: Record<string, string>,
    pod: Pod | null,
    startedAt: number,
    error: unknown,
    signal?: AbortSignal,
  ) {
    const outcome = !error ? 'succeeded' : signal?.aborted ? 'aborted' : 'failed';
    const event = {
      operation,
      outcome,
      labels,
      pod: pod?.metadata.name,
      podIp: pod?.status.podIP,
      durationMs: Date.now() - startedAt,
      error,
    } satisfies SandboxEvent;
    if (this.options.onOperation) this.options.onOperation(event);
    else if (event.outcome === 'failed') console.error(event);
  }

  // GKE starts a pod empty when it cannot restore the requested snapshot. Using that pod would
  // replace saved files with an empty filesystem. Keep the snapshot for the next wake.
  private async confirmRestored(pod: Pod, id: string) {
    const path = `${this.pods}/${pod.metadata.name}`;
    const address = this.commandAddress(pod);
    const check = new TextEncoder().encode(
      `test "$(cat ${quote(SANDBOX_ID_FILE)} 2>/dev/null)" = ${quote(id)}`,
    );
    // GKE fills in the files as the container starts, so the first looks can come too early.
    for (let attempt = 0; attempt < RESTORE_CHECKS; attempt++) {
      const { exitCode } = await (address
        ? directExec(address, execCommand(30), check, 30, {}, true)
        : this.kubernetes.exec(path, execCommand(30), check, 30));
      if (exitCode === 0) {
        await this.kubernetes.request('PATCH', path, {
          metadata: { annotations: { [RESTORED_ANNOTATION]: 'true' } },
        });
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, RESTORE_CHECK_MS));
    }
    await this.kubernetes.request('DELETE', `${this.pods}/${pod.metadata.name}`);
    throw new Error(`Sandbox ${id} was not restored from its snapshot.`);
  }

  /** Where the pod's command server is, or null for a pod that is reached through the API. */
  private commandAddress(pod: Pod): CommandServerAddress | null {
    const { annotations } = pod.metadata;
    const token = annotations[COMMAND_TOKEN_ANNOTATION];
    const port = Number(annotations[COMMAND_PORT_ANNOTATION]);
    return this.options.directCommands && token && port && pod.status.podIP
      ? { host: pod.status.podIP, port, token }
      : null;
  }

  /** Creates a pod, after the command server it mounts, once per process. */
  private async createPod(manifest: unknown) {
    if (this.options.directCommands)
      await (this.commandServer ??= this.kubernetes
        .request('POST', this.configMaps, {
          apiVersion: 'v1',
          kind: 'ConfigMap',
          metadata: { name: COMMAND_SERVER_CONFIG, labels: this.manager },
          binaryData: Object.fromEntries(
            Object.entries(COMMAND_SERVER.binaries).map(([machine, binary]) => [
              `command-server-${machine}`,
              binary,
            ]),
          ),
        })
        .then(
          () => undefined,
          (error) => {
            // Another process, or an earlier run of this one, already published this build.
            if (error instanceof KubernetesApiError && error.status === 409) return;
            this.commandServer = undefined;
            throw error;
          },
        ));
    return this.kubernetes.request('POST', this.pods, manifest);
  }

  private manifest(
    sandbox: Pick<SandboxRecord, 'name' | 'labels'>,
    name: string,
    snapshot: string | undefined,
  ) {
    const { directCommands } = this.options;
    return {
      apiVersion: 'v1',
      kind: 'Pod',
      metadata: {
        name,
        labels: sandbox.labels,
        annotations: {
          [ACQUIRED_ANNOTATION]: String(Date.now()),
          [SLEEPING_ANNOTATION]: '0',
          'kubernetes.io/egress-bandwidth': EGRESS_BANDWIDTH,
          // gVisor gives the root filesystem this size. Without it the filesystem has none: `df`
          // reports 8 EiB free, and the only limit is the kubelet's, which evicts the pod. gVisor
          // cannot size the scratch volume at /tmp, so the kubelet's limit still bounds that.
          'dev.gvisor.flag.overlay2': `root:self,size=${this.diskBytes}`,
          ...(snapshot ? { [RESTORE_ANNOTATION]: snapshot } : {}),
          ...(directCommands
            ? {
                [COMMAND_PORT_ANNOTATION]: String(directCommands.port),
                [COMMAND_TOKEN_ANNOTATION]: randomBytes(32).toString('hex'),
              }
            : {}),
        },
      },
      spec: {
        runtimeClassName: 'gvisor',
        serviceAccountName: this.options.serviceAccount,
        automountServiceAccountToken: false,
        enableServiceLinks: false,
        // A restarted container would start from the image, silently without the sandbox's files.
        restartPolicy: 'Never',
        terminationGracePeriodSeconds: 5,
        volumes: [
          // A rootfs-only snapshot leaves volumes out, so /tmp is scratch: on the node's disk while
          // the pod lives, empty in a woken pod, and never uploaded with a snapshot.
          { name: 'scratch', emptyDir: {} },
          ...(this.options.runtime
            ? [
                {
                  name: 'runtime',
                  image: {
                    reference: this.options.runtime.image,
                    pullPolicy: 'IfNotPresent',
                  },
                },
              ]
            : []),
          ...(directCommands
            ? [
                {
                  name: 'command-server',
                  configMap: { name: COMMAND_SERVER_CONFIG, defaultMode: 0o555 },
                },
              ]
            : []),
        ],
        containers: [
          {
            name: 'sandbox',
            image: this.options.image,
            command: podCommand(
              sandbox.name,
              !snapshot,
              this.options.boot?.(sandbox.labels) ?? null,
              this.options.memoryWatchdog === false
                ? null
                : { protect: this.options.memoryWatchdog?.protect ?? [] },
              !!directCommands,
            ),
            // The pod's own annotations, so the pod template is the same for every pod.
            ...(directCommands
              ? {
                  env: [
                    ['GKE_SANDBOXES_COMMAND_TOKEN', COMMAND_TOKEN_ANNOTATION],
                    ['GKE_SANDBOXES_COMMAND_PORT', COMMAND_PORT_ANNOTATION],
                  ].map(([name, annotation]) => ({
                    name,
                    valueFrom: { fieldRef: { fieldPath: `metadata.annotations['${annotation}']` } },
                  })),
                }
              : {}),
            volumeMounts: [
              { name: 'scratch', mountPath: '/tmp' },
              ...(this.options.runtime
                ? [
                    {
                      name: 'runtime',
                      mountPath: this.options.runtime.mountPath,
                      readOnly: true,
                    },
                  ]
                : []),
              ...(directCommands
                ? [{ name: 'command-server', mountPath: COMMAND_SERVER_MOUNT, readOnly: true }]
                : []),
            ],
            resources: this.options.resources ?? DEFAULT_RESOURCES,
          },
        ],
      },
    };
  }

  // A dispatch and the sweep both claim a pod by patching it on the condition that the other's
  // annotation is unchanged since they read it. The pod's resource version cannot serve: the
  // kubelet's status updates change it all the time.
  private claim(pod: Pod, patch: { op: 'test' | 'replace'; path: string; value: string }[]) {
    return this.kubernetes.request('PATCH', `${this.pods}/${pod.metadata.name}`, patch).then(
      (result) => result !== null,
      (error) => {
        if (error instanceof KubernetesApiError && [409, 422].includes(error.status)) return false;
        throw error;
      },
    );
  }

  /** Records the dispatch. Returns false when the sweep claimed the pod first. */
  private markAcquired(pod: Pod) {
    return this.claim(pod, [
      { op: 'test', path: SLEEPING_PATH, value: pod.metadata.annotations[SLEEPING_ANNOTATION]! },
      { op: 'replace', path: SLEEPING_PATH, value: '0' },
      { op: 'replace', path: ACQUIRED_PATH, value: String(Date.now()) },
    ]);
  }

  /** Reports that the sandbox is in use, which restarts its idle period. */
  private async touch(sandboxId: string) {
    const pod = await this.livePod(sandboxId);
    if (!pod) return;
    const { name, uid } = pod.metadata;
    // Kubernetes reads this time only with six decimals.
    const spec = { renewTime: new Date().toISOString().replace('Z', '000Z') };
    if (await this.kubernetes.request('PATCH', `${this.leases}/${name}`, { spec })) return;
    // The pod owns its lease, so Kubernetes deletes the lease with the pod.
    try {
      await this.kubernetes.request('POST', this.leases, {
        apiVersion: 'coordination.k8s.io/v1',
        kind: 'Lease',
        metadata: {
          name,
          labels: this.manager,
          ownerReferences: [{ apiVersion: 'v1', kind: 'Pod', name, uid }],
        },
        spec,
      });
    } catch (error) {
      // Another report created it at the same moment, with the same time.
      if (!(error instanceof KubernetesApiError && error.status === 409)) throw error;
    }
  }

  async sleep(id: string) {
    const pod = await this.livePod(id);
    if (!pod || terminating(pod)) return;
    if (held(pod)) throw new SandboxInUseError(id);
    await this.sleepPod(pod);
  }

  private async sleepPod(pod: Pod) {
    const name = pod.metadata.labels[SANDBOX_LABEL]!;
    const path = `${this.pods}/${pod.metadata.name}`;
    const acquired = pod.metadata.annotations[ACQUIRED_ANNOTATION]!;
    if (Date.now() - Number(acquired) < this.idleTimeoutMs) throw new SandboxInUseError(name);
    const claimed = await this.claim(pod, [
      { op: 'test', path: ACQUIRED_PATH, value: acquired },
      { op: 'test', path: SLEEPING_PATH, value: pod.metadata.annotations[SLEEPING_ANNOTATION]! },
      { op: 'replace', path: SLEEPING_PATH, value: String(Date.now()) },
    ]);
    if (!claimed) throw new SandboxInUseError(name);
    const startedAt = Date.now();
    try {
      // Read after the claim, so a report of use made while the sweep was deciding still counts.
      const renewed = renewedMs(
        await this.kubernetes.request('GET', `${this.leases}/${pod.metadata.name}`),
      );
      if (Date.now() - renewed < this.idleTimeoutMs) throw new SandboxInUseError(name);
      if (this.options.drain && !(await this.options.drain(this.environment(name, 'started'))))
        throw new SandboxInUseError(name);
      const snapshot = await this.snapshot(pod);
      await this.kubernetes.request('DELETE', path, {
        preconditions: { uid: pod.metadata.uid },
      });
      await this.dropOlderSnapshots(name, snapshot);
      this.report('sleep', pod.metadata.labels, pod, startedAt, null);
    } catch (error) {
      // A service that still has work declined; that is no failed sleep.
      if (!(error instanceof SandboxInUseError))
        this.report('sleep', pod.metadata.labels, pod, startedAt, error);
      await this.kubernetes.request('PATCH', path, {
        metadata: { annotations: { [SLEEPING_ANNOTATION]: '0' } },
      });
      throw error;
    }
  }

  // A pod restores from its newest snapshot; older ones only cost storage.
  private async dropOlderSnapshots(sandboxId: string, newest: string) {
    const { items } = listOf(SnapshotSchema).parse(
      await this.kubernetes.request(
        'GET',
        `${this.snapshotsPath}?labelSelector=${this.selector({ [SANDBOX_LABEL]: sandboxId })}`,
      ),
    );
    await Promise.all(
      items
        .filter(({ metadata }) => metadata.name !== newest)
        .map(({ metadata }) =>
          this.kubernetes.request('DELETE', `${this.snapshotsPath}/${metadata.name}`),
        ),
    );
  }

  private async snapshot(pod: Pod) {
    // An interrupted wake can leave a running pod whose files nobody has verified.
    if (
      pod.metadata.annotations[RESTORE_ANNOTATION] &&
      !pod.metadata.annotations[RESTORED_ANNOTATION]
    )
      await this.confirmRestored(pod, pod.metadata.labels[SANDBOX_LABEL]!);
    const trigger = `${pod.metadata.name}-${Date.now()}`;
    const labels = pod.metadata.labels;
    await this.kubernetes.request('POST', this.triggers, {
      apiVersion: 'podsnapshot.gke.io/v1',
      kind: 'PodSnapshotManualTrigger',
      metadata: { name: trigger, labels },
      spec: { targetPod: pod.metadata.name },
    });
    try {
      const deadline = Date.now() + SNAPSHOT_SECONDS * 1000;
      const inTime = <T>(found: T | undefined) => {
        if (found === undefined)
          throw new Error(`Sandbox ${pod.metadata.name} was not snapshotted in time.`);
        return found;
      };
      // The trigger names its snapshot, and the snapshot then reports that its files are stored.
      const name = inTime(
        await this.kubernetes.watch(
          this.triggers,
          trigger,
          (found) => {
            const { status } = TriggerSchema.parse(found);
            const failed = status.conditions.find(
              ({ type, status }) => type === 'Triggered' && status === 'False',
            );
            if (failed)
              throw new Error(`Sandbox ${pod.metadata.name} snapshot failed: ${failed.message}`);
            return status.snapshotCreated?.name;
          },
          deadline,
          null,
        ),
      );
      inTime(
        await this.kubernetes.watch(
          this.snapshotsPath,
          name,
          (found) => ready(SnapshotSchema.parse(found).status.conditions) || undefined,
          deadline,
          null,
        ),
      );
      return name;
    } finally {
      await this.kubernetes.request('DELETE', `${this.triggers}/${trigger}`);
    }
  }

  async delete(id: string) {
    sandboxIdSchema.parse(id);
    await this.deleteMatching({ [SANDBOX_LABEL]: id });
  }

  environment(name: string, state: 'started' | 'stopped'): SandboxEnvironment {
    return this.makeEnvironment(name, state, null);
  }

  private makeEnvironment(
    name: string,
    state: 'started' | 'stopped',
    firstPod: Pod | null,
  ): SandboxEnvironment {
    // Only the immediate handoff can use the claim directly. A handle kept over I/O
    // may have slept and woken in another pod, even before its first command.
    if (firstPod)
      setImmediate(() => {
        firstPod = null;
      }).unref();
    // The command server this handle last reached. The pod need not be looked up again for every
    // command: the server must show that it holds the pod's token before it is told what to run,
    // so a pod that is gone, or whose address another pod has taken, is sent nothing and runs
    // nothing. The sandbox's pod is then looked up as for a first command.
    let reached: CommandServerAddress | null = null;
    const run = async (
      command: string[],
      stdin: Uint8Array,
      timeout: number,
      options: ExecOptions = {},
    ) => {
      if (reached)
        try {
          return await directExec(reached, command, stdin, timeout, options, false);
        } catch (error) {
          if (!(error instanceof CommandServerUnreachableError)) throw error;
          reached = null;
        }
      const pod = firstPod ?? (await this.livePod(name));
      firstPod = null;
      if (!pod) throw new Error(`Sandbox ${name} is not running.`);
      const address = this.commandAddress(pod);
      if (!address)
        return this.kubernetes.exec(
          `${this.pods}/${pod.metadata.name}`,
          command,
          stdin,
          timeout,
          options,
        );
      const result = await directExec(address, command, stdin, timeout, options, true);
      reached = address;
      return result;
    };
    return {
      id: name,
      state,
      process: {
        executeCommand: async (command, cwd = '/', env = {}, timeout = 60, options = {}) => {
          const script = [
            `cd ${quote(cwd)} || exit 1`,
            ...Object.entries(env).map(([key, value]) => {
              if (!/^[a-zA-Z_][a-zA-Z_0-9]*$/.test(key))
                throw new Error('Invalid environment variable name.');
              return `export ${key}=${quote(value)}`;
            }),
            command,
          ].join('\n');
          const { exitCode, output, truncated } = await run(
            execCommand(timeout),
            new TextEncoder().encode(script),
            timeout,
            options,
          );
          return { exitCode, result: output, truncated };
        },
      },
      fs: {
        uploadFile: async (local, remote, timeout = 300) => {
          const { exitCode, output } = await run(
            ['sh', '-c', 'cat > "$0"', remote],
            await readFile(local),
            timeout,
          );
          if (exitCode !== 0) throw new Error(`Could not upload ${remote}: ${output}`);
        },
        downloadFile: async (remote, local, timeout = 300) => {
          const file = createWriteStream(local);
          const errors: Uint8Array[] = [];
          // The file's bytes go straight to disk; only what `cat` complains about is kept.
          const { exitCode } = await run(
            ['sh', '-c', 'cat "$0"', remote],
            new Uint8Array(),
            timeout,
            {
              maxOutputBytes: 0,
              onOutput: (chunk, stream) =>
                stream === 'stdout' ? file.write(chunk.slice()) : errors.push(chunk.slice()),
            },
          ).finally(() => file.end());
          await finished(file);
          if (exitCode === 0) return;
          await rm(local, { force: true });
          throw new Error(
            `Could not download ${remote}: ${Buffer.concat(errors).toString('utf8').trim()}`,
          );
        },
      },
    };
  }
}

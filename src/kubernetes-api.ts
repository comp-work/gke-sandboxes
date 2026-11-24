import { z } from 'zod';

const ExecStatusSchema = z.object({
  status: z.enum(['Success', 'Failure']),
  message: z.string().optional(),
  details: z
    .object({ causes: z.array(z.object({ reason: z.string(), message: z.string() })).optional() })
    .optional(),
});

// Kubernetes multiplexes an exec session's streams over one WebSocket: the first byte of every
// frame names the stream. Version 5 adds the close frame that ends stdin.
const EXEC_PROTOCOL = 'v5.channel.k8s.io';
const STDIN = 0;
const STDOUT = 1;
const STDERR = 2;
const STATUS = 3;
const CLOSE = 255;
const STDIN_CHUNK_BYTES = 256 * 1024;

const GONE = 410;
const WatchedSchema = z.object({ metadata: z.object({ resourceVersion: z.string() }) });
const WatchEventSchema = z.object({
  type: z.enum(['ADDED', 'MODIFIED', 'DELETED', 'BOOKMARK', 'ERROR']),
  object: z.unknown(),
});
// What an `ERROR` event carries.
const WatchStatusSchema = z.object({ code: z.number(), message: z.string().default('') });

export class KubernetesApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = 'KubernetesApiError';
  }
}

export type KubernetesExecResult = { exitCode: number; output: string; truncated: boolean };
export type ExecOptions = {
  /** Called with each piece of output as it arrives, before any cap is applied. */
  onOutput?: (chunk: Uint8Array, stream: 'stdout' | 'stderr') => void;
  /** How much output to keep for the result. More than this is dropped and `truncated` is set. */
  maxOutputBytes?: number;
};
// A command that prints without end must not exhaust the caller's memory.
export const DEFAULT_MAX_OUTPUT_BYTES = 32 * 2 ** 20;

/** Gathers a command's output as it arrives, for the result every way of running one returns. */
export const collectOutput = ({
  onOutput,
  maxOutputBytes = DEFAULT_MAX_OUTPUT_BYTES,
}: ExecOptions) => {
  const output: Uint8Array[] = [];
  let kept = 0;
  let truncated = false;
  return {
    add(payload: Uint8Array, stream: 'stdout' | 'stderr') {
      onOutput?.(payload, stream);
      const fits = payload.subarray(0, Math.max(0, maxOutputBytes - kept));
      if (fits.byteLength) output.push(fits);
      kept += fits.byteLength;
      truncated ||= fits.byteLength < payload.byteLength;
    },
    result: (exitCode: number): KubernetesExecResult => ({
      exitCode,
      output: Buffer.concat(output).toString('utf8'),
      truncated,
    }),
  };
};

/** The few Kubernetes API calls sandboxes need: JSON resources, watches and exec. */
export class KubernetesApi {
  constructor(
    private readonly baseUrl: string,
    private readonly accessToken: () => Promise<string>,
  ) {}

  /**
   * Returns `null` for a missing resource and throws `KubernetesApiError` for other failures. A
   * POST has no resource to miss: its 404 means the namespace or resource type does not exist. A
   * PATCH body is a JSON Patch when it is an array and a merge patch otherwise.
   */
  async request(
    method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
    path: string,
    body?: unknown,
  ): Promise<unknown> {
    const response = await fetch(new URL(path, this.baseUrl), {
      method,
      headers: {
        authorization: `Bearer ${await this.accessToken()}`,
        ...(body === undefined
          ? {}
          : {
              'content-type':
                method !== 'PATCH'
                  ? 'application/json'
                  : Array.isArray(body)
                    ? 'application/json-patch+json'
                    : 'application/merge-patch+json',
            }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: 'error',
      signal: AbortSignal.timeout(60_000),
    });
    if (response.status === 404 && method !== 'POST') return null;
    if (!response.ok)
      throw new KubernetesApiError(
        response.status,
        `Kubernetes ${method} ${path} failed (HTTP ${response.status}): ${(await response.text()).slice(0, 500)}`,
      );
    return response.json();
  }

  /**
   * Waits for the resource `name` of `collection` to become what `until` accepts, and returns
   * what `until` returned for it. `until` is given the resource, or `null` while there is none,
   * and returns `undefined` to keep waiting. The resource is read once and then watched, so the
   * server reports each change as it happens.
   *
   * Returns `undefined` when `deadline`, a time in milliseconds, passes first, and rejects with
   * the signal's reason when it aborts.
   */
  async watch<T>(
    collection: string,
    name: string,
    until: (resource: unknown) => T | undefined,
    deadline: number,
    signal: AbortSignal | null,
  ): Promise<T | undefined> {
    const expired = AbortSignal.timeout(Math.max(0, deadline - Date.now()));
    const ended = signal ? AbortSignal.any([signal, expired]) : expired;
    // The server ends a watch whenever it likes, and refuses one from a version it no longer
    // remembers. The resource is then read again and watched from the version it has by then, so
    // no change is missed in between.
    while (true) {
      const resource = await this.request('GET', `${collection}/${name}`);
      const found = until(resource);
      if (found !== undefined) return found;
      const url = new URL(collection, this.baseUrl);
      url.searchParams.set('watch', '1');
      url.searchParams.set('fieldSelector', `metadata.name=${name}`);
      // Without a version the server starts from what exists now, which is nothing.
      if (resource)
        url.searchParams.set(
          'resourceVersion',
          WatchedSchema.parse(resource).metadata.resourceVersion,
        );
      for await (const event of this.events(url, ended)) {
        if (event.type === 'BOOKMARK') continue;
        if (event.type === 'ERROR') {
          const status = WatchStatusSchema.parse(event.object);
          if (status.code === GONE) break;
          throw new KubernetesApiError(
            status.code,
            `Kubernetes watch of ${collection}/${name} failed (HTTP ${status.code}): ${status.message.slice(0, 500)}`,
          );
        }
        const changed = until(event.type === 'DELETED' ? null : event.object);
        if (changed !== undefined) return changed;
      }
      signal?.throwIfAborted();
      if (expired.aborted) return undefined;
    }
  }

  /** One watch's events, until the server ends it, the connection breaks or `ended` aborts. */
  private async *events(url: URL, ended: AbortSignal) {
    // Whoever stops reading closes the connection.
    const closed = new AbortController();
    try {
      const response = await fetch(url, {
        headers: { authorization: `Bearer ${await this.accessToken()}` },
        redirect: 'error',
        signal: AbortSignal.any([ended, closed.signal]),
      }).catch((error: unknown) => {
        if (ended.aborted) return null;
        throw error;
      });
      if (!response || response.status === GONE) return;
      if (!response.ok || !response.body)
        throw new KubernetesApiError(
          response.status,
          `Kubernetes watch ${url.pathname} failed (HTTP ${response.status}): ${(await response.text()).slice(0, 500)}`,
        );
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      // One event per line; a line may arrive in pieces.
      let partial = '';
      while (true) {
        // A connection that breaks is a watch that ended early.
        const chunk = await reader.read().catch(() => null);
        if (!chunk || chunk.done) return;
        const lines = (partial + decoder.decode(chunk.value, { stream: true })).split('\n');
        partial = lines.pop()!;
        for (const line of lines) if (line) yield WatchEventSchema.parse(JSON.parse(line));
      }
    } finally {
      closed.abort();
    }
  }

  /** Runs a command in a pod's container, feeding it `stdin` and collecting its output. */
  async exec(
    podPath: string,
    command: string[],
    stdin: Uint8Array,
    timeoutSeconds: number,
    options: ExecOptions = {},
  ): Promise<KubernetesExecResult> {
    const url = new URL(`${podPath}/exec`, this.baseUrl);
    url.protocol = 'wss:';
    for (const stream of ['stdin', 'stdout', 'stderr']) url.searchParams.set(stream, 'true');
    for (const argument of command) url.searchParams.append('command', argument);
    // Bun's and Node's WebSocket accept request headers in an options object; the standard
    // constructor has no way to send them.
    const socket = new WebSocket(url, {
      headers: { authorization: `Bearer ${await this.accessToken()}` },
      protocols: [EXEC_PROTOCOL],
    } as unknown as string[]);
    socket.binaryType = 'arraybuffer';
    const finished = Promise.withResolvers<KubernetesExecResult>();
    const collected = collectOutput(options);
    const timer = setTimeout(() => {
      finished.reject(new Error(`Sandbox command timed out after ${timeoutSeconds} seconds.`));
      socket.close();
    }, timeoutSeconds * 1000);
    socket.onopen = () => {
      for (let offset = 0; offset < stdin.byteLength; offset += STDIN_CHUNK_BYTES) {
        const chunk = stdin.subarray(offset, offset + STDIN_CHUNK_BYTES);
        const frame = new Uint8Array(chunk.byteLength + 1);
        frame[0] = STDIN;
        frame.set(chunk, 1);
        socket.send(frame);
      }
      socket.send(new Uint8Array([CLOSE, STDIN]));
    };
    socket.onmessage = ({ data }) => {
      const frame = new Uint8Array(data as ArrayBuffer);
      const payload = frame.subarray(1);
      if (frame[0] === STDOUT || frame[0] === STDERR)
        collected.add(payload, frame[0] === STDOUT ? 'stdout' : 'stderr');
      if (frame[0] !== STATUS) return;
      // GKE sends the final status after stdout and stderr. Node can keep the socket
      // open after this frame, so waiting for a close event can time out a finished command.
      try {
        const status = ExecStatusSchema.parse(JSON.parse(new TextDecoder().decode(payload)));
        const exitCode = status.details?.causes?.find(({ reason }) => reason === 'ExitCode');
        if (status.status === 'Failure' && !exitCode) {
          finished.reject(new Error(`Sandbox command failed: ${status.message}`));
        } else {
          finished.resolve(collected.result(exitCode ? Number(exitCode.message) : 0));
        }
      } catch (error) {
        finished.reject(
          new Error('Sandbox command returned an unreadable status.', { cause: error }),
        );
      }
      socket.close();
    };
    socket.onerror = () => finished.reject(new Error('Sandbox command connection failed.'));
    socket.onclose = () => finished.reject(new Error('Sandbox command ended without a status.'));
    return finished.promise.finally(() => clearTimeout(timer));
  }
}

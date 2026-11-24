import { timingSafeEqual } from 'node:crypto';
import { connect, type Socket } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { collectOutput, type ExecOptions, type KubernetesExecResult } from './kubernetes-api.js';

// The frames of command-server/command-server.c: a type, a length and a payload.
const STDIN = 0;
const STDOUT = 1;
const STDERR = 2;
const EXIT = 3;
const ACCEPTED = 4;
const AUTH = 16;
const ARGV = 17;
const CLOSE = 255;
const STDIN_CHUNK_BYTES = 256 * 1024;
const MAX_ARGV_BYTES = 1024 * 1024;
const MAX_ARGUMENTS = 256;
// A pod reports ready a moment before its command server listens.
const LISTEN_WAIT_MS = 5000;
const CONNECT_TIMEOUT_MS = 5000;
// An address remembered from an earlier command answers at once or belongs to no pod any more.
const REMEMBERED_CONNECT_TIMEOUT_MS = 1000;
// How long the server may take to show that it is the pod the token belongs to.
const ACCEPT_TIMEOUT_MS = 5000;

export type CommandServerAddress = { host: string; port: number; token: string };

/**
 * No command ran: nothing answered at the address, or what answered is not the pod the token
 * belongs to. Pod addresses are reused, so an address remembered from earlier fails this way once
 * its pod is gone.
 */
export class CommandServerUnreachableError extends Error {
  constructor(address: CommandServerAddress, reason: string) {
    super(
      `Could not reach the sandbox command server at ${address.host}:${address.port} (${reason}). The caller must be on the pod network and allowed in by the sandboxes' network policy.`,
    );
    this.name = 'CommandServerUnreachableError';
  }
}

const frame = (type: number, payload: Uint8Array) => {
  const header = Buffer.alloc(5);
  header[0] = type;
  header.writeUInt32BE(payload.byteLength, 1);
  return [header, payload];
};

const open = async (address: CommandServerAddress, waitForListener: boolean) => {
  const deadline = Date.now() + LISTEN_WAIT_MS;
  while (true) {
    const socket = connect({ host: address.host, port: address.port, noDelay: true });
    const connected = Promise.withResolvers<NodeJS.ErrnoException | null>();
    const timer = setTimeout(
      () => connected.resolve(Object.assign(new Error('timed out'), { code: 'ETIMEDOUT' })),
      waitForListener ? CONNECT_TIMEOUT_MS : REMEMBERED_CONNECT_TIMEOUT_MS,
    );
    socket.once('connect', () => connected.resolve(null));
    socket.once('error', connected.resolve);
    const error = await connected.promise.finally(() => clearTimeout(timer));
    if (!error) return socket;
    socket.destroy();
    if (!(waitForListener && error.code === 'ECONNREFUSED' && Date.now() < deadline))
      throw new CommandServerUnreachableError(address, error.code ?? error.message);
    await delay(20);
  }
};

/**
 * Runs a command in a sandbox through its command server, feeding it `stdin` and collecting its
 * output, as `KubernetesApi.exec` does through the cluster. `waitForListener` allows a pod that
 * has just started the moment its server needs.
 *
 * The token has two halves. The server is sent the first and must answer with the second before
 * it is told what to run, so whatever answers at a reused address learns nothing and can report
 * nothing. `CommandServerUnreachableError` therefore always means that no command ran.
 */
export const directExec = async (
  address: CommandServerAddress,
  command: string[],
  stdin: Uint8Array,
  timeoutSeconds: number,
  options: ExecOptions,
  waitForListener: boolean,
): Promise<KubernetesExecResult> => {
  const argv = Buffer.from(command.map((argument) => `${argument}\0`).join(''));
  if (
    command.length > MAX_ARGUMENTS ||
    command.some((argument) => argument.includes('\0')) ||
    argv.byteLength > MAX_ARGV_BYTES
  )
    throw new Error(
      `A sandbox command takes at most ${MAX_ARGUMENTS} arguments and 1 MiB, without zero bytes.`,
    );
  const half = address.token.length >> 1;
  const expected = Buffer.from(address.token.slice(half));
  const socket: Socket = await open(address, waitForListener);
  const finished = Promise.withResolvers<KubernetesExecResult>();
  const collected = collectOutput(options);
  const unreachable = (reason: string) => {
    finished.reject(new CommandServerUnreachableError(address, reason));
    socket.destroy();
  };
  const timer = setTimeout(() => {
    finished.reject(new Error(`Sandbox command timed out after ${timeoutSeconds} seconds.`));
    socket.destroy();
  }, timeoutSeconds * 1000);
  const accepting = setTimeout(() => unreachable('it did not answer the token'), ACCEPT_TIMEOUT_MS);
  let accepted = false;
  // Frames arrive in pieces, and several at once.
  let received: Buffer = Buffer.alloc(0);
  socket.on('data', (chunk: Buffer) => {
    received = received.byteLength ? Buffer.concat([received, chunk]) : chunk;
    while (received.byteLength >= 5) {
      const length = received.readUInt32BE(1);
      if (received.byteLength < 5 + length) return;
      const type = received[0];
      const payload = received.subarray(5, 5 + length);
      received = received.subarray(5 + length);
      if (!accepted) {
        if (
          type !== ACCEPTED ||
          payload.byteLength !== expected.byteLength ||
          !timingSafeEqual(payload, expected)
        )
          return unreachable('what answered is not the pod the token belongs to');
        accepted = true;
        clearTimeout(accepting);
        for (const piece of frame(ARGV, argv)) socket.write(piece);
        for (let offset = 0; offset < stdin.byteLength; offset += STDIN_CHUNK_BYTES)
          for (const piece of frame(STDIN, stdin.subarray(offset, offset + STDIN_CHUNK_BYTES)))
            socket.write(piece);
        for (const piece of frame(CLOSE, new Uint8Array())) socket.write(piece);
        continue;
      }
      if (type === STDOUT || type === STDERR)
        collected.add(payload, type === STDOUT ? 'stdout' : 'stderr');
      if (type !== EXIT) continue;
      finished.resolve(collected.result(Number(payload.toString('utf8'))));
      socket.destroy();
    }
  });
  const ended = (reason: string) =>
    accepted
      ? finished.reject(new Error(`Sandbox command ${reason}.`))
      : unreachable('the token was not accepted');
  socket.on('error', () => ended('connection failed'));
  socket.on('close', () => ended('ended without a status'));
  for (const piece of frame(AUTH, Buffer.from(address.token.slice(0, half)))) socket.write(piece);
  return finished.promise.finally(() => {
    clearTimeout(timer);
    clearTimeout(accepting);
  });
};

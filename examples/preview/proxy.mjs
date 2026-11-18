// A preview proxy: lets a browser reach a web server running in a sandbox, through a link that
// names the sandbox and port, expires, and cannot be altered.
//
//   https://<proxy>/<sandbox id>/<port>/<expires>/<signature>/<path>
//
// Your backend makes the link with `previewPath` after checking that the user may open the
// sandbox. Opening it keeps the sandbox awake while the preview is in use. It does not wake a
// sleeping one: only your backend knows the options the sandbox runs with, so wake it there first.
// It proxies HTTP only, with no WebSockets, and serves the app under a path prefix, so an app
// that links to its assets from `/` needs to be told its base path.
//
// Run it inside the cluster (see deployment.yaml): sandbox addresses are private.
import { createHmac, timingSafeEqual } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { GkeSandboxes, KubernetesApi } from '@getenergy/gke-sandboxes';

const secret = process.env.PREVIEW_SECRET;
if (!secret || secret.length < 32) throw new Error('Set PREVIEW_SECRET to 32 or more characters.');

const sign = (id, port, expires) =>
  createHmac('sha256', secret).update(`${id}:${port}:${expires}`).digest('base64url');

/** The path of a preview link for one sandbox port, valid for `seconds`. */
export const previewPath = (id, port, seconds) => {
  const expires = Math.floor(Date.now() / 1000) + seconds;
  return `/${id}/${port}/${expires}/${sign(id, port, expires)}/`;
};

if (process.argv[2] === 'sign') {
  console.log(
    previewPath(process.argv[3], Number(process.argv[4]), Number(process.argv[5] ?? 3600)),
  );
  process.exit(0);
}

const token = async () =>
  (await readFile('/var/run/secrets/kubernetes.io/serviceaccount/token', 'utf8')).trim();
const sandboxes = new GkeSandboxes(new KubernetesApi('https://kubernetes.default.svc', token), {
  namespace: 'sandboxes',
  serviceAccount: 'sandbox',
  // Only used to start sandboxes, which the proxy never does.
  image: 'unused',
});

// Marking a sandbox as in use costs API calls, so do it at most twice a minute each.
const awake = new Map();
/** 'awake' once the sandbox is marked as in use, or why it cannot be reached. */
const keepAwake = (id) => {
  const known = awake.get(id);
  if (known && known.at > Date.now() - 30_000) return known.state;
  const state = (async () => {
    const found = await sandboxes.find(id);
    if (!found) return 'missing';
    // `acquire` would start a sleeping sandbox with this proxy's options instead of yours.
    if (!found.pod) return 'asleep';
    await sandboxes.acquire(id, AbortSignal.timeout(120_000));
    return 'awake';
  })();
  awake.set(id, { at: Date.now(), state });
  state.then(
    (value) => value !== 'awake' && awake.delete(id),
    () => awake.delete(id),
  );
  return state;
};

const HOP_BY_HOP = [
  'connection',
  'keep-alive',
  'transfer-encoding',
  'upgrade',
  'host',
  'content-length',
];

createServer(async (request, response) => {
  try {
    const [, id, port, expires, signature, ...rest] = new URL(
      request.url,
      'http://x',
    ).pathname.split('/');
    const expected = Buffer.from(sign(id, port, expires));
    const given = Buffer.from(signature ?? '');
    if (
      !/^[a-z0-9-]{1,56}$/.test(id ?? '') ||
      !/^\d{1,5}$/.test(port ?? '') ||
      given.length !== expected.length ||
      !timingSafeEqual(given, expected) ||
      Number(expires) < Date.now() / 1000
    ) {
      response.writeHead(403).end('This preview link is not valid or has expired.\n');
      return;
    }
    const state = await keepAwake(id);
    if (state === 'missing') {
      response.writeHead(404).end('This sandbox no longer exists.\n');
      return;
    }
    if (state === 'asleep') {
      response
        .writeHead(503, { 'retry-after': '5' })
        .end('This sandbox is asleep. Open it in the app to wake it.\n');
      return;
    }
    const { url } = await sandboxes.endpoint({ id }, Number(port));
    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers))
      if (!HOP_BY_HOP.includes(name) && typeof value === 'string') headers.set(name, value);
    const body = ['GET', 'HEAD'].includes(request.method) ? undefined : Readable.toWeb(request);
    const upstream = await fetch(
      `${url}/${rest.join('/')}${new URL(request.url, 'http://x').search}`,
      {
        method: request.method,
        headers,
        body,
        duplex: 'half',
        redirect: 'manual',
        signal: AbortSignal.timeout(120_000),
      },
    );
    const reply = {};
    for (const [name, value] of upstream.headers)
      if (!HOP_BY_HOP.includes(name)) reply[name] = value;
    response.writeHead(upstream.status, reply);
    if (upstream.body) Readable.fromWeb(upstream.body).pipe(response);
    else response.end();
  } catch (error) {
    if (!response.headersSent) response.writeHead(502);
    response.end(`The sandbox did not answer: ${error instanceof Error ? error.message : error}\n`);
  }
}).listen(Number(process.env.PORT ?? 8080));

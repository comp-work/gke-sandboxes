// Client-side wall times for the released SDK. AsyncLocalStorage keeps concurrent
// requests apart and excludes the maintainer, the identity check, and cleanup.
import { AsyncLocalStorage } from 'node:async_hooks';
import type { KubernetesApi } from '../src/index.ts';

export const newTrace = () => ({
  listReadyMs: 0,
  claimMs: 0,
  claimAttempts: 0,
  lookupCalls: 0,
  lookupMs: 0,
  openExecMs: 0,
  runAndReturnMs: 0,
});
const requests = new AsyncLocalStorage<ReturnType<typeof newTrace>>();
export const traceRequest = requests.run.bind(requests);

export function observeStartup(client: KubernetesApi) {
  const request = client.request.bind(client);
  client.request = async (method, path, body) => {
    const trace = requests.getStore();
    if (!trace) return request(method, path, body);
    const start = performance.now();
    try {
      return await request(method, path, body);
    } finally {
      const ms = performance.now() - start;
      if (method === 'PATCH') {
        trace.claimAttempts++;
        trace.claimMs += ms;
      } else if (method === 'GET' && path.includes('!energy-sandbox')) {
        trace.listReadyMs += ms;
      } else if (method === 'GET' && path.includes('/pods?')) {
        trace.lookupCalls++;
        trace.lookupMs += ms;
      }
    }
  };
  // Observe the SDK's actual WebSocket. Leave its URL, headers and message handlers intact.
  globalThis.WebSocket = new Proxy(globalThis.WebSocket, {
    construct(target, args) {
      const trace = requests.getStore();
      const start = performance.now();
      const socket: WebSocket = Reflect.construct(target, args);
      if (trace) {
        socket.addEventListener(
          'open',
          () => {
            trace.openExecMs = performance.now() - start;
            socket.addEventListener('message', (event: MessageEvent) => {
              if (new Uint8Array(event.data as ArrayBuffer)[0] === 3)
                trace.runAndReturnMs = performance.now() - start - trace.openExecMs;
            });
          },
          { once: true },
        );
      }
      return socket;
    },
  });
}

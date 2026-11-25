# ComputeSDK sandbox benchmark, November 2026

What gke-sandboxes scores on the [ComputeSDK sandbox benchmark](https://www.computesdk.com/benchmarks/sandboxes/ratings), with the features and settings each score needs. [Back to the comparison](../comparison.md)

## Summary

gke-sandboxes averages **about 71** over 16 suites on E2 machines and **about 77** on C4D machines. On the public table on November 24, 2026, the top provider (Superserve) had 83.8, E2B 73.7, Daytona 69.1 and Boat 19.3.

Four things make the scores: a warm pool, commands sent straight to the sandbox (`directCommands`), Pod snapshots, and the machine type.

## How we ran it

- **Benchmark:** the [ComputeSDK repository](https://github.com/computesdk/benchmarks) at the time, run locally with `--no-ingest`, through a provider adapter written for gke-sandboxes. The adapter is not in this repository.
- **Cluster:** GKE 1.36 in us-central1, gVisor, a Debian 12 image with Node.js and common build tools, and the library's default reservation of 0.25 vCPU and 1 GiB. The driver ran in a pod in the same cluster.
- **Machines:** `e2-highmem-16` nodes, as in the quickstart, and one `c4d-standard-16` node. Burst survival, OS capabilities, CI capabilities and OCI boot were measured on C4D only. Burst TTI and CPU were measured on E2 only.
- **Scores:** each suite's own composite formula. The runner printed none for CPU and Rootfs, so those two are our calculation from the raw throughput and the suite's published ceilings. Burst TTI is our calculation too, from our own timing of the released warm pool; see below.
- **Public scores:** the seven-day figures on the ratings page on November 24.
- **Not run:** Soak (six hours) and Volumes (the library has no persistent volumes).
- **Every run was on an otherwise empty node.** Nothing competed for CPU or disk. On a node packed with sandboxes, the build, CPU and disk suites would score lower; we did not measure by how much.

## Results

| Suite                 | E2           | C4D          | Superserve | E2B      | Daytona  | Boat     | Best public |
| --------------------- | ------------ | ------------ | ---------- | -------- | -------- | -------- | ----------- |
| Burst TTI             | 74–89        | –            | 94         | 87       | 93       | 0        | 99          |
| DAX (build)           | 43           | 73           | 71         | 48       | 60       | 9        | 80          |
| Burst survival        | –            | 97           | 100        | 98       | 100      | 64       | 100         |
| OS capabilities       | –            | 87           | 99         | 76       | 67       | 47       | 99          |
| CI capabilities       | –            | 94           | 100        | 94       | 65       | 50       | 100         |
| CPU burst             | 67           | 66           | 68         | 69       | 59       | 49       | 76          |
| CPU                   | about 8      | –            | 22         | 10       | 33       | 9        | 52          |
| Rootfs                | about 68     | about 95     | 70         | 57       | 63       | 57       | 85          |
| FS I/O (default mode) | 72           | 86           | 73         | 67       | 69       | 52       | 81          |
| FS API                | 83           | 93           | 83         | 60       | 74       | 38       | 86          |
| Snap & restore        | 77           | 81           | 94         | 86       | 68       | 31       | 98          |
| Snapshot              | 71           | 75           | 94         | 86       | 41       | 0        | 99          |
| Fidelity              | 65           | 70           | 91         | 80       | 43       | 9        | 97          |
| Snap fan-out          | 71           | 74           | 95         | 87       | 75       | 12       | 98          |
| Pause / resume        | 75           | 75           | 97         | 92       | 0        | 0        | 99          |
| OCI boot              | –            | 75–78        | 98         | 87       | 67       | 0        | 98          |
| Soak                  | not run      | not run      | 100        | 100      | 100      | 67       | 100         |
| Volumes               | not run      | not run      | 0          | 0        | 49       | 0        | 84          |
| **Average**           | **about 71** | **about 77** | **83.8**   | **73.7** | **69.1** | **19.3** |             |

A dash is a suite measured on the other machine only, and each average uses the other machine's score for it. The public average counts a suite a provider fails as 0 and leaves out one it has no score for.

## What each score needs

### Burst TTI: a warm pool

Burst TTI creates 100 sandboxes at once and runs `node -v` in each. A new pod cannot do that fast: 100 cold starts at once took 17–18 s at the median and scored 0. One at a time, a cold start took 2.07 s (score 77.7).

**Every pod create, pod update and exec passes a GKE-managed admission webhook** on the control plane. In a burst each call took 0.7–1.7 s there, and requests queued behind them for about 2.8 s on average. A warm pool hands out a running sandbox by creating a Lease, which that webhook does not check, and `directCommands` sends the first command straight to the pod.

We timed the released pool (0.6.1) from a pod in the cluster: 100 `pool.acquire` calls with new ids at once, from two replicas, each followed by `node -v`, all served from the pool.

| Run | Median | p95    | Worst  | Score |
| --- | ------ | ------ | ------ | ----- |
| 1   | 1.20 s | 1.47 s | 1.76 s | 87    |
| 2   | 0.90 s | 1.29 s | 1.40 s | 89    |
| 3   | 2.34 s | 3.04 s | 3.06 s | 74    |

The score is ComputeSDK's formula (60% median, 25% p95, 15% p99, each against a 10 s ceiling), with the worst time standing in for p99. The hand-out alone took 0.6–0.9 s at the median in all three runs. In run 3 the first command was slow: 100 sandboxes starting `node` at once on a few nodes.

A hand-out reads three lists to decide whether the sandbox already exists. Read from etcd, as a list is by default, each took 0.5–0.8 s in a burst; the library reads them from the API server's cache, once the cache has caught up with a read made during the call, at about 0.1 s each.

### Work inside the sandbox: the machine

| Same settings                          | E2                   | C4D                  |
| -------------------------------------- | -------------------- | -------------------- |
| DAX build                              | 110–118 s            | 53–55 s              |
| Rootfs: small files / sequential write | 162 ops/s / 348 MB/s | 541 ops/s / 963 MB/s |
| FS I/O (default / fsync / large)       | 72 / 87 / 23         | 86 / 95 / 41         |

C4D roughly doubles build and disk throughput. Start-up, snapshots and capabilities barely change with the machine.

- **DAX** needs root, which is the image's user, and more than 2 vCPU / 4 GiB: its typecheck runs out of memory at that size. It ran as root with `resources` limits of 8 vCPU / 16 GiB. The reservation stayed at 0.25 vCPU and 1 GiB.
- **CPU burst** ran with a CPU limit of twice the reservation's size, set with `resources`.
- **CPU** hashes with `sha256sum`, and Debian 12's does not use the CPU's SHA instructions: 2 GB took 9.5 s against 3.9 s with `openssl`. The score is with the image's own `sha256sum`. An image with an OpenSSL-backed `sha256sum` scored about 45 on E2 and about 78 on C4D.

### Snapshots: Pod snapshots

The five snapshot suites ran through an adapter onto GKE Pod snapshots, used as the library now uses them: `snapshot(id)` to keep one, `acquire(id, signal, labels, { from })` to start sandboxes from it, and `sleep(id, { now: true })` and `acquire(id)` to pause and resume. They were not rerun with the released code.

- E2 used files-only snapshots, the default. C4D used `snapshot_scope = "whole-pod"`, which keeps memory and running processes and needs machines newer than E2; it added 3–5 points.
- A snapshot took about 2.7–2.9 s and a restore about 2 s, which is what holds these suites in the 70s. A pause took about 3.2 s and a resume 2.2 s.

### Capabilities: the image and the capabilities option

OS and CI capabilities ran as root with extra capabilities inside gVisor (`capabilities`, with `pod_security_level = "privileged"`); without them they scored 71 and 59. CI capabilities also needs Docker in the image, set up as in [run Docker in a sandbox](../client.md#run-docker-in-a-sandbox). Containers cannot reach each other by name under gVisor, which is the one CI check that failed.

### Files: direct commands

FS API ran with `directCommands`. Through the Kubernetes API, writes of 64 MB and up timed out and the suite did not finish.

### Remaining weak spots

- No KVM and no loop devices in a gVisor sandbox.
- Snapshot and restore speed caps the snapshot suites in the 70s.
- OCI boot was measured with commands through the Kubernetes API, at about 0.28 s per command. With `directCommands` the command server is mounted into any image, but the suite was not rerun that way.

## Keep-alive: Lease against pod writes

A sandbox in use renews a Lease instead of writing to its pod. Measured from a laptop through `kubectl proxy`, 40 sandboxes:

|                             | Pod patch      | Lease patch     |
| --------------------------- | -------------- | --------------- |
| One at a time, median       | 256 ms         | 170 ms          |
| 40 at once, median / worst  | 0.73 s / 1.4 s | 0.40 s / 0.69 s |
| 200 at once, median / worst | 4.5 s / 9.3 s  | 1.2 s / 2.0 s   |

## Direct commands

Measured with the 0.5.0 code, from a pod in the same cluster, on E2:

| One sandbox                     | Direct              | Kubernetes API          |
| ------------------------------- | ------------------- | ----------------------- |
| `true`, 30 runs, median / worst | 25–30 ms / 32–45 ms | 169–220 ms / 458–473 ms |
| 50 MiB upload                   | 0.4–0.7 s           | 2.3–3.1 s               |
| 50 MiB download                 | 1.8 s               | 3.6–3.8 s               |

| 100 sandboxes at once, three rounds       | Median         | Worst          |
| ----------------------------------------- | -------------- | -------------- |
| Direct, handle already used once          | 213–230 ms     | 264–291 ms     |
| Direct, new handle (one pod lookup first) | 295–302 ms     | 368–1,021 ms   |
| Kubernetes API                            | 1,245–1,604 ms | 1,572–2,102 ms |
| 1 MiB upload to each, direct              | 280 ms         | 501 ms         |
| 1 MiB upload to each, Kubernetes API      | 4,207 ms       | 5,489 ms       |

The direct median rises from about 30 ms alone to about 220 ms with 100 at once. We did not find out whether that is the single client process or the sandboxes starting 100 shells.

## Where each feature is

| Feature                                                       | Version                                                             |
| ------------------------------------------------------------- | ------------------------------------------------------------------- |
| Keep-alive through a Lease                                    | 0.4.1                                                               |
| `directCommands`                                              | 0.5.0, off by default                                               |
| Warm pool handed out with a Lease, under the caller's id      | 0.6.0                                                               |
| Lists from the API server's cache                             | 0.6.1                                                               |
| `snapshot`, `acquire(…, { from })`, `sleep(…, { now: true })` | 0.6.0                                                               |
| `capabilities`, `snapshot_scope`, `pod_security_level`        | 0.6.0                                                               |
| C4D machines                                                  | Terraform settings; see [deploy on C4D](../deploy.md#deploy-on-c4d) |

## Cost

Prices for C4D and E2 are from third-party listings, not Google's pricing page.

| Setting                                        | Cost                                                                                                                                             |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Warm pool                                      | About $8 a month per idle pooled sandbox on E2, about $11 on C4D. A pool of 100 is about 1.6 machines: roughly $830 a month on E2, $1,100 on C4D |
| Lease keep-alive, `directCommands`             | None                                                                                                                                             |
| Files-only snapshots                           | The snapshot storage                                                                                                                             |
| `whole-pod` snapshots                          | Machines newer than E2; snapshots also store memory, size not measured                                                                           |
| Higher limits (8 vCPU / 16 GiB, twice the CPU) | Nothing reserved; noisier neighbours under load                                                                                                  |
| C4D instead of E2                              | About 33% more per machine: about $0.96 against $0.72 an hour for the 16-vCPU high-memory types                                                  |
| The benchmark runs                             | A few machine-hours, about $5–10                                                                                                                 |

# ComputeSDK sandbox benchmark, November 2026

What gke-sandboxes scored on the [ComputeSDK sandbox benchmark](https://www.computesdk.com/benchmarks/sandboxes/ratings), what moved each score, and which of those changes are now in the library. [Back to the comparison](../comparison.md)

## Summary

As the library stood on November 23, 2026, it would have averaged **about 30** over the 16 suites we ran. With every change described below it averaged **about 82**, against 83.9 for the top provider on the public table that day and 69.2 for Daytona. On E2 machines, without the move to C4D, the same changes reached **about 74**.

Most of the gain came from four things: keeping running sandboxes ready, keeping the Kubernetes API out of the path of a start or a command, letting the benchmark reach features the library already had, and faster machines.

## How we ran it

- **Benchmark:** the ComputeSDK repository at the time, run locally with `--no-ingest`, through a provider adapter written for gke-sandboxes. The adapter is not in this repository; it was a throwaway built for the run.
- **Cluster:** GKE 1.36 in us-central1, gVisor, a production sandbox image (Debian 12) and the library's default reservation of 0.25 vCPU and 1 GiB. The driver ran in a pod in the same cluster.
- **Machines:** `e2-highmem-16` nodes, as in the quickstart, and one `c4d-standard-16` node for the C4D runs.
- **Scores:** each suite's own composite formula. The runner printed no composite for CPU and Rootfs, so those two are our calculation from the raw throughput and the suite's published ceilings. Public scores are the seven-day figures on the ratings page on November 23–24.
- **Not run:** Soak (six hours) and Volumes (the library has no persistent volumes).
- **Every run was on an otherwise empty node.** Nothing competed for CPU or disk. On a node packed with sandboxes the build, CPU and disk suites would score lower; we did not measure by how much.

## Results

| Suite                 | Before         | Improved, E2 | Improved, C4D | Daytona | E2B | Best public |
| --------------------- | -------------- | ------------ | ------------- | ------- | --- | ----------- |
| Burst TTI             | 0              | 96           | 96            | 93      | 88  | 99          |
| DAX (build)           | 0              | 43           | 73            | 60      | 53  | 80          |
| Burst survival        | 96             | not rerun    | 97            | 100     | 98  | 100         |
| OS capabilities       | 71             | not rerun    | 87            | 67      | 76  | 99          |
| CI capabilities       | 59             | not rerun    | 94            | 65      | 94  | 100         |
| CPU burst             | 60             | 67           | 66            | 60      | 69  | 76          |
| CPU                   | about 8        | about 45     | about 78      | 31      | 11  | 52          |
| Rootfs                | about 68       | about 68     | about 95      | 63      | 58  | 85          |
| FS I/O (default mode) | 70             | 72           | 86            | 69      | 67  | 81          |
| FS API                | did not finish | 83           | 93            | 74      | 60  | 87          |
| Snap & restore        | no adapter     | 77           | 81            | 61      | 85  | 98          |
| Snapshot              | no adapter     | 71           | 75            | 42      | 85  | 98          |
| Fidelity              | 0              | 65           | 70            | 52      | 80  | 97          |
| Snap fan-out          | no adapter     | 71           | 74            | 73      | 86  | 98          |
| Pause / resume        | 0              | 75           | 75            | 0       | 91  | 99          |
| OCI boot              | not run        | not rerun    | 75–78         | 70      | 88  | 98          |

The E2 average borrows the C4D score for the four suites not rerun on E2. A suite that runs and fails counts as 0 in the public average, while one a provider has no score for is left out, so the snapshot suites mattered more than polishing suites already in the 60s.

## What moved each score

### Starting sandboxes in a burst

Burst TTI creates sandboxes, runs `node -v` in each and destroys them, 100 at once.

| 100 at once                                                           | Median      | p95        | Score      |
| --------------------------------------------------------------------- | ----------- | ---------- | ---------- |
| Cold start                                                            | 17–18 s     | 28–31 s    | 0          |
| Warm pool, claimed by updating the pod, commands through exec         | about 5.5 s | about 10 s | not scored |
| Warm pool, claimed with a Lease, commands straight to the pod         | 1.14–1.25 s | 1.3–1.4 s  | 87–88      |
| Warm pool, claimed with no cluster call, commands straight to the pod | 0.27–0.31 s |            | 96         |

One at a time, a cold start took 2.07 s (score 77.7) on E2 and about 1.3 s on C4D.

The cause was not slow pods. **Every pod create, pod update and exec passes a GKE-managed admission webhook** on the control plane. In a burst each call took 0.7–1.7 s there, and requests queued behind them for about 2.8 s on average. A dedicated API priority level would not have helped: the control plane had about 63 seats and our requests already borrowed up to 33. Leases are not checked by that webhook, and a connection straight to the pod does not touch the API at all.

Cold starts are bound by the node. With 50 pods starting at once on one node, a cold burst of 100 still had a median of about 20 s with the leaner API calls. A warm pool is what makes a burst fast.

The fastest row keeps the pool's bookkeeping in the caller's own database, so a claim makes no cluster call. That is a design for a caller, not something the library does.

### Work inside the sandbox

| Same settings                          | E2                   | C4D                  |
| -------------------------------------- | -------------------- | -------------------- |
| DAX build                              | 110–118 s            | 53–55 s              |
| CPU, single core                       | 580 MB/s             | 1,240 MB/s           |
| Rootfs: small files / sequential write | 162 ops/s / 348 MB/s | 541 ops/s / 963 MB/s |
| FS I/O (default / fsync / large)       | 72 / 87 / 23         | 86 / 95 / 41         |

C4D roughly doubles build, CPU and disk throughput. Start-up, snapshots and capabilities barely change with the machine.

The build suite needs root, and at 2 vCPU / 4 GiB its typecheck runs out of memory, so it scored 0 at the default size. It ran with root and an 8 vCPU / 16 GiB limit. The reservation stayed at 0.25 vCPU and 1 GiB.

The CPU suite hashes with `sha256sum`, and Debian 12's does not use the CPU's SHA instructions: 2 GB took 9.5 s against 3.9 s with `openssl`. An OpenSSL-backed `sha256sum`, and a limit of twice the sized CPU, lift that suite from about 8 to about 45 on E2. That helps the benchmark more than users.

### Features the benchmark could not reach

- **Snapshots.** Five suites scored 0 for want of an adapter onto Pod snapshots. With one, files-only snapshots score 65–77 on E2. Memory-preserving snapshots, which keep running processes and need non-E2 machines, add 3–5 points. A snapshot took about 2.7–2.9 s and a restore about 2 s, which is what holds these suites in the 70s.
- **Pause / resume** is a snapshot followed by deleting the pod, and a resume starts a pod from it under the same id: about 3.2 s and 2.2 s.
- **Capabilities.** Root and extra capabilities inside gVisor take OS capabilities from 71 to 87. A Docker-ready image (legacy iptables, and Docker told to leave the firewall alone) takes CI capabilities to 94. Containers still cannot reach each other by name under gVisor. The quickstart enforces the `baseline` Pod Security level, which forbids those capabilities.
- **File transfer.** Writes of 64 MB and up timed out through exec, so FS API did not finish. Sent straight to the sandbox it scores 83–93.

### Remaining weak spots

- No KVM and no loop devices in a gVisor sandbox.
- Snapshot and restore speed caps the snapshot suites in the 70s.
- A caller's own image (OCI boot) has no command server, so it was driven through exec at about 0.28 s per command.

## What is in the library now

| Change                                                        | Status                                                               |
| ------------------------------------------------------------- | -------------------------------------------------------------------- |
| Keep-alive renews a Lease instead of writing to the pod       | In 0.4.1                                                             |
| Commands and files straight to the sandbox (`directCommands`) | In 0.5.0, off by default                                             |
| Warm pool (`warmPool()`)                                      | Already in the library; its claim still updates the pod              |
| Claim a pooled sandbox without a cluster call                 | Not in the library: needs the caller's own bookkeeping               |
| Snapshot and pause/resume adapters                            | Benchmark only                                                       |
| Root, extra capabilities, Docker-ready image                  | Benchmark only; a security decision                                  |
| C4D machines                                                  | A deployment choice; see [deploy on C4D](../deploy.md#deploy-on-c4d) |

## Keep-alive: Lease against pod writes

Measured from a laptop through `kubectl proxy`, 40 sandboxes:

|                             | Pod patch      | Lease patch     |
| --------------------------- | -------------- | --------------- |
| One at a time, median       | 256 ms         | 170 ms          |
| 40 at once, median / worst  | 0.73 s / 1.4 s | 0.40 s / 0.69 s |
| 200 at once, median / worst | 4.5 s / 9.3 s  | 1.2 s / 2.0 s   |

## Direct commands with the released library

Measured on November 24 with the 0.5.0 code, from a pod in the same cluster, on E2:

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

## Cost of each change

Prices for C4D and E2 are from third-party listings, not Google's pricing page.

| Change                                               | Cost                                                                                                                                             |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Warm pool                                            | About $8 a month per idle pooled sandbox on E2, about $11 on C4D. A pool of 100 is about 1.6 machines: roughly $830 a month on E2, $1,100 on C4D |
| Lease keep-alive, direct commands                    | None                                                                                                                                             |
| Snapshot adapter, files-only                         | None beyond the snapshot storage already paid for                                                                                                |
| Memory-preserving snapshots                          | Need non-E2 machines; snapshots also store memory, size not measured                                                                             |
| Higher limits (8 vCPU / 16 GiB, twice the sized CPU) | Nothing reserved; noisier neighbours under load                                                                                                  |
| C4D instead of E2                                    | About 33% more per machine: about $0.96 against $0.72 an hour for the 16-vCPU high-memory types                                                  |
| The benchmark runs                                   | A few machine-hours, about $5–10                                                                                                                 |

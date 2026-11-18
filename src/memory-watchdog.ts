// gVisor has no out-of-memory killer of its own: a sandbox that reaches its memory limit is killed
// whole, with every process and everything written since its last snapshot. This script runs in
// each sandbox and ends its largest processes before that, so a runaway program costs a command
// and not the sandbox. It is plain POSIX shell, so it runs in any image the sandbox can.
//
// Its arguments are command-line fragments of processes that must never be ended.
//
// - gVisor reports the limit and the use in the older cgroup layout. Without those files, or with
//   no limit, the script exits.
// - The threshold is an eighth of the limit below it, at least 256 MiB and at most a quarter of
//   the limit.
// - Every look costs CPU, about a millisecond in gVisor, so it looks only as often as it must. A
//   program can take about 4 GiB in a second. The wait before the next look is half the time such
//   a program would need to reach the threshold from here: half a second in an idle sandbox, and
//   30 ms once use is close.
// - Waiting should not start a process, which costs four times a look. Under bash it waits with
//   \`read -t\` on a pipe nothing writes to. Other shells use \`sleep\`: busybox's \`read -t\` came
//   back at once with a fractional timeout, which made the loop spin.
// - Once a sandbox has been snapshotted or woken, gVisor reports every file on its disk as shared
//   memory and adds it to the use. Those files do not count against the limit, so they are taken
//   out. Files in /dev/shm are shared memory too, and do count.
// - It ends the processes holding the most, largest first, as few as bring use back under. Never
//   the pod's first process, itself, a protected process, or one under 64 MiB. It first writes
//   the reason to the process's error output, which is what its command shows the caller.
// - Memory returns a moment after a kill, so it waits a second before it looks again.
// - Each time it acts it prints one JSON line. It shares the pod's first process's output, so the
//   line lands in the container's log, which GKE sends to Cloud Logging as structured fields with
//   the pod's name and labels.
export const MEMORY_WATCHDOG = `limit_file=/sys/fs/cgroup/memory/memory.limit_in_bytes
usage_file=/sys/fs/cgroup/memory/memory.usage_in_bytes
[ -r "$limit_file" ] && [ -r "$usage_file" ] || exit 0
read limit < "$limit_file"
[ "$limit" -lt 1125899906842624 ] || exit 0
margin=$((limit / 8))
[ "$margin" -ge 268435456 ] || margin=268435456
[ "$margin" -le $((limit / 4)) ] || margin=$((limit / 4))
threshold=$((limit - margin))
small=67108864
gib() { echo "$(($1 / 1073741824)).$(($1 % 1073741824 * 10 / 1073741824))"; }
tick=/tmp/.gke-sandboxes-memory-watchdog.tick
quick=
rm -f "$tick"
if [ -n "\${BASH_VERSION:-}" ] && mkfifo "$tick" 2>/dev/null && (exec 9<>"$tick") 2>/dev/null; then
  exec 9<>"$tick"
  quick=1
fi
nap() {
  ms=$((($threshold - $1) / 8589935))
  [ "$ms" -ge 30 ] || ms=30
  [ "$ms" -le 500 ] || ms=500
  if [ "$ms" -lt 100 ]; then ms=0.0$ms; else ms=0.$ms; fi
  if [ -n "$quick" ]; then read -t "$ms" _ <&9; else sleep "$ms"; fi
}
unrelieved=
while :; do
  read usage < "$usage_file" || usage=0
  if [ "$usage" -lt "$threshold" ]; then
    unrelieved=
    nap "$usage"
    continue
  fi
  shared=0
  while read key value unit; do
    if [ "$key" = Shmem: ]; then shared=$((value * 1024)); break; fi
  done < /proc/meminfo
  shm_files=$(df -Pk /dev/shm 2>/dev/null | { read header; read device blocks used rest; echo $((used * 1024)); })
  files=$((shared - \${shm_files:-0}))
  [ "$files" -gt 0 ] || files=0
  usage=$((usage - files))
  if [ "$usage" -lt "$threshold" ]; then
    unrelieved=
    nap "$usage"
    continue
  fi
  need=$((usage - threshold + small))
  freed=0
  ended=
  while [ "$freed" -lt "$need" ]; do
    victim=
    victim_bytes=0
    for dir in /proc/[0-9]*; do
      pid=\${dir#/proc/}
      [ "$pid" -ne 1 ] && [ "$pid" -ne $$ ] || continue
      case " $ended " in *" $pid "*) continue ;; esac
      read size resident rest < "$dir/statm" 2>/dev/null || continue
      bytes=$((resident * 4096))
      [ "$bytes" -ge "$small" ] && [ "$bytes" -gt "$victim_bytes" ] || continue
      if [ $# -gt 0 ]; then
        command=$(tr '\\000' ' ' < "$dir/cmdline" 2>/dev/null)
        protected=
        for fragment in "$@"; do
          case "$command" in *"$fragment"*) protected=1 ;; esac
        done
        [ -z "$protected" ] || continue
      fi
      victim=$pid
      victim_bytes=$bytes
    done
    [ -n "$victim" ] || break
    printf '\\ngke-sandboxes ended this process: the sandbox was about to run out of memory (%s GiB) and this process held %s GiB of it. Running it again unchanged will end the same way. Use less memory, for example by working through the data in smaller pieces.\\n' "$(gib "$limit")" "$(gib "$victim_bytes")" > "/proc/$victim/fd/2" 2>/dev/null
    name=$(tr '\\000' ' ' < "/proc/$victim/cmdline" 2>/dev/null | tr -d '"\\\\\\001-\\037' | cut -c1-200)
    kill -9 "$victim" 2>/dev/null
    printf '{"event":"sandbox_memory_pressure","outcome":"process_ended","pid":%s,"command":"%s","resident_bytes":%s,"usage_bytes":%s,"threshold_bytes":%s,"limit_bytes":%s}\\n' "$victim" "$name" "$victim_bytes" "$usage" "$threshold" "$limit"
    ended="$ended $victim"
    freed=$((freed + victim_bytes))
  done
  if [ -z "$ended" ] && [ -z "$unrelieved" ]; then
    printf '{"event":"sandbox_memory_pressure","outcome":"nothing_to_end","usage_bytes":%s,"threshold_bytes":%s,"limit_bytes":%s}\\n' "$usage" "$threshold" "$limit"
    unrelieved=1
  fi
  sleep 1
done
`;

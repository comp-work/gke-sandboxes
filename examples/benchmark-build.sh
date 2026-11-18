#!/bin/bash
# The build benchmark behind the README's GKE column. Run it in a sandbox whose image has Node.js 22
# and git, such as docker.io/library/node:22-bookworm, with 4 vCPU and 8 GiB limits. It prints one
# RESULT line per measurement: seconds for each Better Auth step, and runs per second for V8's web
# tooling benchmark.
#
# Better Auth is pinned to the commit and pnpm commands of the realworld-better-auth profile in
# hpc-sandbox-benchmarks. The web tooling benchmark is the current GitHub version, run directly; that
# harness runs it through a pinned Phoronix profile.
set -uo pipefail
cd "$HOME"
timed() {
  local name=$1 started
  shift
  started=$(date +%s.%N)
  "$@" >"/tmp/$name.log" 2>&1
  local status=$?
  echo "RESULT $name $(node -e "console.log(($(date +%s.%N)-$started).toFixed(2))") rc=$status"
}
echo "nproc=$(nproc) node=$(node --version) mem=$(awk '/MemTotal/{print $2}' /proc/meminfo)kB"
corepack enable >/dev/null 2>&1

git clone -q --depth 1 https://github.com/v8/web-tooling-benchmark wtb && cd wtb
npm install --silent >/tmp/wtb-install.log 2>&1
node dist/cli.js >/tmp/wtb.log 2>&1
echo "RESULT web_tooling_runs_per_s $(grep -i 'Geometric mean' /tmp/wtb.log | grep -oE '[0-9]+\.[0-9]+')"
cd "$HOME"

mkdir ba && cd ba && git init -q
git remote add origin https://github.com/better-auth/better-auth.git
timed git_clone sh -c \
  'git fetch -q --depth 1 origin 6f3ba45639579da152b69e8e5342e02f28288670 && git checkout -q FETCH_HEAD'
COREPACK_ENABLE_DOWNLOAD_PROMPT=0 pnpm --version >/dev/null 2>&1
timed cold_install pnpm install --frozen-lockfile
timed build pnpm build

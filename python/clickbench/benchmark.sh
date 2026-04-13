#!/usr/bin/env bash

set -e
cd "$(dirname "$0")"
set -a; source ../.env.staging; set +a

./run.sh 2>&1 | tee log.txt

cat log.txt | grep -oP 'Time: \d+\.\d+ ms|psql: error' \
  | sed -r 's/Time: ([0-9]+\.[0-9]+) ms/\1/; s/^.*psql: error.*$/null/' \
  | awk '{
      if (i % 3 == 0) { printf "[" }
      if ($1 == "null") { printf $1 } else { printf $1 / 1000 }
      if (i % 3 != 2) { printf "," } else { print "]," }
      ++i
    }'

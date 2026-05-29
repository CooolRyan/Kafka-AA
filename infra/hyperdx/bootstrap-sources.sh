#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker exec -i hyperdx-mongo mongosh hyperdx --quiet < "$DIR/add-mirror-delay-source.js"
docker exec -i hyperdx-mongo mongosh hyperdx --quiet < "$DIR/list-sources.js"

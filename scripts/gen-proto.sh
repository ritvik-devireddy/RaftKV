#!/usr/bin/env bash
# Regenerates api/raftkvpb/*.pb.go from api/raftkv.proto. See gen-proto.ps1 for setup.
set -euo pipefail
cd "$(dirname "$0")/.."
protoc \
  --go_out=. --go_opt=module=github.com/ritvik-devireddy/raftkv \
  --go-grpc_out=. --go-grpc_opt=module=github.com/ritvik-devireddy/raftkv \
  api/raftkv.proto
echo "generated api/raftkvpb"

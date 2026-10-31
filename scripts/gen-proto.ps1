# Regenerates api/raftkvpb/*.pb.go from api/raftkv.proto.
#
# One-time setup:
#   winget install protobuf          (or download protoc from github.com/protocolbuffers/protobuf/releases)
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# Make sure %USERPROFILE%\go\bin is on your PATH.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
protoc `
  --go_out=. --go_opt=module=github.com/ritvik-devireddy/raftkv `
  --go-grpc_out=. --go-grpc_opt=module=github.com/ritvik-devireddy/raftkv `
  api/raftkv.proto
Write-Host "generated api/raftkvpb"

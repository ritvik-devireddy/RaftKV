# Starts a local three-node RaftKV cluster, each node in its own PowerShell
# window, for demos and screen recordings.
#
#   .\scripts\demo.ps1            start the cluster (keeps any existing data)
#   .\scripts\demo.ps1 -Fresh     delete ./data first, so every node starts empty
#
# Then, in this window:
#   .\bin\raftkv-cli.exe put hello world
#   .\bin\raftkv-cli.exe get hello
# Close a node's window (or press Ctrl+C in it) to kill it, and watch the others
# elect a new leader while reads and writes keep working.
param([switch]$Fresh)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/raftkv-server.exe ./cmd/raftkv-server
go build -o bin/raftkv-cli.exe ./cmd/raftkv-cli

if ($Fresh -and (Test-Path data)) { Remove-Item -Recurse -Force data }

$peers = "localhost:7001,localhost:7002,localhost:7003"
foreach ($id in 0..2) {
    $port = 7001 + $id
    $cmd = "`$Host.UI.RawUI.WindowTitle = 'raftkv node $id'; " +
           ".\bin\raftkv-server.exe -id $id -listen :$port -data data\node$id -peers $peers"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
}
Write-Host "Started 3 nodes (ports 7001-7003). Give them a second to elect a leader, then try the CLI."

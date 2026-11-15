param([string]$Environment = "dev")

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$target = Join-Path $PSScriptRoot "../environments/$Environment"
terraform -chdir=$target init -backend=false
terraform -chdir=$target validate

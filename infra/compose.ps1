param(
    [Parameter(ValueFromRemainingArguments = $true)]
    $Args
)

$repoRoot = Split-Path -Parent $PSScriptRoot

# If user passes "up", force detached mode unless they explicitly used -d/--detach
if ($Args.Count -gt 0 -and $Args[0] -eq "up") {
    if (-not ($Args -contains "-d") -and -not ($Args -contains "--detach")) {
        $Args = @("up", "-d") + $Args[1..($Args.Count-1)]
    }
}

$envFile = Join-Path $repoRoot ".env"
$composeFile = Join-Path $repoRoot "infra\docker-compose.yml"

docker compose `
  --project-directory $repoRoot `
  --env-file $envFile `
  -f $composeFile `
  @Args

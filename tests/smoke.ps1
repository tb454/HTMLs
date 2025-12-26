$base = "https://bridge.scrapfutures.com"
$cookie = "$PSScriptRoot\cookies.txt"

# (1) login (needs a real admin email/password you already have)
$adminEmail = $env:BRIDGE_ADMIN_EMAIL
$adminPass  = $env:BRIDGE_ADMIN_PASS
if (-not $adminEmail -or -not $adminPass) {
  Write-Host "Set env vars BRIDGE_ADMIN_EMAIL and BRIDGE_ADMIN_PASS first."
  exit 1
}

# Use curl.exe explicitly (NOT PowerShell curl alias)
curl.exe -s -c $cookie -b $cookie `
  -H "Content-Type: application/json" `
  -d "{`"username`":`"$adminEmail`",`"password`":`"$adminPass`"}" `
  "$base/login" | Out-Null

function Hit($path) {
  $url = "$base$path"
  $r = curl.exe -s -b $cookie -o NUL -w "%{http_code}" $url
  if ($r -ne "200") {
    Write-Host "FAIL $path => $r"
    exit 1
  } else {
    Write-Host "OK   $path"
  }
}

Hit "/healthz"
Hit "/ops/smoke"
Hit "/ops/perf_floor"
Hit "/admin/ops/log_schema.json"
# (optional) snapshot now
# Hit "/admin/run_snapshot_now?storage=supabase"
Write-Host "ALL SMOKE CHECKS PASSED"

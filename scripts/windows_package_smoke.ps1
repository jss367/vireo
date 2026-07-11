param(
  [Parameter(Mandatory = $true)][string]$Installer,
  [switch]$RequireSignature
)

$ErrorActionPreference = "Stop"

function Assert-Signed([string]$Path) {
  $signature = Get-AuthenticodeSignature -FilePath $Path
  if ($signature.Status -ne "Valid") {
    throw "Invalid Authenticode signature for $Path ($($signature.Status))"
  }
  if (-not $signature.TimeStamperCertificate) {
    throw "Missing Authenticode timestamp for $Path"
  }
}

function Wait-Vireo([string]$RuntimePath) {
  for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 1
    if (-not (Test-Path $RuntimePath)) { continue }
    try {
      $candidate = Get-Content $RuntimePath -Raw | ConvertFrom-Json
      $health = Invoke-RestMethod `
        -Uri "http://127.0.0.1:$($candidate.port)/api/v1/health" `
        -Headers @{ "X-Vireo-Token" = $candidate.token } `
        -TimeoutSec 2
      if ($health.service -eq "vireo") { return $candidate }
    } catch { }
  }
  throw "Packaged Vireo backend did not become healthy"
}

function Stop-Vireo($Runtime, $AppProcess) {
  try {
    Invoke-RestMethod -Method Post `
      -Uri "http://127.0.0.1:$($Runtime.port)/api/v1/shutdown" `
      -Headers @{ "X-Vireo-Token" = $Runtime.token } `
      -TimeoutSec 5 | Out-Null
  } catch {
    Write-Warning "Graceful backend shutdown failed: $_"
  }
  Start-Sleep -Seconds 1
  if (-not $AppProcess.HasExited) {
    Stop-Process -Id $AppProcess.Id -Force -ErrorAction SilentlyContinue
  }
}

$installerPath = (Resolve-Path $Installer).Path
if ($RequireSignature) { Assert-Signed $installerPath }

$stateDir = Join-Path $HOME ".vireo"
$preserveMarker = Join-Path $stateDir "windows-smoke-preserve.txt"
New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
Set-Content -Path $preserveMarker -Value "must survive uninstall"

$install = Start-Process -FilePath $installerPath -ArgumentList "/S" -Wait -PassThru
if ($install.ExitCode -ne 0) { throw "Installer exited $($install.ExitCode)" }

$searchRoots = @(
  (Join-Path $env:LOCALAPPDATA "Programs\Vireo"),
  (Join-Path $env:LOCALAPPDATA "Vireo"),
  (Join-Path $env:ProgramFiles "Vireo")
)
$appExe = $null
foreach ($root in $searchRoots) {
  if (Test-Path $root) {
    $appExe = Get-ChildItem -Path $root -Filter "Vireo.exe" -Recurse -File |
      Select-Object -First 1 -ExpandProperty FullName
    if ($appExe) { break }
  }
}
if (-not $appExe) { throw "Installed Vireo.exe was not found" }
if ($RequireSignature) {
  Assert-Signed $appExe
  $installedSidecar = Get-ChildItem -Path (Split-Path $appExe -Parent) -Filter "vireo-server.exe" -Recurse -File |
    Select-Object -First 1 -ExpandProperty FullName
  if (-not $installedSidecar) { throw "Installed vireo-server.exe was not found" }
  Assert-Signed $installedSidecar
}

$app = Start-Process -FilePath $appExe -PassThru
$runtimePath = Join-Path $stateDir "runtime.json"
$runtime = Wait-Vireo $runtimePath
$headers = @{ "X-Vireo-Token" = $runtime.token; "Content-Type" = "application/json" }

# Exercise a real filesystem journey through the packaged sidecar.
$fixtureDir = Join-Path $env:TEMP "vireo-windows-smoke-library"
Remove-Item $fixtureDir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $fixtureDir | Out-Null
Add-Type -AssemblyName System.Drawing
$bitmap = New-Object System.Drawing.Bitmap 32, 32
$fixturePhoto = Join-Path $fixtureDir "windows-smoke.jpg"
$bitmap.Save($fixturePhoto, [System.Drawing.Imaging.ImageFormat]::Jpeg)
$bitmap.Dispose()

$scan = Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:$($runtime.port)/api/jobs/scan" `
  -Headers $headers `
  -Body (@{ root = $fixtureDir } | ConvertTo-Json) `
  -TimeoutSec 10
if (-not $scan.job_id) { throw "Packaged scan did not return a job id" }
$job = $null
for ($i = 0; $i -lt 90; $i++) {
  Start-Sleep -Seconds 1
  $job = Invoke-RestMethod `
    -Uri "http://127.0.0.1:$($runtime.port)/api/jobs/$($scan.job_id)" `
    -Headers $headers `
    -TimeoutSec 5
  if ($job.status -in "completed", "failed", "cancelled") { break }
}
if ($job.status -ne "completed") { throw "Packaged scan ended with status $($job.status)" }
$photos = Invoke-RestMethod `
  -Uri "http://127.0.0.1:$($runtime.port)/api/v1/photos" `
  -Headers $headers `
  -TimeoutSec 5
if (-not ($photos.photos | Where-Object { $_.filename -eq "windows-smoke.jpg" })) {
  throw "Scanned fixture did not appear in the catalog"
}

# Restart and prove that the catalog survives the native shell lifecycle.
Stop-Vireo $runtime $app
$app = Start-Process -FilePath $appExe -PassThru
$runtime = Wait-Vireo $runtimePath
$headers = @{ "X-Vireo-Token" = $runtime.token }
$photos = Invoke-RestMethod `
  -Uri "http://127.0.0.1:$($runtime.port)/api/v1/photos" `
  -Headers $headers `
  -TimeoutSec 5
if (-not ($photos.photos | Where-Object { $_.filename -eq "windows-smoke.jpg" })) {
  throw "Catalog fixture did not survive restart"
}
Stop-Vireo $runtime $app

$installRoot = Split-Path $appExe -Parent
$uninstaller = Get-ChildItem -Path $installRoot -Filter "uninstall*.exe" -File |
  Select-Object -First 1 -ExpandProperty FullName
if (-not $uninstaller) { throw "Vireo uninstaller was not found" }
$uninstall = Start-Process -FilePath $uninstaller -ArgumentList "/S" -Wait -PassThru
if ($uninstall.ExitCode -ne 0) { throw "Uninstaller exited $($uninstall.ExitCode)" }
if (-not (Test-Path $preserveMarker)) {
  throw "Uninstall removed Vireo user data"
}

Write-Host "Windows packaged-app smoke test passed"

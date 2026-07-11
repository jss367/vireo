param(
  [Parameter(Mandatory = $true)][string]$Root,
  [Parameter(Mandatory = $true)][string]$ExpectedPublisher
)

$ErrorActionPreference = "Stop"
$artifacts = Get-ChildItem -Path $Root -Recurse -File |
  Where-Object { $_.Extension -in ".exe", ".msi" }
if (-not $artifacts) { throw "No Windows executable artifacts found below $Root" }

foreach ($artifact in $artifacts) {
  $signature = Get-AuthenticodeSignature -FilePath $artifact.FullName
  if ($signature.Status -ne "Valid") {
    throw "$($artifact.Name) signature is $($signature.Status)"
  }
  if (-not $signature.SignerCertificate.Subject.Contains($ExpectedPublisher)) {
    throw "$($artifact.Name) publisher '$($signature.SignerCertificate.Subject)' does not contain '$ExpectedPublisher'"
  }
  if (-not $signature.TimeStamperCertificate) {
    throw "$($artifact.Name) has no timestamp certificate"
  }
  Write-Host "Verified $($artifact.Name): $($signature.SignerCertificate.Subject)"
}

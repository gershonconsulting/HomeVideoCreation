#Requires -Version 5
# install-windows.ps1
# Installs Node.js LTS, FFmpeg, and yt-dlp via winget, then sets up the project.
#
# Run from a fresh PowerShell window inside the souvenir folder:
#   .\install-windows.ps1

$ErrorActionPreference = 'Stop'
$flags = @(
  '--silent',
  '--accept-package-agreements',
  '--accept-source-agreements',
  '-e'
)

function Install-IfMissing {
  param([string]$Id, [string]$Cmd, [string]$Label)
  Write-Host ("→ {0}..." -f $Label) -ForegroundColor Cyan
  if (Get-Command $Cmd -ErrorAction SilentlyContinue) {
    Write-Host ("  already installed: {0}" -f (Get-Command $Cmd).Source) -ForegroundColor DarkGray
    return
  }
  & winget install --id $Id @flags
  if ($LASTEXITCODE -ne 0) {
    Write-Warning ("winget install for {0} returned exit code {1}" -f $Id, $LASTEXITCODE)
  }
}

Write-Host "Installing Souvenir dependencies via winget" -ForegroundColor Green
Write-Host ""

Install-IfMissing -Id 'OpenJS.NodeJS.LTS' -Cmd 'node'   -Label 'Node.js LTS'
Install-IfMissing -Id 'Gyan.FFmpeg'       -Cmd 'ffmpeg' -Label 'FFmpeg (Gyan build, with PATH)'
Install-IfMissing -Id 'yt-dlp.yt-dlp'     -Cmd 'yt-dlp' -Label 'yt-dlp'

Write-Host ""
Write-Host "Done. Next steps:" -ForegroundColor Green
Write-Host "  1. Close this PowerShell window and open a new one (so PATH updates)" -ForegroundColor Yellow
Write-Host "  2. cd into the souvenir folder"
Write-Host "  3. npm install"
Write-Host "  4. npm start"
Write-Host "  5. Open http://localhost:3737 in your browser"
Write-Host ""

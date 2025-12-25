# PowerShell script to create Lambda deployment package
Write-Host "[INFO] Creating Lambda deployment package..." -ForegroundColor Cyan

$zipFile = "openaq_fetcher_code.zip"

# Remove old zip if exists
if (Test-Path $zipFile) {
    Remove-Item $zipFile -Force
    Write-Host "  Removed old $zipFile" -ForegroundColor Yellow
}

# Create temporary directory for packaging
$tempDir = "temp_package"
if (Test-Path $tempDir) {
    Remove-Item $tempDir -Recurse -Force
}
New-Item -ItemType Directory -Path $tempDir | Out-Null

# Copy files to temp directory
Copy-Item "handler.py" -Destination $tempDir
Copy-Item "six.py" -Destination $tempDir
Copy-Item "etls" -Destination $tempDir -Recurse
Copy-Item "utils" -Destination $tempDir -Recurse

# Remove __pycache__ directories
Get-ChildItem -Path $tempDir -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
Get-ChildItem -Path $tempDir -Recurse -File -Filter "*.pyc" | Remove-Item -Force

Write-Host "[INFO] Files to package:" -ForegroundColor Cyan
Get-ChildItem -Path $tempDir -Recurse -File | ForEach-Object {
    $relativePath = $_.FullName.Substring($tempDir.Length + 1)
    Write-Host "  + $relativePath" -ForegroundColor Green
}

# Create ZIP file
Write-Host "[INFO] Creating ZIP archive..." -ForegroundColor Cyan
Compress-Archive -Path "$tempDir\*" -DestinationPath $zipFile -Force

# Cleanup temp directory
Remove-Item $tempDir -Recurse -Force

# Show result
$sizeKB = [math]::Round((Get-Item $zipFile).Length / 1KB, 2)
Write-Host "[SUCCESS] Created $zipFile ($sizeKB KB)" -ForegroundColor Green

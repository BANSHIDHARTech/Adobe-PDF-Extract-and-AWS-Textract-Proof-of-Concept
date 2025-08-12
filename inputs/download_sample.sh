#!/usr/bin/env bash
set -e
mkdir -p inputs
# Download a sample PDF document (replace with an actual working URL)
# For this POC, you may need to manually place a PDF in inputs/bray_sample.pdf
echo "Downloading sample PDF..."
# Note: Replace this with a working PDF URL or manually download a multi-page PDF
curl -L -o inputs/bray_sample.pdf "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf" || echo "Download failed. Please manually place a multi-page PDF as inputs/bray_sample.pdf"
echo "Sample PDF ready at inputs/bray_sample.pdf"
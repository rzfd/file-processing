#!/bin/bash

# Create a test CSV file
cat > /tmp/test-loki.csv << 'EOF'
id,name,email,age
1,John Doe,john@example.com,30
2,Jane Smith,jane@example.com,25
3,Bob Johnson,bob@example.com,35
4,Alice Williams,alice@example.com,28
5,Charlie Brown,charlie@example.com,42
EOF

echo "📤 Uploading test file..."
RESPONSE=$(curl -s -X POST http://localhost:8080/upload \
  -F "file=@/tmp/test-loki.csv" \
  -F "filename=test-loki.csv")

echo "Response: $RESPONSE"

FILE_ID=$(echo $RESPONSE | jq -r '.id')

if [ "$FILE_ID" != "null" ] && [ -n "$FILE_ID" ]; then
  echo "✅ File uploaded successfully! ID: $FILE_ID"
  echo ""
  echo "⏳ Waiting for processing..."
  sleep 3
  
  echo ""
  echo "📊 Checking file status..."
  curl -s http://localhost:8080/files/$FILE_ID | jq .
else
  echo "❌ Upload failed"
fi

# Cleanup
rm /tmp/test-loki.csv


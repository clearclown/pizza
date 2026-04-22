#!/usr/bin/env bash
# 🍕 proto.sh — buf generate のラッパ + post-check

set -euo pipefail

echo "🔌 buf lint..."
buf lint api

echo "🔧 buf format..."
buf format -w api

echo "📜 buf generate..."
buf generate api

echo "📊 Output:"
find gen -type f | sort | sed 's/^/    /'

echo "✅ Proto regenerated."

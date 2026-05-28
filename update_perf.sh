#!/bin/bash
# update_perf.sh: Run benchmarks and tests and update PERF.md

# Exit on error
set -e

# Change to script directory
cd "$(dirname "$0")"

# Read version from Cargo.toml
VERSION=$(grep -m1 '^version =' Cargo.toml | cut -d '"' -f2)
TITLE="## Version $VERSION - $(date '+%Y-%m-%d')"

echo "Running tests in benches workspace..."
TEST_OUT=$(cargo test --manifest-path benches/Cargo.toml 2>&1)

echo "Running criteria benchmarks..."
BENCH_OUT=$(cargo bench --manifest-path benches/Cargo.toml 2>&1)

# Create a temporary file
TMP_FILE=$(mktemp)

# Write title
echo "$TITLE" > "$TMP_FILE"
echo "" >> "$TMP_FILE"

# Write test output
echo "### Integration Test & Output" >> "$TMP_FILE"
echo '```text' >> "$TMP_FILE"
echo "$TEST_OUT" >> "$TMP_FILE"
echo '```' >> "$TMP_FILE"
echo "" >> "$TMP_FILE"

# Write bench output
echo "### Criterion Benchmark Output" >> "$TMP_FILE"
echo '```text' >> "$TMP_FILE"
echo "$BENCH_OUT" >> "$TMP_FILE"
echo '```' >> "$TMP_FILE"
echo "" >> "$TMP_FILE"

# Append existing content
if [ -f "PERF.md" ]; then
    cat PERF.md >> "$TMP_FILE"
fi

# Overwrite
mv "$TMP_FILE" PERF.md

echo "Successfully updated PERF.md with version $VERSION"

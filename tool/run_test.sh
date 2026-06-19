#!/bin/bash
rm -rf data/
cargo test --test integration_tests
cargo test --test memory_leak_test
rm -rf data/

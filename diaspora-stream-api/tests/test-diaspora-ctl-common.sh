#!/usr/bin/env bash

# Common utilities for diaspora-ctl test suites

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BUILD_DIR="${SCRIPT_DIR}/.."
DIASPORA_CTL="${BUILD_DIR}/bin/diaspora-ctl"
TEST_DATA_DIR="${SCRIPT_DIR}/diaspora-ctl-test-data"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Setup test environment
setup() {
    echo "================================================"
    echo "Setting up test environment"
    echo "================================================"

    # Check if diaspora-ctl exists
    if [ ! -f "$DIASPORA_CTL" ]; then
        echo -e "${RED}ERROR: diaspora-ctl not found at $DIASPORA_CTL${NC}"
        echo "Please build the project first"
        exit 1
    fi

    # Create and clean test data directory
    mkdir -p "$TEST_DATA_DIR"
    rm -rf "${TEST_DATA_DIR:?}"/*

    echo "Test data directory: $TEST_DATA_DIR"
    echo "diaspora-ctl: $DIASPORA_CTL"
    echo ""
}

# Cleanup test environment
cleanup() {
    echo ""
    echo "================================================"
    echo "Cleaning up test environment"
    echo "================================================"
    rm -rf "${TEST_DATA_DIR:?}"
    echo "Cleanup complete"
}

# Print test result
print_result() {
    local test_name=$1
    local result=$2

    TESTS_RUN=$((TESTS_RUN + 1))

    if [ "$result" -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}✗ FAIL${NC}: $test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Print error message in red
print_error() {
    echo -e "${RED}  ERROR: $1${NC}"
}

# Print info message in yellow
print_info() {
    echo -e "${YELLOW}  INFO: $1${NC}"
}

# Print summary
print_summary() {
    echo ""
    echo "================================================"
    echo "Test Summary"
    echo "================================================"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
    echo "================================================"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    fi
}

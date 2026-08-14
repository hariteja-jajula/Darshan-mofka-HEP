#!/usr/bin/env bash

# Test suite for diaspora-ctl topic commands with files driver

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/test-diaspora-ctl-common.sh"

# Test: Display help
test_help() {
    echo ""
    echo "Test: Display help"
    echo "-------------------"

    local output
    output=$("$DIASPORA_CTL" --help 2>&1)
    local result=$?

    if [ $result -ne 0 ]; then
        print_error "diaspora-ctl --help returned non-zero exit code: $result"
        print_info "Output: $output"
    fi

    print_result "diaspora-ctl --help" $result
}

# Test: Create a simple topic
test_create_simple_topic() {
    echo ""
    echo "Test: Create a simple topic"
    echo "----------------------------"

    local root_path="${TEST_DATA_DIR}/simple"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "test-topic-1" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    elif [ ! -d "$root_path/test-topic-1" ]; then
        print_error "Topic directory not created at $root_path/test-topic-1"
        print_info "Directory contents: $(ls -la "$root_path" 2>&1)"
        result=1
    else
        result=0
    fi

    print_result "Create simple topic" $result
}

# Test: Create topic with multiple partitions
test_create_topic_with_partitions() {
    echo ""
    echo "Test: Create topic with multiple partitions"
    echo "---------------------------------------------"

    local root_path="${TEST_DATA_DIR}/partitions"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "multi-partition-topic" \
        --topic.num_partitions 4 \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        # Verify partition directories were created (00000000, 00000001, 00000002, 00000003)
        local missing_partitions=""
        for i in 0 1 2 3; do
            local partition_dir="$root_path/multi-partition-topic/partitions/0000000$i"
            if [ ! -d "$partition_dir" ]; then
                missing_partitions="$missing_partitions 0000000$i"
            fi
        done

        if [ -n "$missing_partitions" ]; then
            print_error "Missing partition directories:$missing_partitions"
            print_info "Partitions directory contents: $(ls -la "$root_path/multi-partition-topic/partitions/" 2>&1)"
            result=1
        else
            result=0
        fi
    fi

    print_result "Create topic with 4 partitions" $result
}

# Test: Create topic with config file
test_create_topic_with_config_file() {
    echo ""
    echo "Test: Create topic with config file"
    echo "-------------------------------------"

    local root_path="${TEST_DATA_DIR}/config"
    mkdir -p "$root_path"

    # Create driver config file
    local driver_config="${TEST_DATA_DIR}/driver-config.json"
    cat > "$driver_config" << EOF
{
    "root_path": "$root_path"
}
EOF

    # Create topic config file
    local topic_config="${TEST_DATA_DIR}/topic-config.json"
    cat > "$topic_config" << EOF
{
    "num_partitions": 2
}
EOF

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver-config "$driver_config" \
        --name "config-topic" \
        --topic-config "$topic_config" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        # Verify topic was created with 2 partitions
        if [ ! -d "$root_path/config-topic/partitions/00000000" ]; then
            print_error "Partition 00000000 not created"
            print_info "Partitions directory: $(ls -la "$root_path/config-topic/partitions/" 2>&1)"
            result=1
        elif [ ! -d "$root_path/config-topic/partitions/00000001" ]; then
            print_error "Partition 00000001 not created"
            print_info "Partitions directory: $(ls -la "$root_path/config-topic/partitions/" 2>&1)"
            result=1
        else
            result=0
        fi
    fi

    # Cleanup config files
    rm -f "$driver_config" "$topic_config"

    print_result "Create topic with config files" $result
}

# Test: Verify component metadata files are created
test_component_metadata_files() {
    echo ""
    echo "Test: Verify component metadata files"
    echo "---------------------------------------"

    local root_path="${TEST_DATA_DIR}/components"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "component-topic" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        result=0

        # Verify component JSON files exist
        if [ ! -f "$root_path/component-topic/validator.json" ]; then
            print_error "validator.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        if [ ! -f "$root_path/component-topic/serializer.json" ]; then
            print_error "serializer.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        if [ ! -f "$root_path/component-topic/partition-selector.json" ]; then
            print_error "partition-selector.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        # Verify JSON files are valid JSON
        if [ $result -eq 0 ]; then
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/validator.json'))" 2>/dev/null; then
                print_error "validator.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/validator.json" 2>&1)"
                result=1
            fi
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/serializer.json'))" 2>/dev/null; then
                print_error "serializer.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/serializer.json" 2>&1)"
                result=1
            fi
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/partition-selector.json'))" 2>/dev/null; then
                print_error "partition-selector.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/partition-selector.json" 2>&1)"
                result=1
            fi
        fi
    fi

    print_result "Component metadata files created and valid" $result
}

# Test: Create topic with nested metadata parameters
test_nested_metadata_parameters() {
    echo ""
    echo "Test: Create topic with nested metadata"
    echo "-----------------------------------------"

    local root_path="${TEST_DATA_DIR}/nested"
    mkdir -p "$root_path"

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "nested-topic" \
        --topic.config.option1 "value1" \
        --topic.config.option2 "value2" \
        > /dev/null 2>&1

    local result=$?

    # Just verify the topic was created
    if [ -d "$root_path/nested-topic" ]; then
        result=0
    else
        result=1
    fi

    print_result "Create topic with nested metadata parameters" $result
}

# Test: Error handling - missing required arguments
test_missing_required_args() {
    echo ""
    echo "Test: Error handling - missing required arguments"
    echo "---------------------------------------------------"

    # Should fail without --driver
    "$DIASPORA_CTL" topic create --name "test" > /dev/null 2>&1
    local result1=$?

    # Should fail without --name
    "$DIASPORA_CTL" topic create --driver "files" > /dev/null 2>&1
    local result2=$?

    # Both should fail (non-zero exit code)
    if [ $result1 -ne 0 ] && [ $result2 -ne 0 ]; then
        result=0
    else
        result=1
    fi

    print_result "Missing required arguments detected" $result
}

# Test: Error handling - duplicate topic
test_duplicate_topic() {
    echo ""
    echo "Test: Error handling - duplicate topic"
    echo "----------------------------------------"

    local root_path="${TEST_DATA_DIR}/duplicate"
    mkdir -p "$root_path"

    # Create first topic (should succeed)
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "duplicate-topic" \
        > /dev/null 2>&1

    # Try to create same topic again (should fail)
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "duplicate-topic" \
        > /dev/null 2>&1

    local result=$?

    # Should fail (non-zero exit code)
    if [ $result -ne 0 ]; then
        result=0
    else
        result=1
    fi

    print_result "Duplicate topic creation rejected" $result
}

# Test: Create multiple topics in same driver
test_multiple_topics() {
    echo ""
    echo "Test: Create multiple topics"
    echo "-----------------------------"

    local root_path="${TEST_DATA_DIR}/multiple"
    mkdir -p "$root_path"

    # Create multiple topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-a" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-b" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-c" \
        > /dev/null 2>&1

    local result=0

    # Verify all topics exist
    if [ ! -d "$root_path/topic-a" ]; then
        result=1
    fi
    if [ ! -d "$root_path/topic-b" ]; then
        result=1
    fi
    if [ ! -d "$root_path/topic-c" ]; then
        result=1
    fi

    print_result "Create multiple topics" $result
}

# Test: List topics (basic)
test_list_topics_basic() {
    echo ""
    echo "Test: List topics (basic)"
    echo "--------------------------"

    local root_path="${TEST_DATA_DIR}/list-basic"
    mkdir -p "$root_path"

    # Create some topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "list-topic-1" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "list-topic-2" \
        > /dev/null 2>&1

    # List topics
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        2>/dev/null)

    local result=0

    # Check that both topics are in the output
    if ! echo "$output" | grep -q "list-topic-1"; then
        print_error "list-topic-1 not found in output"
        print_info "Output: $output"
        result=1
    fi

    if ! echo "$output" | grep -q "list-topic-2"; then
        print_error "list-topic-2 not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that output has exactly 2 lines (one per topic)
    local line_count=$(echo "$output" | wc -l)
    if [ "$line_count" -ne 2 ]; then
        print_error "Expected 2 lines, got $line_count"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (basic)" $result
}

# Test: List topics with verbose flag
test_list_topics_verbose() {
    echo ""
    echo "Test: List topics (verbose)"
    echo "----------------------------"

    local root_path="${TEST_DATA_DIR}/list-verbose"
    mkdir -p "$root_path"

    # Create a topic
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "verbose-topic" \
        --topic.num_partitions 3 \
        > /dev/null 2>&1

    # List topics with verbose flag
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        --verbose \
        2>/dev/null)

    local result=0

    # Check that topic name is in the output
    if ! echo "$output" | grep -q "verbose-topic"; then
        print_error "verbose-topic not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that metadata is present (should contain JSON)
    if ! echo "$output" | grep -q "{"; then
        print_error "JSON metadata not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that num_partitions is in the metadata
    if ! echo "$output" | grep -q "num_partitions"; then
        print_error "num_partitions not found in metadata"
        print_info "Output: $output"
        result=1
    fi

    # Check that validator/serializer/partition_selector are in metadata
    if ! echo "$output" | grep -q "validator"; then
        print_error "validator not found in metadata"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (verbose)" $result
}

# Test: List topics on empty driver
test_list_topics_empty() {
    echo ""
    echo "Test: List topics (empty driver)"
    echo "----------------------------------"

    local root_path="${TEST_DATA_DIR}/list-empty"
    mkdir -p "$root_path"

    # List topics without creating any
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        2>/dev/null)

    local result=0

    # Output should be empty (no topics)
    if [ -n "$output" ]; then
        print_error "Expected empty output, got non-empty output"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (empty driver)" $result
}

# Main test execution
main() {
    echo "================================================"
    echo "diaspora-ctl Test Suite"
    echo "================================================"

    setup

    # Run all tests
    test_help
    test_create_simple_topic
    test_create_topic_with_partitions
    test_create_topic_with_config_file
    test_component_metadata_files
    test_nested_metadata_parameters
    test_missing_required_args
    test_duplicate_topic
    test_multiple_topics
    test_list_topics_basic
    test_list_topics_verbose
    test_list_topics_empty

    cleanup
    print_summary

    return $?
}

# Run tests
main
exit $?

#!/usr/bin/env bash

# Test suite for diaspora-ctl fifo command with files driver

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/test-diaspora-ctl-common.sh"

# Test: FIFO daemon basic functionality
test_fifo_daemon() {
    echo ""
    echo "Test: FIFO daemon basic functionality"
    echo "---------------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-daemon"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo"
    local daemon_log="${TEST_DATA_DIR}/daemon-output.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "fifo-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging error \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start and create the control FIFO
    sleep 1

    # Check if daemon is still running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # Verify control FIFO was created
        if [ ! -p "$control_file" ]; then
            print_error "Control FIFO not created at $control_file"
            print_info "Directory contents: $(ls -la "$(dirname "$control_file")" 2>&1)"
            result=1
        else
            # Send a control command to create a producer
            echo "$producer_fifo -> fifo-topic" > "$control_file" 2>/dev/null || true

            # Give daemon time to process the command and create the producer FIFO
            sleep 1

            # Verify producer FIFO was created
            if [ ! -p "$producer_fifo" ]; then
                print_error "Producer FIFO not created at $producer_fifo"
                print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
                result=1
            else
                # Write some test data to the producer FIFO
                if ! echo "test message 1" > "$producer_fifo" 2>/dev/null; then
                    print_error "Failed to write to producer FIFO (message 1)"
                    print_info "FIFO may not have a reader or is in an invalid state"
                    result=1
                fi

                if ! echo "test message 2" > "$producer_fifo" 2>/dev/null; then
                    print_error "Failed to write to producer FIFO (message 2)"
                    print_info "FIFO may not have a reader or is in an invalid state"
                    result=1
                fi

                # Give daemon time to process the data
                sleep 1
            fi
        fi

        # Stop the daemon gracefully
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit (with timeout)
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if still running
        if kill -0 "$daemon_pid" 2>/dev/null; then
            print_error "Daemon did not shut down gracefully within timeout"
            print_info "Force killing daemon"
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon basic functionality" $result
}

# Test: FIFO daemon with batch_size option
test_fifo_daemon_options() {
    echo ""
    echo "Test: FIFO daemon with options"
    echo "--------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-options"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control-options"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo-options"
    local daemon_log="${TEST_DATA_DIR}/daemon-output-options.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "fifo-options-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging error \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start
    sleep 1

    # Check if daemon is running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # Send a control command with batch_size option
        echo "$producer_fifo -> fifo-options-topic (batch_size=256)" > "$control_file" 2>/dev/null || true

        # Give daemon time to process
        sleep 1

        # Verify producer FIFO was created
        if [ ! -p "$producer_fifo" ]; then
            print_error "Producer FIFO not created with batch_size option"
            print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
            result=1
        fi

        # Stop the daemon
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if needed
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon with options" $result
}

# Test: FIFO daemon consumer functionality
test_fifo_daemon_consumer() {
    echo ""
    echo "Test: FIFO daemon consumer functionality"
    echo "------------------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-consumer"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control-consumer"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo-consumer"
    local consumer_fifo="${TEST_DATA_DIR}/consumer-fifo-consumer"
    local daemon_log="${TEST_DATA_DIR}/daemon-output-consumer.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "consumer-test-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging debug \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start
    sleep 1

    # Check if daemon is running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # PHASE 1: Produce messages to the topic
        print_info "Phase 1: Producing messages to topic"

        # Send producer command
        echo "$producer_fifo -> consumer-test-topic" > "$control_file" 2>/dev/null || true
        sleep 1

        # Verify producer FIFO was created
        if [ ! -p "$producer_fifo" ]; then
            print_error "Producer FIFO not created at $producer_fifo"
            print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
            result=1
        fi

        if [ $result -eq 0 ]; then
            # Write test data to producer FIFO
            if ! echo "test message 1" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 1 to producer FIFO"
                result=1
            fi

            if ! echo "test message 2" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 2 to producer FIFO"
                result=1
            fi

            if ! echo "test message 3" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 3 to producer FIFO"
                result=1
            fi

            # Give daemon time to process messages into the topic
            sleep 2
            print_info "Phase 1 complete: 3 messages produced to topic"
        fi

        # PHASE 2: Add consumer and read messages from topic
        if [ $result -eq 0 ]; then
            print_info "Phase 2: Adding consumer to read from topic"

            # Create the consumer FIFO (user's responsibility)
            if ! mkfifo "$consumer_fifo" 2>/dev/null; then
                print_error "Failed to create consumer FIFO at $consumer_fifo"
                print_info "Directory contents: $(ls -la "$(dirname "$consumer_fifo")" 2>&1)"
                result=1
            else
                print_info "Created consumer FIFO at $consumer_fifo"
            fi
        fi

        if [ $result -eq 0 ]; then
            # Start reading from consumer FIFO in background BEFORE sending consumer command
            # This ensures a reader is ready when the daemon tries to open the FIFO for writing
            timeout 10s cat "$consumer_fifo" > "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null &
            local cat_pid=$!

            # Give cat time to open the FIFO for reading
            sleep 0.5

            # Now send consumer command
            echo "$consumer_fifo <- consumer-test-topic" > "$control_file" 2>/dev/null || true
            sleep 1

            # Give cat time to open the FIFO and consumer to pull/write messages
            sleep 3

            # Stop the cat process
            kill $cat_pid 2>/dev/null || true
            wait $cat_pid 2>/dev/null || true

            # Check if messages were received
            if [ -f "${TEST_DATA_DIR}/consumer-output.txt" ]; then
                local line_count=$(wc -l < "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null || echo 0)
                if [ "$line_count" -lt 3 ]; then
                    print_error "Expected at least 3 messages, got $line_count"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                else
                    print_info "Phase 2 complete: $line_count messages consumed from topic"
                fi

                # Verify message content (should contain test messages)
                if ! grep -q "test message 1" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 1' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi

                if ! grep -q "test message 2" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 2' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi

                if ! grep -q "test message 3" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 3' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi
            else
                print_error "Consumer output file not created"
                print_info "cat process may have failed to capture data"
                result=1
            fi
        fi

        # Stop the daemon
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if needed
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$consumer_fifo" "${TEST_DATA_DIR}/consumer-output.txt" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon consumer functionality" $result
}

# Test: FIFO daemon concurrent producer/consumer
# This test verifies that both producer and consumer FIFOs can operate simultaneously.
# Note: The files driver batches events, so we use batch_size=1 for immediate writes.
test_fifo_concurrent_producer_consumer() {
    echo ""
    echo "Test: FIFO daemon concurrent producer/consumer"
    echo "-----------------------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-concurrent"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control-concurrent"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo-concurrent"
    local consumer_fifo="${TEST_DATA_DIR}/consumer-fifo-concurrent"
    local daemon_log="${TEST_DATA_DIR}/daemon-output-concurrent.log"
    local consumer_output="${TEST_DATA_DIR}/concurrent-consumer-output.txt"
    local num_messages=5

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "concurrent-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging error \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start
    sleep 1

    # Check if daemon is running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # Step 1: Create consumer FIFO and start reader
        if ! mkfifo "$consumer_fifo" 2>/dev/null; then
            print_error "Failed to create consumer FIFO at $consumer_fifo"
            result=1
        fi

        if [ $result -eq 0 ]; then
            # Start consumer reader in background BEFORE registering with daemon
            timeout 30s cat "$consumer_fifo" > "$consumer_output" 2>/dev/null &
            local consumer_cat_pid=$!

            # Give cat time to open the FIFO for reading
            sleep 0.5

            # Register producer with batch_size=1 for immediate writes
            # This is necessary because the files driver batches events
            print_info "Registering producer with batch_size=1"
            echo "$producer_fifo -> concurrent-topic (batch_size=1)" > "$control_file" 2>/dev/null || true
            sleep 1

            # Verify producer FIFO was created
            if [ ! -p "$producer_fifo" ]; then
                print_error "Producer FIFO not created at $producer_fifo"
                result=1
            fi
        fi

        if [ $result -eq 0 ]; then
            # Produce messages (keep FIFO open for all writes to avoid POLLHUP)
            print_info "Producing $num_messages messages"
            {
                for i in $(seq 1 $num_messages); do
                    echo "concurrent_message_$i"
                    sleep 0.2
                done
            } > "$producer_fifo" 2>/dev/null
            if [ $? -ne 0 ]; then
                print_error "Failed to write messages"
                result=1
            fi
            sleep 2

            # Register consumer with daemon
            print_info "Registering consumer"
            echo "$consumer_fifo <- concurrent-topic" > "$control_file" 2>/dev/null || true

            # Wait for messages to be consumed
            sleep 3

            # Stop the consumer cat process
            kill $consumer_cat_pid 2>/dev/null || true
            wait $consumer_cat_pid 2>/dev/null || true

            # Verify messages were received
            if [ -f "$consumer_output" ]; then
                local received=$(wc -l < "$consumer_output" 2>/dev/null || echo 0)
                received=$(echo "$received" | tr -d ' ')

                if [ "$received" -ge "$num_messages" ]; then
                    print_info "Received $received messages"

                    # Verify message content
                    if grep -q "concurrent_message_1" "$consumer_output" 2>/dev/null; then
                        print_info "Message content verified"
                    else
                        print_error "'concurrent_message_1' not found"
                        print_info "Consumer output: $(cat "$consumer_output" 2>&1)"
                        result=1
                    fi
                else
                    print_error "Expected at least $num_messages messages, got $received"
                    print_info "Consumer output: $(cat "$consumer_output" 2>&1)"
                    result=1
                fi
            else
                print_error "Consumer output file not created"
                result=1
            fi
        fi

        # Stop the daemon
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if needed
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -9 "$daemon_pid" 2>/dev/null || true
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$consumer_fifo" "$consumer_output" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon concurrent producer/consumer" $result
}

# Main test execution
main() {
    echo "================================================"
    echo "diaspora-ctl FIFO Test Suite"
    echo "================================================"

    setup

    # Run all FIFO tests
    test_fifo_daemon
    test_fifo_daemon_options
    test_fifo_daemon_consumer
    test_fifo_concurrent_producer_consumer

    cleanup
    print_summary

    return $?
}

# Run tests
main
exit $?

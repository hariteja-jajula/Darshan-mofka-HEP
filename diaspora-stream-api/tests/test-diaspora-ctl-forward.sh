#!/usr/bin/env bash

# Test suite for diaspora-ctl forward command with files driver

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source "${SCRIPT_DIR}/test-diaspora-ctl-common.sh"

# Test: Basic forwarding between two topics
test_forward_basic() {
    echo ""
    echo "Test: Basic forwarding between two topics"
    echo "-------------------------------------------"

    local root_path="${TEST_DATA_DIR}/forward-basic"
    mkdir -p "$root_path"

    local config_file="${TEST_DATA_DIR}/forward-basic.yaml"
    local daemon_log="${TEST_DATA_DIR}/forward-basic-daemon.log"
    local result=0

    # Create source and destination topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "source-topic" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "dest-topic" \
        > /dev/null 2>&1

    # Produce events into the source topic using the fifo daemon
    local fifo_control="${TEST_DATA_DIR}/forward-basic-fifo-control"
    local producer_fifo="${TEST_DATA_DIR}/forward-basic-producer"
    local fifo_log="${TEST_DATA_DIR}/forward-basic-fifo.log"

    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$fifo_control" \
        --logging error \
        > "$fifo_log" 2>&1 &
    local fifo_pid=$!
    sleep 1

    if ! kill -0 "$fifo_pid" 2>/dev/null; then
        print_error "FIFO daemon failed to start"
        result=1
    fi

    if [ $result -eq 0 ]; then
        echo "$producer_fifo -> source-topic (batch_size=1)" > "$fifo_control" 2>/dev/null || true
        sleep 1

        # Write test messages (keep FIFO open for all writes to avoid POLLHUP)
        {
            for i in 1 2 3; do
                echo "forward_test_$i"
                sleep 0.2
            done
        } > "$producer_fifo" 2>/dev/null || true
        sleep 2

        # Stop fifo daemon
        kill -TERM "$fifo_pid" 2>/dev/null || true
        wait "$fifo_pid" 2>/dev/null || true
    fi

    # Write YAML config for forwarding
    cat > "$config_file" <<EOF
drivers:
  local:
    type: files
    options:
      root_path: "${root_path}"

forward:
  - from: local/source-topic
    to:   local/dest-topic
EOF

    if [ $result -eq 0 ]; then
        # Start the forward daemon
        "$DIASPORA_CTL" forward \
            --config "$config_file" \
            --logging error \
            > "$daemon_log" 2>&1 &
        local forward_pid=$!
        sleep 3

        if ! kill -0 "$forward_pid" 2>/dev/null; then
            print_error "Forward daemon failed to start or exited early"
            if [ -f "$daemon_log" ]; then
                print_info "Daemon output:"
                cat "$daemon_log" | while IFS= read -r line; do
                    print_info "  $line"
                done
            fi
            result=1
        else
            # Stop forward daemon gracefully
            kill -TERM "$forward_pid" 2>/dev/null || true

            local timeout=5
            while [ $timeout -gt 0 ] && kill -0 "$forward_pid" 2>/dev/null; do
                sleep 1
                timeout=$((timeout - 1))
            done

            if kill -0 "$forward_pid" 2>/dev/null; then
                kill -9 "$forward_pid" 2>/dev/null || true
            fi

            # Now consume from dest-topic to verify events were forwarded
            local consumer_fifo="${TEST_DATA_DIR}/forward-basic-consumer"
            local consumer_output="${TEST_DATA_DIR}/forward-basic-consumer-output.txt"

            "$DIASPORA_CTL" fifo \
                --driver "files" \
                --driver.root_path "$root_path" \
                --control-file "${TEST_DATA_DIR}/forward-basic-fifo-control2" \
                --logging error \
                > "${TEST_DATA_DIR}/forward-basic-fifo2.log" 2>&1 &
            local fifo2_pid=$!
            sleep 1

            if kill -0 "$fifo2_pid" 2>/dev/null; then
                mkfifo "$consumer_fifo" 2>/dev/null || true

                # Start reader before registering consumer
                timeout 10s cat "$consumer_fifo" > "$consumer_output" 2>/dev/null &
                local cat_pid=$!
                sleep 0.5

                echo "$consumer_fifo <- dest-topic" > "${TEST_DATA_DIR}/forward-basic-fifo-control2" 2>/dev/null || true
                sleep 5

                kill $cat_pid 2>/dev/null || true
                wait $cat_pid 2>/dev/null || true

                kill -TERM "$fifo2_pid" 2>/dev/null || true
                wait "$fifo2_pid" 2>/dev/null || true

                # Verify forwarded messages
                if [ -f "$consumer_output" ]; then
                    local line_count=$(wc -l < "$consumer_output" 2>/dev/null || echo 0)
                    line_count=$(echo "$line_count" | tr -d ' ')
                    if [ "$line_count" -ge 3 ]; then
                        if grep -q "forward_test_1" "$consumer_output" 2>/dev/null && \
                           grep -q "forward_test_2" "$consumer_output" 2>/dev/null && \
                           grep -q "forward_test_3" "$consumer_output" 2>/dev/null; then
                            print_info "All 3 forwarded messages verified"
                        else
                            print_error "Not all messages found in consumer output"
                            print_info "Consumer output: $(cat "$consumer_output" 2>&1)"
                            result=1
                        fi
                    else
                        print_error "Expected at least 3 messages, got $line_count"
                        print_info "Consumer output: $(cat "$consumer_output" 2>&1)"
                        result=1
                    fi
                else
                    print_error "Consumer output file not created"
                    result=1
                fi
            else
                print_error "Second FIFO daemon failed to start"
                result=1
            fi
        fi
    fi

    # Cleanup
    rm -f "$config_file" "$daemon_log" "$fifo_control" "$producer_fifo" \
          "$fifo_log" "$consumer_fifo" "$consumer_output" \
          "${TEST_DATA_DIR}/forward-basic-fifo-control2" \
          "${TEST_DATA_DIR}/forward-basic-fifo2.log" 2>/dev/null || true

    print_result "Basic forwarding between two topics" $result
}

# Test: Multi-policy forwarding
test_forward_multi_policy() {
    echo ""
    echo "Test: Multi-policy forwarding"
    echo "------------------------------"

    local root_path="${TEST_DATA_DIR}/forward-multi"
    mkdir -p "$root_path"

    local config_file="${TEST_DATA_DIR}/forward-multi.yaml"
    local daemon_log="${TEST_DATA_DIR}/forward-multi-daemon.log"
    local result=0

    # Create topics
    for topic in src-a src-b dst-a dst-b; do
        "$DIASPORA_CTL" topic create \
            --driver "files" \
            --driver.root_path "$root_path" \
            --name "$topic" \
            > /dev/null 2>&1
    done

    # Produce events into both source topics
    local fifo_control="${TEST_DATA_DIR}/forward-multi-fifo-control"
    local producer_a="${TEST_DATA_DIR}/forward-multi-producer-a"
    local producer_b="${TEST_DATA_DIR}/forward-multi-producer-b"
    local fifo_log="${TEST_DATA_DIR}/forward-multi-fifo.log"

    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$fifo_control" \
        --logging error \
        > "$fifo_log" 2>&1 &
    local fifo_pid=$!
    sleep 1

    if ! kill -0 "$fifo_pid" 2>/dev/null; then
        print_error "FIFO daemon failed to start"
        result=1
    fi

    if [ $result -eq 0 ]; then
        echo "$producer_a -> src-a (batch_size=1)" > "$fifo_control" 2>/dev/null || true
        sleep 1
        echo "$producer_b -> src-b (batch_size=1)" > "$fifo_control" 2>/dev/null || true
        sleep 1

        echo "msg_from_a" > "$producer_a" 2>/dev/null || true
        echo "msg_from_b" > "$producer_b" 2>/dev/null || true
        sleep 2

        kill -TERM "$fifo_pid" 2>/dev/null || true
        wait "$fifo_pid" 2>/dev/null || true
    fi

    # Write YAML config
    cat > "$config_file" <<EOF
drivers:
  local:
    type: files
    options:
      root_path: "${root_path}"

forward:
  - from: local/src-a
    to:   local/dst-a
  - from: local/src-b
    to:   local/dst-b
EOF

    if [ $result -eq 0 ]; then
        # Run forward daemon
        "$DIASPORA_CTL" forward \
            --config "$config_file" \
            --logging error \
            > "$daemon_log" 2>&1 &
        local forward_pid=$!
        sleep 3

        if ! kill -0 "$forward_pid" 2>/dev/null; then
            print_error "Forward daemon failed to start"
            if [ -f "$daemon_log" ]; then
                cat "$daemon_log" | while IFS= read -r line; do
                    print_info "  $line"
                done
            fi
            result=1
        else
            kill -TERM "$forward_pid" 2>/dev/null || true
            local timeout=5
            while [ $timeout -gt 0 ] && kill -0 "$forward_pid" 2>/dev/null; do
                sleep 1
                timeout=$((timeout - 1))
            done
            if kill -0 "$forward_pid" 2>/dev/null; then
                kill -9 "$forward_pid" 2>/dev/null || true
            fi

            # Verify both destination topics received events
            local fifo_control2="${TEST_DATA_DIR}/forward-multi-fifo-control2"
            local consumer_a="${TEST_DATA_DIR}/forward-multi-consumer-a"
            local consumer_b="${TEST_DATA_DIR}/forward-multi-consumer-b"
            local output_a="${TEST_DATA_DIR}/forward-multi-output-a.txt"
            local output_b="${TEST_DATA_DIR}/forward-multi-output-b.txt"

            "$DIASPORA_CTL" fifo \
                --driver "files" \
                --driver.root_path "$root_path" \
                --control-file "$fifo_control2" \
                --logging error \
                > "${TEST_DATA_DIR}/forward-multi-fifo2.log" 2>&1 &
            local fifo2_pid=$!
            sleep 1

            if kill -0 "$fifo2_pid" 2>/dev/null; then
                mkfifo "$consumer_a" 2>/dev/null || true
                mkfifo "$consumer_b" 2>/dev/null || true

                timeout 10s cat "$consumer_a" > "$output_a" 2>/dev/null &
                local cat_a_pid=$!
                timeout 10s cat "$consumer_b" > "$output_b" 2>/dev/null &
                local cat_b_pid=$!
                sleep 0.5

                echo "$consumer_a <- dst-a" > "$fifo_control2" 2>/dev/null || true
                sleep 0.5
                echo "$consumer_b <- dst-b" > "$fifo_control2" 2>/dev/null || true
                sleep 3

                kill $cat_a_pid $cat_b_pid 2>/dev/null || true
                wait $cat_a_pid $cat_b_pid 2>/dev/null || true

                kill -TERM "$fifo2_pid" 2>/dev/null || true
                wait "$fifo2_pid" 2>/dev/null || true

                # Check dst-a
                if [ -f "$output_a" ] && grep -q "msg_from_a" "$output_a" 2>/dev/null; then
                    print_info "dst-a received msg_from_a"
                else
                    print_error "dst-a did not receive msg_from_a"
                    result=1
                fi

                # Check dst-b
                if [ -f "$output_b" ] && grep -q "msg_from_b" "$output_b" 2>/dev/null; then
                    print_info "dst-b received msg_from_b"
                else
                    print_error "dst-b did not receive msg_from_b"
                    result=1
                fi
            else
                print_error "Second FIFO daemon failed to start"
                result=1
            fi
        fi
    fi

    # Cleanup
    rm -f "$config_file" "$daemon_log" "$fifo_control" "$producer_a" "$producer_b" \
          "$fifo_log" "$fifo_control2" "$consumer_a" "$consumer_b" \
          "$output_a" "$output_b" \
          "${TEST_DATA_DIR}/forward-multi-fifo2.log" 2>/dev/null || true

    print_result "Multi-policy forwarding" $result
}

# Test: Graceful shutdown via SIGTERM
test_forward_graceful_shutdown() {
    echo ""
    echo "Test: Graceful shutdown via SIGTERM"
    echo "------------------------------------"

    local root_path="${TEST_DATA_DIR}/forward-shutdown"
    mkdir -p "$root_path"

    local config_file="${TEST_DATA_DIR}/forward-shutdown.yaml"
    local daemon_log="${TEST_DATA_DIR}/forward-shutdown-daemon.log"
    local result=0

    # Create topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "shutdown-src" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "shutdown-dst" \
        > /dev/null 2>&1

    # Write config
    cat > "$config_file" <<EOF
drivers:
  local:
    type: files
    options:
      root_path: "${root_path}"

forward:
  - from: local/shutdown-src
    to:   local/shutdown-dst
EOF

    # Start forward daemon
    "$DIASPORA_CTL" forward \
        --config "$config_file" \
        --logging error \
        > "$daemon_log" 2>&1 &
    local forward_pid=$!
    sleep 2

    if ! kill -0 "$forward_pid" 2>/dev/null; then
        print_error "Forward daemon failed to start"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        fi
        result=1
    else
        # Send SIGTERM
        kill -TERM "$forward_pid" 2>/dev/null || true

        # Wait for clean exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$forward_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        if kill -0 "$forward_pid" 2>/dev/null; then
            print_error "Daemon did not shut down gracefully within timeout"
            kill -9 "$forward_pid" 2>/dev/null || true
            result=1
        else
            # Verify clean exit (exit code 0)
            wait "$forward_pid"
            local exit_code=$?
            if [ "$exit_code" -eq 0 ]; then
                print_info "Daemon exited cleanly with code 0"
            else
                print_error "Daemon exited with code $exit_code (expected 0)"
                result=1
            fi
        fi
    fi

    # Cleanup
    rm -f "$config_file" "$daemon_log" 2>/dev/null || true

    print_result "Graceful shutdown via SIGTERM" $result
}

# Main test execution
main() {
    echo "================================================"
    echo "diaspora-ctl Forward Test Suite"
    echo "================================================"

    setup

    test_forward_basic
    test_forward_multi_policy
    test_forward_graceful_shutdown

    cleanup
    print_summary

    return $?
}

# Run tests
main
exit $?

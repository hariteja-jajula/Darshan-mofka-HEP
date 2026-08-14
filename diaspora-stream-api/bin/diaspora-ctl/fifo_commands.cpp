/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "fifo_commands.hpp"
#include "common.hpp"
#include <diaspora/Driver.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/BatchParams.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <errno.h>
#include <cstring>

namespace diaspora_ctl {

// Global flag for signal handling
static std::atomic<bool> g_shutdown_requested{false};

/**
 * @brief Signal handler for graceful shutdown
 */
static void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        spdlog::info("Received shutdown signal...");
        g_shutdown_requested = true;
    }
}

/**
 * @brief Create and initialize a Diaspora driver
 */
static diaspora::Driver create_driver(const std::string& driver_name,
                                       const diaspora::Metadata& driver_config) {
    try {
        auto driver = diaspora::Driver::New(driver_name.c_str(), driver_config);
        if (!driver) {
            spdlog::error("Failed to create driver '{}'", driver_name);
            std::exit(1);
        }
        return driver;
    } catch (const diaspora::Exception& e) {
        spdlog::error("Failed to create driver '{}': {}", driver_name, e.what());
        std::exit(1);
    } catch (const std::exception& e) {
        spdlog::error("Unexpected error creating driver: {}", e.what());
        std::exit(1);
    }
}

/**
 * @brief Command type enumeration
 */
enum class CommandType {
    Producer,   // fifo-name -> topic-name
    Consumer,   // fifo-name <- topic-name
    Invalid
};

/**
 * @brief Parse a control command in the format:
 *        "<fifo-name> -> <topic-name> (key1=value1, ...)" for producers
 *        "<fifo-name> <- <topic-name> (key1=value1, ...)" for consumers
 */
static CommandType parse_control_command(const std::string& command,
                                          std::string& fifo_name,
                                          std::string& topic_name,
                                          std::unordered_map<std::string, std::string>& options) {
    // Helper function to trim whitespace
    auto trim = [](std::string& s) {
        s.erase(0, s.find_first_not_of(" \t\r\n"));
        s.erase(s.find_last_not_of(" \t\r\n") + 1);
    };

    // Determine command type and find the arrow separator
    CommandType cmd_type = CommandType::Invalid;
    size_t arrow_pos = std::string::npos;
    size_t arrow_len = 0;

    // Check for producer arrow " -> "
    arrow_pos = command.find(" -> ");
    if (arrow_pos != std::string::npos) {
        cmd_type = CommandType::Producer;
        arrow_len = 4;
    } else {
        // Check for consumer arrow " <- "
        arrow_pos = command.find(" <- ");
        if (arrow_pos != std::string::npos) {
            cmd_type = CommandType::Consumer;
            arrow_len = 4;
        }
    }

    if (cmd_type == CommandType::Invalid) {
        return CommandType::Invalid;
    }

    fifo_name = command.substr(0, arrow_pos);
    trim(fifo_name);

    std::string rest = command.substr(arrow_pos + arrow_len);
    trim(rest);

    // Check for optional key-value pairs in parentheses
    auto paren_pos = rest.find('(');
    if (paren_pos != std::string::npos) {
        // Extract topic name before the parenthesis
        topic_name = rest.substr(0, paren_pos);
        trim(topic_name);

        // Extract and parse key-value pairs
        auto close_paren_pos = rest.find(')', paren_pos);
        if (close_paren_pos == std::string::npos) {
            spdlog::warn("Missing closing parenthesis in command");
            return CommandType::Invalid;
        }

        std::string options_str = rest.substr(paren_pos + 1, close_paren_pos - paren_pos - 1);
        trim(options_str);

        if (!options_str.empty()) {
            // Split on commas
            std::stringstream ss(options_str);
            std::string pair;
            while (std::getline(ss, pair, ',')) {
                trim(pair);

                // Split on equals sign
                auto eq_pos = pair.find('=');
                if (eq_pos != std::string::npos) {
                    std::string key = pair.substr(0, eq_pos);
                    std::string value = pair.substr(eq_pos + 1);
                    trim(key);
                    trim(value);

                    if (!key.empty()) {
                        options[key] = value;
                    }
                } else {
                    spdlog::warn("Invalid key-value pair (missing '='): '{}'", pair);
                }
            }
        }
    } else {
        // No parentheses, just topic name
        topic_name = rest;
        trim(topic_name);
    }

    // Validate that fifo_name and topic_name are not empty
    if (fifo_name.empty() || topic_name.empty()) {
        return CommandType::Invalid;
    }

    return cmd_type;
}

/**
 * @brief Structure to hold information about a producer FIFO
 */
struct ProducerInfo {
    int fd;
    std::string fifo_path;
    std::string topic_name;
    diaspora::Producer producer;
    std::string read_buffer;
    std::unordered_map<std::string, std::string> options;
};

/**
 * @brief Structure to hold information about a consumer FIFO
 */
struct ConsumerInfo {
    int fd;
    std::string fifo_path;
    std::string topic_name;
    diaspora::Consumer consumer;
    std::unordered_map<std::string, std::string> options;
    std::thread worker_thread;
    std::atomic<bool> shutdown_requested;

    ConsumerInfo() : fd(-1), shutdown_requested(false) {}

    // Delete copy constructor and copy assignment
    ConsumerInfo(const ConsumerInfo&) = delete;
    ConsumerInfo& operator=(const ConsumerInfo&) = delete;

    // Implement move constructor and move assignment
    ConsumerInfo(ConsumerInfo&& other) noexcept
        : fd(other.fd),
          fifo_path(std::move(other.fifo_path)),
          topic_name(std::move(other.topic_name)),
          consumer(std::move(other.consumer)),
          options(std::move(other.options)),
          worker_thread(std::move(other.worker_thread)),
          shutdown_requested(other.shutdown_requested.load())
    {
        other.fd = -1;
    }

    ConsumerInfo& operator=(ConsumerInfo&& other) noexcept {
        if (this != &other) {
            fd = other.fd;
            fifo_path = std::move(other.fifo_path);
            topic_name = std::move(other.topic_name);
            consumer = std::move(other.consumer);
            options = std::move(other.options);
            worker_thread = std::move(other.worker_thread);
            shutdown_requested.store(other.shutdown_requested.load());
            other.fd = -1;
        }
        return *this;
    }
};

// Forward declaration
static void consumer_worker_thread(ConsumerInfo* info);

/**
 * @brief Initialize and open the control FIFO
 */
static int initialize_control_fifo(const std::string& control_file) {
    namespace fs = std::filesystem;

    fs::path control_path(control_file);

    // Ensure the directory exists
    if (control_path.has_parent_path()) {
        fs::create_directories(control_path.parent_path());
    }

    // Remove existing FIFO if present
    if (fs::exists(control_path)) {
        fs::remove(control_path);
    }

    // Create the control FIFO
    if (mkfifo(control_file.c_str(), 0666) == -1) {
        spdlog::error("Failed to create control FIFO: {}", std::strerror(errno));
        return -1;
    }
    spdlog::info("Created control FIFO: {}", control_file);

    // Open control FIFO for reading (non-blocking initially)
    int control_fd = open(control_file.c_str(), O_RDONLY | O_NONBLOCK);
    if (control_fd == -1) {
        spdlog::error("Failed to open control FIFO: {}", std::strerror(errno));
        return -1;
    }

    // Make it blocking after opening
    int flags = fcntl(control_fd, F_GETFL);
    fcntl(control_fd, F_SETFL, flags & ~O_NONBLOCK);

    return control_fd;
}

/**
 * @brief Handle a control command and create a producer
 */
static bool handle_control_command(
    diaspora::Driver& driver,
    const std::string& fifo_name,
    const std::string& topic_name,
    const std::unordered_map<std::string, std::string>& options,
    std::unordered_map<std::string, diaspora::TopicHandle>& topics,
    std::unordered_map<std::string, ProducerInfo>& producers) {

    namespace fs = std::filesystem;

    // Check if this FIFO is already registered
    if (producers.find(fifo_name) != producers.end()) {
        spdlog::warn("FIFO '{}' already registered", fifo_name);
        return false;
    }

    try {
        // Open the topic
        diaspora::TopicHandle topic;
        if (topics.count(topic_name)) {
            topic = topics[topic_name];
        } else {
            topic = driver.openTopic(topic_name);
            topics[topic_name] = topic;
        }

        // Parse batch_size option (default is 128)
        size_t batch_size_value = 128;
        auto batch_size_it = options.find("batch_size");
        if (batch_size_it != options.end()) {
            try {
                batch_size_value = std::stoull(batch_size_it->second);
                spdlog::debug("Using batch_size={} for producer '{}'", batch_size_value, fifo_name);
            } catch (const std::exception& e) {
                spdlog::warn("Invalid batch_size '{}' for FIFO '{}', using default (128)",
                           batch_size_it->second, fifo_name);
            }
        }

        // Create a producer for this topic with the specified batch size
        auto producer = topic.producer(diaspora::BatchSize{batch_size_value});

        // Create the FIFO if it doesn't exist
        fs::path fifo_path(fifo_name);
        if (!fs::exists(fifo_path)) {
            if (mkfifo(fifo_name.c_str(), 0666) == -1) {
                spdlog::error("Failed to create FIFO '{}': {}", fifo_name, std::strerror(errno));
                return false;
            }
            spdlog::info("Created FIFO: {}", fifo_name);
        }

        // Open the FIFO for reading (non-blocking)
        int fifo_fd = open(fifo_name.c_str(), O_RDONLY | O_NONBLOCK);
        if (fifo_fd == -1) {
            spdlog::error("Failed to open FIFO '{}': {}", fifo_name, std::strerror(errno));
            return false;
        }

        // Store the producer info
        producers[fifo_name] = ProducerInfo{
            fifo_fd,
            fifo_name,
            topic_name,
            producer,
            "",
            options
        };

        spdlog::info("Registered producer: {} -> {}", fifo_name, topic_name);
        return true;

    } catch (const diaspora::Exception& e) {
        spdlog::error("Failed to create producer: {}", e.what());
        return false;
    }
}

/**
 * @brief Handle a consumer command and create a consumer
 */
static bool handle_consumer_command(
    diaspora::Driver& driver,
    const std::string& fifo_name,
    const std::string& topic_name,
    const std::unordered_map<std::string, std::string>& options,
    std::unordered_map<std::string, diaspora::TopicHandle>& topics,
    std::unordered_map<std::string, ConsumerInfo>& consumers) {

    namespace fs = std::filesystem;

    // Check if this FIFO is already registered
    if (consumers.find(fifo_name) != consumers.end()) {
        spdlog::warn("Consumer FIFO '{}' already registered", fifo_name);
        return false;
    }

    try {
        // Open the topic
        diaspora::TopicHandle topic;
        if (topics.count(topic_name)) {
            topic = topics[topic_name];
        } else {
            topic = driver.openTopic(topic_name);
            topics[topic_name] = topic;
        }

        // Parse batch_size option (default is 128)
        size_t batch_size_value = 128;
        auto batch_size_it = options.find("batch_size");
        if (batch_size_it != options.end()) {
            try {
                batch_size_value = std::stoull(batch_size_it->second);
                spdlog::debug("Using batch_size={} for consumer '{}'", batch_size_value, fifo_name);
            } catch (const std::exception& e) {
                spdlog::warn("Invalid batch_size '{}' for consumer FIFO '{}', using default (128)",
                           batch_size_it->second, fifo_name);
            }
        }

        // Create a consumer for this topic with the specified batch size
        auto consumer = topic.consumer(fifo_name, diaspora::BatchSize{batch_size_value});

        // Check that the FIFO exists (user should have created it)
        fs::path fifo_path(fifo_name);
        if (!fs::exists(fifo_path)) {
            spdlog::error("Consumer FIFO '{}' does not exist. Please create it first with mkfifo.", fifo_name);
            return false;
        }

        // Verify it's actually a FIFO
        struct stat st;
        if (stat(fifo_name.c_str(), &st) == -1) {
            spdlog::error("Failed to stat '{}': {}", fifo_name, std::strerror(errno));
            return false;
        }
        if (!S_ISFIFO(st.st_mode)) {
            spdlog::error("'{}' exists but is not a FIFO", fifo_name);
            return false;
        }

        // Create ConsumerInfo using emplace to construct in-place
        // Note: We don't open the FIFO here - the worker thread will do it
        auto [it, inserted] = consumers.try_emplace(fifo_name);
        if (!inserted) {
            spdlog::error("Consumer FIFO '{}' already exists in map", fifo_name);
            return false;
        }

        ConsumerInfo& info = it->second;
        info.fd = -1;  // Will be opened by worker thread
        info.fifo_path = fifo_name;
        info.topic_name = topic_name;
        info.consumer = std::move(consumer);
        info.options = options;
        info.shutdown_requested.store(false);

        // Start the worker thread
        info.worker_thread = std::thread(consumer_worker_thread, &info);

        spdlog::info("Registered consumer: {} <- {}", fifo_name, topic_name);
        return true;

    } catch (const diaspora::Exception& e) {
        spdlog::error("Failed to create consumer: {}", e.what());
        return false;
    }
}

/**
 * @brief Process data from a producer FIFO
 */
static bool handle_producer_data(ProducerInfo& info) {
    char buffer[4096];
    ssize_t n = read(info.fd, buffer, sizeof(buffer));

    if (n > 0) {
        info.read_buffer.append(buffer, n);

        // Determine the format (default is "raw")
        std::string format = "raw";
        auto format_it = info.options.find("format");
        if (format_it != info.options.end()) {
            format = format_it->second;
        }

        bool format_is_json = false;
        if (format == "json") {
            format_is_json = true;
        } else if (format != "raw") {
            spdlog::warn("Unknown format '{}' for FIFO '{}', using 'raw' instead",
                         format, info.fifo_path);
        }

        // Process complete lines
        size_t pos;
        while ((pos = info.read_buffer.find('\n')) != std::string::npos) {
            std::string line = info.read_buffer.substr(0, pos);
            info.read_buffer.erase(0, pos + 1);

            if (!line.empty()) {
                try {
                    // Format the line according to the format option
                    if (!format_is_json) {
                        line = "\"" + line + "\"";
                    }

                    // Push the formatted line as metadata to the producer
                    diaspora::Metadata metadata(line);
                    info.producer.push(metadata);
                    spdlog::debug("Pushed to '{}': {}", info.topic_name, line);
                } catch (const std::exception& e) {
                    spdlog::error("Failed to push event: {}", e.what());
                }
            }
        }
        return true;
    } else if (n == 0) {
        // EOF - writer closed the FIFO; flush buffered events to disk before returning
        spdlog::info("Writer closed FIFO: {}", info.fifo_path);
        info.producer.flush().wait(-1);
        return false;
    } else {
        // Read error
        spdlog::error("Read error on FIFO '{}': {}", info.fifo_path, std::strerror(errno));
        return false;
    }
}

/**
 * @brief Clean up a single producer
 */
static void cleanup_producer(ProducerInfo& info) {
    namespace fs = std::filesystem;

    spdlog::info("Closing and removing producer for FIFO: {}", info.fifo_path);

    // Close the file descriptor
    close(info.fd);

    // Remove the FIFO file
    try {
        fs::remove(info.fifo_path);
        spdlog::info("Removed FIFO file: {}", info.fifo_path);
    } catch (const std::exception& e) {
        spdlog::warn("Failed to remove FIFO file '{}': {}", info.fifo_path, e.what());
    }
}

/**
 * @brief Worker thread for consuming events and writing to FIFO
 */
static void consumer_worker_thread(ConsumerInfo* info) {
    spdlog::debug("Consumer worker thread started for FIFO: {}", info->fifo_path);

    // Open the FIFO for writing with retry logic
    // This allows time for a reader to open the FIFO before we try to write
    constexpr int max_open_retries = 50;  // 5 seconds total (50 * 100ms)
    int open_retry_count = 0;

    while (info->fd == -1 && open_retry_count < max_open_retries && !info->shutdown_requested) {
        info->fd = open(info->fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
        if (info->fd == -1) {
            if (errno == ENXIO) {
                // No reader yet, retry after a short delay
                open_retry_count++;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
                // Other error, log and exit
                spdlog::error("Failed to open consumer FIFO '{}': {}",
                            info->fifo_path, std::strerror(errno));
                return;
            }
        }
    }

    if (info->fd == -1) {
        spdlog::error("Failed to open consumer FIFO '{}' after {} retries: timed out waiting for reader",
                     info->fifo_path, max_open_retries);
        return;
    }

    spdlog::info("Consumer FIFO '{}' opened successfully for writing", info->fifo_path);

    while (!info->shutdown_requested) {
        try {
            // Pull event with timeout
            auto future = info->consumer.pull();
            auto maybe_event = future.wait(100);  // 100ms timeout

            if (!maybe_event.has_value()) {
                // Timeout, continue loop
                continue;
            }

            auto event = maybe_event.value();
            if (!event) {
                // No more events
                spdlog::debug("No more events for consumer FIFO: {}", info->fifo_path);
                continue;
            }

            // Get metadata as JSON string
            std::string output = event.metadata().json().dump();

            // Apply format option
            auto format_it = info->options.find("format");
            if (format_it == info->options.end() || format_it->second == "raw") {
                // Extract string value from JSON if it's a string literal
                try {
                    nlohmann::json j = nlohmann::json::parse(output);
                    if (j.is_string()) {
                        output = j.get<std::string>();
                    }
                } catch (...) {
                    // Keep output as-is if parsing fails
                }
            }
            // else format=json: use output as-is

            // Write to FIFO with newline
            output += "\n";
            ssize_t written = write(info->fd, output.c_str(), output.size());

            if (written < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // FIFO full, try again next loop
                    spdlog::debug("Consumer FIFO '{}' is full, retrying", info->fifo_path);
                    continue;
                } else {
                    spdlog::error("Write error on consumer FIFO '{}': {}",
                                 info->fifo_path, std::strerror(errno));
                    break;  // Exit thread on error
                }
            } else if (written < static_cast<ssize_t>(output.size())) {
                spdlog::warn("Partial write on consumer FIFO '{}': wrote {}/{} bytes",
                            info->fifo_path, written, output.size());
            } else {
                spdlog::debug("Wrote event to consumer FIFO '{}': {}", info->fifo_path, output);
            }

            // Note: Not acknowledging events per user request

        } catch (const std::exception& e) {
            spdlog::error("Error in consumer worker thread for '{}': {}",
                         info->fifo_path, e.what());
            break;
        }
    }

    spdlog::debug("Consumer worker thread exiting for FIFO: {}", info->fifo_path);
}

/**
 * @brief Clean up a single consumer
 */
static void cleanup_consumer(ConsumerInfo& info) {
    spdlog::info("Closing consumer for FIFO: {}", info.fifo_path);

    // Signal thread to stop
    info.shutdown_requested = true;

    // Wait for thread to finish
    if (info.worker_thread.joinable()) {
        info.worker_thread.join();
    }

    // Close the file descriptor if it was opened
    if (info.fd != -1) {
        close(info.fd);
    }

    // Note: We don't remove the FIFO file since the user created it
    spdlog::info("Consumer for FIFO '{}' closed (FIFO file not removed)", info.fifo_path);
}

/**
 * @brief Run the daemon with the specified driver and control file
 */
static void run_daemon(diaspora::Driver& driver, const std::string& control_file) {
    namespace fs = std::filesystem;

    try {
        spdlog::info("Starting Diaspora FIFO daemon...");
        spdlog::info("Driver: {}", fmt::ptr(&driver));
        spdlog::info("Control file: {}", control_file);

        // Initialize control FIFO
        int control_fd = initialize_control_fifo(control_file);
        if (control_fd == -1) {
            return;
        }

        spdlog::info("Daemon is running. Waiting for commands on control file...");
        spdlog::info("Send commands in format:");
        spdlog::info("  Producer: <fifo-name> -> <topic-name> (key=value, ...)");
        spdlog::info("  Consumer: <fifo-name> <- <topic-name> (key=value, ...)");
        spdlog::info("Press Ctrl+C to stop.");

        // Cached TopicHandle instances
        std::unordered_map<std::string, diaspora::TopicHandle> topics;

        // Map of FIFO paths to producer info
        std::unordered_map<std::string, ProducerInfo> producers;

        // Map of FIFO paths to consumer info
        std::unordered_map<std::string, ConsumerInfo> consumers;

        // Buffer for control file reads
        std::string control_buffer;

        // Main event loop
        while (!g_shutdown_requested) {
            // Build poll file descriptor array
            std::vector<struct pollfd> fds;
            fds.push_back({control_fd, POLLIN, 0});

            // Add all producer FIFOs
            std::vector<std::string> fifo_paths;
            for (const auto& [path, info] : producers) {
                fds.push_back({info.fd, POLLIN, 0});
                fifo_paths.push_back(path);
            }

            // Poll with 100ms timeout
            int ret = poll(fds.data(), fds.size(), 100);

            if (ret < 0) {
                if (errno == EINTR) continue;
                spdlog::error("Poll error: {}", std::strerror(errno));
                break;
            }

            if (ret == 0) continue; // Timeout

            // Check control file for new commands
            if (fds[0].revents & POLLIN) {
                char buffer[4096];
                ssize_t n = read(control_fd, buffer, sizeof(buffer));
                if (n > 0) {
                    control_buffer.append(buffer, n);

                    // Process complete lines
                    size_t pos;
                    while ((pos = control_buffer.find('\n')) != std::string::npos) {
                        std::string command = control_buffer.substr(0, pos);
                        control_buffer.erase(0, pos + 1);

                        std::string fifo_name, topic_name;
                        std::unordered_map<std::string, std::string> options;
                        CommandType cmd_type = parse_control_command(command, fifo_name, topic_name, options);

                        if (cmd_type == CommandType::Producer) {
                            // Log the received command
                            if (options.empty()) {
                                spdlog::info("Received command: '{}' -> '{}'", fifo_name, topic_name);
                            } else {
                                std::string opts_str;
                                for (const auto& [key, value] : options) {
                                    if (!opts_str.empty()) opts_str += ", ";
                                    opts_str += key + "=" + value;
                                }
                                spdlog::info("Received command: '{}' -> '{}' ({})",
                                           fifo_name, topic_name, opts_str);
                            }

                            // Handle the producer command
                            handle_control_command(driver, fifo_name, topic_name, options, topics, producers);
                        } else if (cmd_type == CommandType::Consumer) {
                            // Log the received command
                            if (options.empty()) {
                                spdlog::info("Received command: '{}' <- '{}'", fifo_name, topic_name);
                            } else {
                                std::string opts_str;
                                for (const auto& [key, value] : options) {
                                    if (!opts_str.empty()) opts_str += ", ";
                                    opts_str += key + "=" + value;
                                }
                                spdlog::info("Received command: '{}' <- '{}' ({})",
                                           fifo_name, topic_name, opts_str);
                            }

                            // Handle the consumer command
                            handle_consumer_command(driver, fifo_name, topic_name, options, topics, consumers);
                        } else {
                            spdlog::warn("Invalid command format: '{}'", command);
                        }
                    }
                }
            }

            // Track FIFOs that need to be closed
            std::vector<std::string> fifos_to_close;

            // Check producer FIFOs for data
            for (size_t i = 1; i < fds.size(); ++i) {
                const std::string& fifo_path = fifo_paths[i - 1];
                bool should_close = false;

                if (fds[i].revents & POLLIN) {
                    auto& info = producers[fifo_path];
                    if (!handle_producer_data(info)) {
                        should_close = true;
                    }
                }

                if (fds[i].revents & (POLLERR | POLLHUP)) {
                    spdlog::warn("POLLHUP/POLLERR on FIFO: {}", fifo_path);
                    should_close = true;
                }

                if (should_close) {
                    fifos_to_close.push_back(fifo_path);
                }
            }

            // Clean up closed FIFOs
            for (const auto& fifo_path : fifos_to_close) {
                auto it = producers.find(fifo_path);
                if (it != producers.end()) {
                    cleanup_producer(it->second);
                    producers.erase(it);
                }
            }
        }

        spdlog::info("Shutting down daemon...");

        // Cleanup: close control file
        close(control_fd);
        fs::remove(control_file);

        // Cleanup: close all producer FIFOs
        for (auto& [path, info] : producers) {
            cleanup_producer(info);
        }
        producers.clear();

        // Cleanup: close all consumer FIFOs and threads
        for (auto& [path, info] : consumers) {
            cleanup_consumer(info);
        }
        consumers.clear();

    } catch (const std::exception& e) {
        spdlog::error("Error in daemon operation: {}", e.what());
        std::exit(1);
    }
}

int fifo_daemon(int argc, char** argv) {
    try {
        // Extract --driver.* metadata arguments before TCLAP parsing
        auto parsed_args = extract_metadata_args(argc, argv);

        // Prepare arguments for TCLAP
        int filtered_argc = parsed_args.filtered_argv.size();
        char** filtered_argv = parsed_args.filtered_argv.data();

        // Set up command-line argument parser
        TCLAP::CmdLine cmd(
            "Diaspora Stream FIFO Daemon - A daemon for managing Diaspora streaming operations",
            ' ',
            "0.4.0"
        );

        TCLAP::ValueArg<std::string> driverArg(
            "",
            "driver",
            "Name of the Diaspora driver to use (e.g., \"simple:libdiaspora-simple-backend.so\", \"mofka\")",
            true,
            "",
            "string",
            cmd
        );

        TCLAP::ValueArg<std::string> driverConfigArg(
            "",
            "driver-config",
            "Path to JSON configuration file for the driver (optional)",
            false,
            "",
            "filename",
            cmd
        );

        TCLAP::ValueArg<std::string> controlFileArg(
            "",
            "control-file",
            "Path to the daemon's control file",
            true,
            "",
            "filename",
            cmd
        );

        std::vector<std::string> allowed_levels{"trace", "debug", "info", "warn", "error", "critical", "off"};
        TCLAP::ValuesConstraint<std::string> level_constraint(allowed_levels);
        TCLAP::ValueArg<std::string> loggingArg(
            "",
            "logging",
            "Logging level",
            false,
            "info",
            &level_constraint,
            cmd
        );

        // Parse command-line arguments (using filtered argv)
        cmd.parse(filtered_argc, filtered_argv);

        // Configure spdlog
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        std::string driver_name = driverArg.getValue();
        std::string driver_config_path = driverConfigArg.getValue();
        std::string control_file = controlFileArg.getValue();

        // Load driver configuration from file if provided
        std::string driver_config_str = read_config_file(driver_config_path);
        nlohmann::json driver_config = nlohmann::json::parse(driver_config_str);

        // Merge command-line metadata with config file metadata
        // Command-line arguments take precedence
        driver_config.merge_patch(parsed_args.metadata["driver"]);

        spdlog::debug("Driver config: {}", driver_config.dump());

        // Create Metadata object from merged config
        diaspora::Metadata driver_metadata{driver_config.dump()};

        // Create the driver
        auto driver = create_driver(driver_name, driver_metadata);

        // Set up signal handlers for graceful shutdown.
        // Ignore SIGPIPE so that writes to a consumer FIFO whose reader has
        // disappeared return -1/EPIPE instead of killing the daemon; the
        // consumer worker thread already handles that errno cleanly.
        std::signal(SIGPIPE, SIG_IGN);
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        // Run the daemon
        run_daemon(driver, control_file);

        return 0;

    } catch (const TCLAP::ArgException& e) {
        spdlog::error("Error parsing arguments: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}

} // namespace diaspora_ctl

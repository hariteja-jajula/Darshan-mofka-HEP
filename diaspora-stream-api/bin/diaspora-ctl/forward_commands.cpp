/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "forward_commands.hpp"
#include "common.hpp"
#include <diaspora/Driver.hpp>
#include <diaspora/Exception.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Consumer.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/DataSelector.hpp>
#include <diaspora/DataAllocator.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <csignal>
#include <unordered_map>
#include <dlfcn.h>

namespace diaspora_ctl {

// ── Signal handling ─────────────────────────────────────────────────────────

static std::atomic<bool> g_forward_shutdown{false};

static void forward_signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        spdlog::info("Received shutdown signal...");
        g_forward_shutdown = true;
    }
}

// ── Data structures ─────────────────────────────────────────────────────────

struct DriverConfig {
    std::string name;
    std::string type;
    nlohmann::json metadata;
};

struct ForwardPolicy {
    std::string source_driver;
    std::string source_topic;
    std::string dest_driver;
    std::string dest_topic;
    std::string data_selector_spec;
    std::string data_allocator_spec;
};

struct ForwardWorker {
    ForwardPolicy policy;
    diaspora::Consumer consumer;
    diaspora::Producer producer;
    std::thread thread;
};

// ── Helpers ─────────────────────────────────────────────────────────────────

static std::pair<std::string, std::string> parse_topic_ref(const std::string& ref) {
    auto pos = ref.find('/');
    if (pos == std::string::npos || pos == 0 || pos == ref.size() - 1) {
        throw std::runtime_error(
            "Invalid topic reference '" + ref + "': expected 'driver/topic'");
    }
    return {ref.substr(0, pos), ref.substr(pos + 1)};
}

static nlohmann::json yaml_node_to_json(const YAML::Node& node) {
    switch (node.Type()) {
        case YAML::NodeType::Null:
            return nullptr;
        case YAML::NodeType::Scalar: {
            // Try bool, int, double, fall back to string
            try { return node.as<bool>(); } catch (...) {}
            try { return node.as<int64_t>(); } catch (...) {}
            try { return node.as<double>(); } catch (...) {}
            return node.as<std::string>();
        }
        case YAML::NodeType::Sequence: {
            auto arr = nlohmann::json::array();
            for (const auto& elem : node) {
                arr.push_back(yaml_node_to_json(elem));
            }
            return arr;
        }
        case YAML::NodeType::Map: {
            auto obj = nlohmann::json::object();
            for (const auto& kv : node) {
                obj[kv.first.as<std::string>()] = yaml_node_to_json(kv.second);
            }
            return obj;
        }
        default:
            return nullptr;
    }
}

struct ParsedConfig {
    std::unordered_map<std::string, DriverConfig> drivers;
    std::vector<ForwardPolicy> policies;
};

static ParsedConfig parse_yaml_config(const std::string& path) {
    ParsedConfig config;

    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to parse YAML config: " + std::string(e.what()));
    }

    // Parse drivers:
    if (!root["drivers"]) {
        throw std::runtime_error("Config missing 'drivers' section");
    }
    const auto& drivers_node = root["drivers"];
    for (const auto& kv : drivers_node) {
        auto name = kv.first.as<std::string>();
        const auto& drv = kv.second;
        DriverConfig dc;
        dc.name = name;
        if (!drv["type"]) {
            throw std::runtime_error(
                "Driver '" + name + "' missing required 'type' field");
        }
        dc.type = drv["type"].as<std::string>();
        if (drv["options"]) {
            dc.metadata = yaml_node_to_json(drv["options"]);
        } else {
            dc.metadata = nlohmann::json::object();
        }
        config.drivers[name] = std::move(dc);
    }

    // Parse forward:
    if (!root["forward"]) {
        throw std::runtime_error("Config missing 'forward' entries");
    }
    const auto& forwards = root["forward"];
    for (const auto& fwd : forwards) {
        ForwardPolicy fp;
        if (!fwd["from"] || !fwd["to"]) {
            throw std::runtime_error(
                "forward entry missing 'from' or 'to' field");
        }
        auto [src_drv, src_topic] = parse_topic_ref(fwd["from"].as<std::string>());
        auto [dst_drv, dst_topic] = parse_topic_ref(fwd["to"].as<std::string>());
        fp.source_driver = std::move(src_drv);
        fp.source_topic = std::move(src_topic);
        fp.dest_driver = std::move(dst_drv);
        fp.dest_topic = std::move(dst_topic);
        if (fwd["data_selector"]) {
            fp.data_selector_spec = fwd["data_selector"].as<std::string>();
        }
        if (fwd["data_allocator"]) {
            fp.data_allocator_spec = fwd["data_allocator"].as<std::string>();
        }
        config.policies.push_back(std::move(fp));
    }

    return config;
}

// ── Plugin loading ──────────────────────────────────────────────────────────

template<typename FnPtr>
static FnPtr load_plugin(const std::string& spec) {
    auto colon = spec.find(':');
    if (colon == std::string::npos || colon == 0 || colon == spec.size() - 1) {
        throw std::runtime_error(
            "Invalid plugin spec '" + spec + "': expected 'libfoo.so:symbol_name'");
    }
    auto lib_path = spec.substr(0, colon);
    auto sym_name = spec.substr(colon + 1);

    void* handle = dlopen(lib_path.c_str(), RTLD_NOW);
    if (!handle) {
        throw std::runtime_error(
            "dlopen failed for '" + lib_path + "': " + dlerror());
    }

    dlerror(); // clear
    void* sym = dlsym(handle, sym_name.c_str());
    const char* err = dlerror();
    if (err) {
        throw std::runtime_error(
            "dlsym failed for '" + sym_name + "' in '" + lib_path + "': " + err);
    }

    return reinterpret_cast<FnPtr>(sym);
}

// ── Worker thread ───────────────────────────────────────────────────────────

static void forward_worker_thread(ForwardWorker* worker) {
    const auto& p = worker->policy;
    spdlog::info("Forward worker started: {}/{} -> {}/{}",
                 p.source_driver, p.source_topic,
                 p.dest_driver, p.dest_topic);

    while (!g_forward_shutdown) {
        try {
            auto future = worker->consumer.pull();
            auto maybe_event = future.wait(200);

            if (!maybe_event.has_value()) {
                continue;
            }

            auto event = maybe_event.value();
            if (!event) {
                spdlog::debug("No more events for {}/{}", p.source_driver, p.source_topic);
                continue;
            }

            if (event.id() == diaspora::NoMoreEvents) {
                spdlog::info("Reached end of topic {}/{}", p.source_driver, p.source_topic);
                continue;
            }

            auto push_future = worker->producer.push(event.metadata(), event.data());
            auto push_result = push_future.wait(5000);

            if (!push_result.has_value()) {
                spdlog::warn("Push timed out for {}/{} -> {}/{}",
                             p.source_driver, p.source_topic,
                             p.dest_driver, p.dest_topic);
                continue;
            }

            event.acknowledge();

            spdlog::debug("Forwarded event from {}/{} to {}/{}",
                          p.source_driver, p.source_topic,
                          p.dest_driver, p.dest_topic);

        } catch (const std::exception& e) {
            if (g_forward_shutdown) break;
            spdlog::error("Error in forward worker {}/{} -> {}/{}: {}",
                          p.source_driver, p.source_topic,
                          p.dest_driver, p.dest_topic, e.what());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    // Flush producer before exiting
    try {
        spdlog::info("Flushing producer for {}/{}...", p.dest_driver, p.dest_topic);
        auto flush_future = worker->producer.flush();
        flush_future.wait(5000);
    } catch (const std::exception& e) {
        spdlog::error("Error flushing producer for {}/{}: {}",
                      p.dest_driver, p.dest_topic, e.what());
    }

    spdlog::info("Forward worker stopped: {}/{} -> {}/{}",
                 p.source_driver, p.source_topic,
                 p.dest_driver, p.dest_topic);
}

// ── Entry point ─────────────────────────────────────────────────────────────

int forward_daemon(int argc, char** argv) {
    try {
        TCLAP::CmdLine cmd(
            "Diaspora Stream Forwarding Daemon - Forwards events between drivers/topics",
            ' ',
            "0.5.1"
        );

        TCLAP::ValueArg<std::string> configArg(
            "",
            "config",
            "Path to YAML configuration file",
            true,
            "",
            "filename",
            cmd
        );

        std::vector<std::string> allowed_levels{
            "trace", "debug", "info", "warn", "error", "critical", "off"};
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

        cmd.parse(argc, argv);

        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        // Parse YAML config
        auto config = parse_yaml_config(configArg.getValue());

        spdlog::info("Loaded {} driver(s) and {} forwarding policy(ies)",
                     config.drivers.size(), config.policies.size());

        // Instantiate drivers
        std::unordered_map<std::string, diaspora::Driver> drivers;
        for (const auto& [name, dc] : config.drivers) {
            spdlog::info("Creating driver '{}' (type={})", name, dc.type);
            diaspora::Metadata meta{dc.metadata.dump()};
            auto driver = diaspora::Driver::New(dc.type.c_str(), meta);
            if (!driver) {
                spdlog::error("Failed to create driver '{}'", name);
                return 1;
            }
            drivers[name] = driver;
        }

        // Open topics (with cache to avoid re-opening)
        std::unordered_map<std::string, diaspora::TopicHandle> topic_cache;

        auto get_topic = [&](const std::string& driver_name,
                             const std::string& topic_name) -> diaspora::TopicHandle {
            auto key = driver_name + "/" + topic_name;
            auto it = topic_cache.find(key);
            if (it != topic_cache.end()) return it->second;

            auto drv_it = drivers.find(driver_name);
            if (drv_it == drivers.end()) {
                throw std::runtime_error("Unknown driver '" + driver_name + "'");
            }
            auto topic = drv_it->second.openTopic(topic_name);
            topic_cache[key] = topic;
            return topic;
        };

        // Create workers
        std::vector<std::unique_ptr<ForwardWorker>> workers;
        for (auto& policy : config.policies) {
            auto worker = std::make_unique<ForwardWorker>();
            worker->policy = policy;

            auto src_topic = get_topic(policy.source_driver, policy.source_topic);
            auto dst_topic = get_topic(policy.dest_driver, policy.dest_topic);

            // Load optional plugins
            diaspora::DataSelector data_selector;
            diaspora::DataAllocator data_allocator;

            if (!policy.data_selector_spec.empty()) {
                using SelectorFactory = diaspora::DataSelector(*)();
                auto factory = load_plugin<SelectorFactory>(policy.data_selector_spec);
                data_selector = factory();
                spdlog::info("Loaded DataSelector from '{}'", policy.data_selector_spec);
            }

            if (!policy.data_allocator_spec.empty()) {
                using AllocatorFactory = diaspora::DataAllocator(*)();
                auto factory = load_plugin<AllocatorFactory>(policy.data_allocator_spec);
                data_allocator = factory();
                spdlog::info("Loaded DataAllocator from '{}'", policy.data_allocator_spec);
            }

            auto consumer_name = policy.source_driver + "/" + policy.source_topic
                               + "->" + policy.dest_driver + "/" + policy.dest_topic;

            worker->consumer = src_topic.consumer(consumer_name,
                                                  data_selector, data_allocator);
            worker->producer = dst_topic.producer();

            spdlog::info("Created forward policy: {}/{} -> {}/{}",
                         policy.source_driver, policy.source_topic,
                         policy.dest_driver, policy.dest_topic);

            workers.push_back(std::move(worker));
        }

        // Install signal handlers
        std::signal(SIGINT, forward_signal_handler);
        std::signal(SIGTERM, forward_signal_handler);

        // Start worker threads
        for (auto& w : workers) {
            w->thread = std::thread(forward_worker_thread, w.get());
        }

        spdlog::info("Forwarding daemon running. Press Ctrl+C to stop.");

        // Wait for all threads to finish
        for (auto& w : workers) {
            if (w->thread.joinable()) {
                w->thread.join();
            }
        }

        spdlog::info("Forwarding daemon stopped.");
        return 0;

    } catch (const TCLAP::ArgException& e) {
        spdlog::error("Error parsing arguments: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const diaspora::Exception& e) {
        spdlog::error("Diaspora error: {}", e.what());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}

} // namespace diaspora_ctl

/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "topic_commands.hpp"
#include "common.hpp"
#include <diaspora/Driver.hpp>
#include <diaspora/Validator.hpp>
#include <diaspora/Serializer.hpp>
#include <diaspora/PartitionSelector.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace diaspora_ctl {

int topic_create(int argc, char** argv) {
    try {
        // Extract --driver.* and --topic.* metadata arguments before TCLAP parsing
        auto parsed_args = extract_metadata_args(argc, argv);

        // Prepare arguments for TCLAP
        int filtered_argc = parsed_args.filtered_argv.size();
        char** filtered_argv = parsed_args.filtered_argv.data();

        TCLAP::CmdLine cmd("Create a new topic", ' ', "1.0");

        TCLAP::ValueArg<std::string> driverArg("", "driver", "Driver name", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> driverConfigArg("", "driver-config", "Driver config file", false, "", "filename", cmd);
        TCLAP::ValueArg<std::string> nameArg("", "name", "Topic name", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> topicConfigArg("", "topic-config", "Topic config file", false, "", "filename", cmd);
        TCLAP::ValueArg<std::string> serializerArg("", "serializer", "Serializer name", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> validatorArg("", "validator", "Validator name", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> partitionSelectorArg("", "partition-selector", "Partition selector name", false, "", "string", cmd);

        std::vector<std::string> allowed_levels{"trace", "debug", "info", "warn", "error", "critical", "off"};
        TCLAP::ValuesConstraint<std::string> level_constraint(allowed_levels);
        TCLAP::ValueArg<std::string> loggingArg("", "logging", "Logging level", false, "info", &level_constraint, cmd);

        cmd.parse(filtered_argc, filtered_argv);

        // Set logging level
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        // Read configuration files
        std::string driver_config_str = read_config_file(driverConfigArg.getValue());
        std::string topic_config_str = read_config_file(topicConfigArg.getValue());

        // Parse config files as JSON
        nlohmann::json driver_config = nlohmann::json::parse(driver_config_str);
        nlohmann::json topic_config = nlohmann::json::parse(topic_config_str);

        // Merge command-line metadata with config file metadata
        // Command-line arguments take precedence
        driver_config.merge_patch(parsed_args.metadata["driver"]);
        topic_config.merge_patch(parsed_args.metadata["topic"]);

        spdlog::debug("Driver config: {}", driver_config.dump());
        spdlog::debug("Topic config: {}", topic_config.dump());

        spdlog::info("Creating driver: {}", driverArg.getValue());
        auto driver = diaspora::Driver::New(driverArg.getValue().c_str(), diaspora::Metadata{driver_config.dump()});

        spdlog::info("Creating topic: {}", nameArg.getValue());

        // Create validator, serializer, and partition selector from metadata
        diaspora::Validator validator;
        diaspora::Serializer serializer;
        diaspora::PartitionSelector selector;

        // Handle validator creation
        if (!validatorArg.getValue().empty()) {
            spdlog::debug("Creating validator: {}", validatorArg.getValue());
            auto validator_metadata = parsed_args.metadata["validator"];
            validator_metadata["type"] = validatorArg.getValue();
            spdlog::debug("Validator metadata: {}", validator_metadata.dump());
            validator = diaspora::Validator::FromMetadata(diaspora::Metadata{validator_metadata.dump()});
        }

        // Handle serializer creation
        if (!serializerArg.getValue().empty()) {
            spdlog::debug("Creating serializer: {}", serializerArg.getValue());
            auto serializer_metadata = parsed_args.metadata["serializer"];
            serializer_metadata["type"] = serializerArg.getValue();
            spdlog::debug("Serializer metadata: {}", serializer_metadata.dump());
            serializer = diaspora::Serializer::FromMetadata(diaspora::Metadata{serializer_metadata.dump()});
        }

        // Handle partition selector creation
        if (!partitionSelectorArg.getValue().empty()) {
            spdlog::debug("Creating partition selector: {}", partitionSelectorArg.getValue());
            auto selector_metadata = parsed_args.metadata["partition-selector"];
            selector_metadata["type"] = partitionSelectorArg.getValue();
            spdlog::debug("Partition selector metadata: {}", selector_metadata.dump());
            selector = diaspora::PartitionSelector::FromMetadata(diaspora::Metadata{selector_metadata.dump()});
        }

        driver.createTopic(
            nameArg.getValue(),
            diaspora::Metadata{topic_config.dump()},
            validator,
            selector,
            serializer);

        spdlog::info("Topic '{}' created successfully", nameArg.getValue());
        return 0;

    } catch (TCLAP::ArgException& e) {
        spdlog::error("Argument error: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
        return 1;
    }
}

int topic_list(int argc, char** argv) {
    try {
        // Extract --driver.* metadata arguments before TCLAP parsing
        auto parsed_args = extract_metadata_args(argc, argv);

        // Prepare arguments for TCLAP
        int filtered_argc = parsed_args.filtered_argv.size();
        char** filtered_argv = parsed_args.filtered_argv.data();

        TCLAP::CmdLine cmd("List topics", ' ', "1.0");

        TCLAP::ValueArg<std::string> driverArg("", "driver", "Driver name", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> driverConfigArg("", "driver-config", "Driver config file", false, "", "filename", cmd);
        TCLAP::SwitchArg verboseArg("v", "verbose", "Display topic metadata", cmd, false);

        std::vector<std::string> allowed_levels{"trace", "debug", "info", "warn", "error", "critical", "off"};
        TCLAP::ValuesConstraint<std::string> level_constraint(allowed_levels);
        TCLAP::ValueArg<std::string> loggingArg("", "logging", "Logging level", false, "info", &level_constraint, cmd);

        cmd.parse(filtered_argc, filtered_argv);

        // Set logging level
        spdlog::set_level(spdlog::level::from_str(loggingArg.getValue()));

        // Read configuration file
        std::string driver_config_str = read_config_file(driverConfigArg.getValue());

        // Parse config file as JSON
        nlohmann::json driver_config = nlohmann::json::parse(driver_config_str);

        // Merge command-line metadata with config file metadata
        // Command-line arguments take precedence
        driver_config.merge_patch(parsed_args.metadata["driver"]);

        spdlog::debug("Driver config: {}", driver_config.dump());

        spdlog::debug("Creating driver: {}", driverArg.getValue());
        auto driver = diaspora::Driver::New(driverArg.getValue().c_str(), diaspora::Metadata{driver_config.dump()});

        spdlog::debug("Listing topics");
        auto topics = driver.listTopics();

        // Output topics
        for (const auto& [name, metadata] : topics) {
            if (verboseArg.getValue()) {
                // Verbose mode: display topic name and metadata
                std::cout << name << " " << metadata.json().dump() << std::endl;
            } else {
                // Normal mode: just display topic name
                std::cout << name << std::endl;
            }
        }

        return 0;

    } catch (TCLAP::ArgException& e) {
        spdlog::error("Argument error: {} for arg {}", e.error(), e.argId());
        return 1;
    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
        return 1;
    }
}

} // namespace diaspora_ctl

/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_CTL_COMMON_HPP
#define DIASPORA_CTL_COMMON_HPP

#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

namespace diaspora_ctl {

/**
 * @brief Read a JSON configuration file
 * @param filename Path to the JSON file
 * @return JSON string content, or "{}" if filename is empty; exits on file open error
 */
std::string read_config_file(const std::string& filename);

/**
 * @brief Check if a string represents a number
 * @param s The string to check
 * @return true if the string is a valid number
 */
bool is_number(const std::string& s);

/**
 * @brief Parse a value string into appropriate JSON type
 * @param value The string value to parse
 * @return JSON value (number, string, or array)
 */
nlohmann::json parse_value(const std::string& value);

/**
 * @brief Set a nested value in a JSON object using dot notation
 * @param obj The JSON object to modify
 * @param key The key path (e.g., "a.b.c")
 * @param value The value to set
 */
void set_nested_value(nlohmann::json& obj, const std::string& key, const nlohmann::json& value);

/**
 * @brief Structure to hold parsed command-line metadata arguments
 */
struct ParsedArgs {
    std::unordered_map<std::string, nlohmann::json> metadata;
    std::vector<char*> filtered_argv;
    // Owns the storage for tokens that did not come from the original argv
    // (e.g. tokens parsed out of the DIASPORA_CTL_DRIVER_OPTIONS environment
    // variable). Pointers in filtered_argv may reference into these strings,
    // so this vector must stay alive as long as filtered_argv is used.
    std::vector<std::string> owned_args;
};

/**
 * @brief Extract metadata arguments from argv (--driver.*, --topic.*, --validator.*, --serializer.*, --partition-selector.*)
 *
 * If the DIASPORA_CTL_DRIVER_OPTIONS environment variable is set, its
 * contents are tokenized (shell-like quoting: single/double quotes and
 * backslash escapes, no shell expansion) and prepended to the argument
 * list before processing, so users can avoid retyping driver options on
 * every invocation. CLI arguments take precedence over env-var ones.
 *
 * @param argc Argument count
 * @param argv Argument vector
 * @return Parsed arguments with metadata map and filtered argv
 */
ParsedArgs extract_metadata_args(int argc, char** argv);

} // namespace diaspora_ctl

#endif // DIASPORA_CTL_COMMON_HPP

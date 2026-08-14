/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "common.hpp"
#include <spdlog/spdlog.h>
#include <fstream>
#include <sstream>
#include <cctype>
#include <cstdlib>

namespace diaspora_ctl {

namespace {

// Split a string into tokens with shell-like quoting:
//  - whitespace separates tokens
//  - single quotes preserve content verbatim (no escapes)
//  - double quotes preserve content but allow backslash escape of " and backslash itself
//  - outside quotes, backslash escapes the next character
// No variable expansion, command substitution or globbing is performed.
std::vector<std::string> split_shell_like(const std::string& input) {
    std::vector<std::string> tokens;
    std::string current;
    bool in_token = false;
    enum { NONE, SINGLE, DOUBLE } quote = NONE;

    for (size_t i = 0; i < input.size(); ++i) {
        char c = input[i];
        if (quote == SINGLE) {
            if (c == '\'') quote = NONE;
            else current.push_back(c);
        } else if (quote == DOUBLE) {
            if (c == '"') {
                quote = NONE;
            } else if (c == '\\' && i + 1 < input.size()
                       && (input[i+1] == '"' || input[i+1] == '\\')) {
                current.push_back(input[++i]);
            } else {
                current.push_back(c);
            }
        } else {
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (in_token) {
                    tokens.push_back(std::move(current));
                    current.clear();
                    in_token = false;
                }
            } else if (c == '\'') {
                quote = SINGLE;
                in_token = true;
            } else if (c == '"') {
                quote = DOUBLE;
                in_token = true;
            } else if (c == '\\' && i + 1 < input.size()) {
                current.push_back(input[++i]);
                in_token = true;
            } else {
                current.push_back(c);
                in_token = true;
            }
        }
    }
    if (in_token) tokens.push_back(std::move(current));
    return tokens;
}

} // anonymous namespace

std::string read_config_file(const std::string& filename) {
    if (filename.empty()) {
        return "{}";
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        spdlog::error("Failed to open config file: {}", filename);
        std::exit(1);
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

bool is_number(const std::string& s) {
    if (s.empty()) return false;

    size_t start = 0;
    if (s[0] == '-' || s[0] == '+') start = 1;
    if (start >= s.length()) return false;

    bool has_dot = false;
    for (size_t i = start; i < s.length(); ++i) {
        if (s[i] == '.') {
            if (has_dot) return false; // Multiple dots
            has_dot = true;
        } else if (!std::isdigit(s[i])) {
            return false;
        }
    }
    return true;
}

nlohmann::json parse_value(const std::string& value) {
    // Check if it's a number
    if (is_number(value)) {
        if (value.find('.') != std::string::npos) {
            return std::stod(value);
        } else {
            return std::stoll(value);
        }
    }

    // Check if it's a comma-separated list (array)
    if (value.find(',') != std::string::npos) {
        nlohmann::json arr = nlohmann::json::array();
        std::stringstream ss(value);
        std::string item;
        while (std::getline(ss, item, ',')) {
            arr.push_back(parse_value(item));
        }
        return arr;
    }

    // Otherwise, it's a string
    return value;
}

void set_nested_value(nlohmann::json& obj, const std::string& key, const nlohmann::json& value) {
    // Split key by dots to support nested configuration
    std::vector<std::string> parts;
    std::stringstream ss(key);
    std::string part;
    while (std::getline(ss, part, '.')) {
        parts.push_back(part);
    }

    // Navigate to the nested location
    nlohmann::json* current = &obj;
    for (size_t i = 0; i < parts.size() - 1; ++i) {
        const auto& part_key = parts[i];
        if (!current->contains(part_key) || !(*current)[part_key].is_object()) {
            (*current)[part_key] = nlohmann::json::object();
        }
        current = &(*current)[part_key];
    }

    // Set the final value
    (*current)[parts.back()] = value;
}

ParsedArgs extract_metadata_args(int argc, char** argv) {
    ParsedArgs result;

    // Initialize metadata objects for all supported prefixes
    result.metadata["driver"] = nlohmann::json::object();
    result.metadata["topic"] = nlohmann::json::object();
    result.metadata["validator"] = nlohmann::json::object();
    result.metadata["serializer"] = nlohmann::json::object();
    result.metadata["partition-selector"] = nlohmann::json::object();

    // Define all supported metadata prefixes with their string lengths
    const std::vector<std::pair<std::string, size_t>> prefixes = {
        {"--driver.", 9},
        {"--topic.", 8},
        {"--validator.", 12},
        {"--serializer.", 13},
        {"--partition-selector.", 21}
    };

    // Pull tokens from DIASPORA_CTL_DRIVER_OPTIONS, if any. We move them into
    // result.owned_args so the c_str pointers we hand out remain valid as long
    // as the caller keeps the ParsedArgs alive.
    std::vector<std::string> env_tokens;
    if (const char* env = std::getenv("DIASPORA_CTL_DRIVER_OPTIONS")) {
        env_tokens = split_shell_like(env);
    }
    result.owned_args = std::move(env_tokens);

    // Build a merged token list: argv[0], env tokens, argv[1..]. Env-sourced
    // tokens come first so explicit CLI args can override them (TCLAP picks
    // the last occurrence; set_nested_value also overwrites).
    struct Tok { char* ptr; };
    std::vector<Tok> merged;
    merged.reserve(argc + result.owned_args.size());
    if (argc > 0) merged.push_back({argv[0]});
    for (auto& s : result.owned_args) merged.push_back({s.data()});
    for (int i = 1; i < argc; ++i) merged.push_back({argv[i]});

    const int n = static_cast<int>(merged.size());
    for (int i = 0; i < n; ++i) {
        std::string arg = merged[i].ptr;
        bool matched = false;

        for (const auto& [prefix_with_dashes, prefix_len] : prefixes) {
            if (arg.rfind(prefix_with_dashes, 0) == 0) {
                // Extract the prefix name (without -- and trailing .)
                std::string prefix_name = prefix_with_dashes.substr(2, prefix_len - 3);

                // Extract key after the prefix
                std::string key = arg.substr(prefix_len);

                if (i + 1 < n) {
                    std::string value = merged[i + 1].ptr;
                    set_nested_value(result.metadata[prefix_name], key, parse_value(value));
                    i++; // Skip the value argument
                    matched = true;
                    break;
                }
            }
        }

        if (!matched) {
            result.filtered_argv.push_back(merged[i].ptr);
        }
    }

    return result;
}

} // namespace diaspora_ctl

/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "topic_commands.hpp"
#include "fifo_commands.hpp"
#include "forward_commands.hpp"
#include <spdlog/spdlog.h>
#include <iostream>

void print_usage() {
    std::cout << "Usage: diaspora-ctl <command> <action> [options]\n\n";
    std::cout << "Commands:\n";
    std::cout << "  topic create    Create a new topic\n";
    std::cout << "  topic list      List existing topics\n";
    std::cout << "  fifo            Run FIFO daemon\n";
    std::cout << "  forward         Run forwarding daemon\n";
    std::cout << "\nFor help on a specific command, use:\n";
    std::cout << "  diaspora-ctl topic create --help\n";
    std::cout << "  diaspora-ctl topic list --help\n";
    std::cout << "  diaspora-ctl fifo --help\n";
    std::cout << "  diaspora-ctl forward --help\n";
}

int main(int argc, char** argv) {
    // Configure spdlog
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");

    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::string command = argv[1];

    if (command == "topic") {
        if (argc < 3) {
            std::cerr << "Error: 'topic' command requires an action\n";
            print_usage();
            return 1;
        }

        std::string action = argv[2];

        if (action == "create") {
            // Remove the first two arguments (command and action)
            return diaspora_ctl::topic_create(argc - 2, argv + 2);
        } else if (action == "list") {
            // Remove the first two arguments (command and action)
            return diaspora_ctl::topic_list(argc - 2, argv + 2);
        } else {
            spdlog::error("Unknown topic action: {}", action);
            print_usage();
            return 1;
        }
    } else if (command == "fifo") {
        // Remove the first argument (command)
        return diaspora_ctl::fifo_daemon(argc - 1, argv + 1);
    } else if (command == "forward") {
        // Remove the first argument (command)
        return diaspora_ctl::forward_daemon(argc - 1, argv + 1);
    } else if (command == "--help" || command == "-h") {
        print_usage();
        return 0;
    } else {
        spdlog::error("Unknown command: {}", command);
        print_usage();
        return 1;
    }

    return 0;
}

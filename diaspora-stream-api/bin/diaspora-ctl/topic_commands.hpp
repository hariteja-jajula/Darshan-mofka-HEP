/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_CTL_TOPIC_COMMANDS_HPP
#define DIASPORA_CTL_TOPIC_COMMANDS_HPP

namespace diaspora_ctl {

/**
 * @brief Handle the "topic create" command
 * @param argc Argument count
 * @param argv Argument vector
 * @return 0 on success, non-zero on error
 */
int topic_create(int argc, char** argv);

/**
 * @brief Handle the "topic list" command
 * @param argc Argument count
 * @param argv Argument vector
 * @return 0 on success, non-zero on error
 */
int topic_list(int argc, char** argv);

} // namespace diaspora_ctl

#endif // DIASPORA_CTL_TOPIC_COMMANDS_HPP

/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_CTL_FIFO_COMMANDS_HPP
#define DIASPORA_CTL_FIFO_COMMANDS_HPP

namespace diaspora_ctl {

/**
 * @brief Handle the "fifo" command (run FIFO daemon)
 * @param argc Argument count
 * @param argv Argument vector
 * @return 0 on success, non-zero on error
 */
int fifo_daemon(int argc, char** argv);

} // namespace diaspora_ctl

#endif // DIASPORA_CTL_FIFO_COMMANDS_HPP

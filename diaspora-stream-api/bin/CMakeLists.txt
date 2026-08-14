# Diaspora Control Tool executable (includes FIFO daemon)

# Create the executable from multiple source files
add_executable (diaspora-ctl
    diaspora-ctl/main.cpp
    diaspora-ctl/common.cpp
    diaspora-ctl/topic_commands.cpp
    diaspora-ctl/fifo_commands.cpp
    diaspora-ctl/forward_commands.cpp
)

# Link against the diaspora-stream-api library and dependencies
# Use spdlog header-only mode
target_link_libraries (diaspora-ctl
    diaspora-stream-api
    spdlog::spdlog_header_only
    yaml-cpp::yaml-cpp
    ${CMAKE_DL_LIBS}
)

# Include directories
target_include_directories (diaspora-ctl
    PRIVATE ${TCLAP_INCLUDE_PATH}
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/diaspora-ctl
)

# Apply coverage and warnings configurations
target_link_libraries (diaspora-ctl
    coverage_config
    warnings_config
)

# Install the executable
install (TARGETS diaspora-ctl
         RUNTIME DESTINATION bin)

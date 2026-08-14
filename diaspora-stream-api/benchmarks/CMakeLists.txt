# Add benchmark executables
add_executable (diaspora-producer-benchmark diaspora-producer-benchmark.cpp)
add_executable (diaspora-consumer-benchmark diaspora-consumer-benchmark.cpp)

# Link libraries
target_link_libraries (diaspora-producer-benchmark
    PRIVATE
        diaspora-stream-api
        MPI::MPI_CXX
)

target_link_libraries (diaspora-consumer-benchmark
    PRIVATE
        diaspora-stream-api
        MPI::MPI_CXX
)
target_include_directories (diaspora-producer-benchmark PRIVATE ${TCLAP_INCLUDE_PATH})
target_include_directories (diaspora-consumer-benchmark PRIVATE ${TCLAP_INCLUDE_PATH})

install (TARGETS diaspora-producer-benchmark diaspora-consumer-benchmark
         DESTINATION bin)

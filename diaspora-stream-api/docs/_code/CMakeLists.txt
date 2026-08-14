cmake_minimum_required (VERSION 3.21)
project (diaspora-stream-api-example C CXX)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

if (PROJECT_IS_TOP_LEVEL) # allows building from inside the diaspora-stream-api source tree
    find_package (diaspora-stream-api REQUIRED)
endif ()

add_executable (myproducer producer.cpp)
target_link_libraries (myproducer diaspora-stream-api)

add_executable (myconsumer consumer.cpp)
target_link_libraries (myconsumer diaspora-stream-api)

# CUSTOM TOPIC OBJECTS

add_library (energy_validator SHARED energy_validator.cpp)
target_link_libraries (energy_validator diaspora-stream-api)

add_library (energy_partition_selector SHARED energy_partition_selector.cpp)
target_link_libraries (energy_partition_selector diaspora-stream-api)

add_library (energy_serializer SHARED energy_serializer.cpp)
target_link_libraries (energy_serializer diaspora-stream-api)

add_executable (energy_topic energy_topic.cpp)
target_link_libraries (energy_topic diaspora-stream-api)


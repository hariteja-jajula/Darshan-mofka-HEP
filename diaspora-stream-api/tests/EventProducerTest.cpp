/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

TEST_CASE("Event producer test", "[event-producer]") {

    SECTION("Initialize client/topic/producer") {
        diaspora::Metadata options;
        diaspora::Driver driver = diaspora::Driver::New("simple", options);
        REQUIRE(static_cast<bool>(driver));
        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        diaspora::Metadata partition_config;
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        auto thread_count = GENERATE(as<diaspora::ThreadCount>{}, 0, 1, 2);
        auto batch_size   = GENERATE(diaspora::BatchSize::Adaptive(), 10);
        auto ordering     = GENERATE(diaspora::Ordering::Strict, diaspora::Ordering::Loose);

        auto producer = topic.producer(
            "myproducer", batch_size, thread_count, ordering);
        REQUIRE(static_cast<bool>(producer));

        {
            for(size_t i = 0; i < 100; ++i) {
                diaspora::Future<std::optional<diaspora::EventID>> future;
                auto metadata = diaspora::Metadata{
                    std::string{"{\"event_num\":"} + std::to_string(i) + "}"
                };
                REQUIRE_NOTHROW(future = producer.push(metadata, diaspora::DataView{0, nullptr}));
                if((i+1) % 5 == 0) {
                    if(batch_size != diaspora::BatchSize::Adaptive())
                        REQUIRE_NOTHROW(producer.flush().wait(1000));
                    REQUIRE_NOTHROW(future.wait(1000));
                }
            }
            REQUIRE_NOTHROW(producer.flush().wait(1000));
        }

        std::vector<std::string> data(100);
        {
            for(size_t i = 0; i < 100; ++i) {
                diaspora::Future<std::optional<diaspora::EventID>> future;
                auto metadata = diaspora::Metadata{
                    std::string{"{\"event_num\":"} + std::to_string(i) + "}"
                };
                data[i] = std::string{"This is some data for event "} + std::to_string(i);
                REQUIRE_NOTHROW(future = producer.push(
                            metadata,
                            diaspora::DataView{data[i].data(), data[i].size()}));
                if((i+1) % 5 == 0) {
                    if(batch_size != diaspora::BatchSize::Adaptive())
                        REQUIRE_NOTHROW(producer.flush().wait(1000));
                    REQUIRE_NOTHROW(future.wait(1000));
                }
            }
            REQUIRE_NOTHROW(producer.flush().wait(1000));
        }
    }
}

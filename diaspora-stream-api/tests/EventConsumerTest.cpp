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

static int topic_num = 0;

TEST_CASE("Event consumer test", "[event-consumer]") {

    const char* backend      = std::getenv("DIASPORA_TEST_BACKEND");
    const char* backend_args = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
    const char* topic_args   = std::getenv("DIASPORA_TEST_TOPIC_ARGS");
    backend                  = backend ? backend : "simple";
    backend_args             = backend_args ? backend_args : "{}";
    topic_args               = topic_args ? topic_args : "{}";

    SECTION("Producer/consumer") {
        diaspora::Metadata options{backend_args};
        diaspora::Driver driver = diaspora::Driver::New(backend, options);
        REQUIRE(static_cast<bool>(driver));
        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));

        std::string topic_name = "my_topic_" + std::to_string(topic_num);
        topic_num += 1;

        REQUIRE_NOTHROW(driver.createTopic(topic_name, diaspora::Metadata{topic_args}));
        REQUIRE_NOTHROW(topic = driver.openTopic(topic_name));
        REQUIRE(static_cast<bool>(topic));

        {
            std::vector<std::string> data(100);
            auto producer = topic.producer(driver.defaultThreadPool());
            REQUIRE(static_cast<bool>(producer));
            for(unsigned i=0; i < 100; ++i) {
                diaspora::Metadata metadata = diaspora::Metadata{
                    std::string{"{\"event_num\":"} + std::to_string(i) + "}"
                };
                data[i] = std::string{"This is data for event "} + std::to_string(i);
                diaspora::Future<std::optional<diaspora::EventID>> future;
                REQUIRE_NOTHROW(future = producer.push(
                    metadata,
                    diaspora::DataView{data[i].data(), data[i].size()}));
            }
            REQUIRE_NOTHROW(producer.flush().wait(1000));
        }

        SECTION("Consumer without data")
        {
            diaspora::Consumer consumer;
            REQUIRE_NOTHROW(consumer = topic.consumer("myconsumer", driver.defaultThreadPool()));
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                std::optional<diaspora::Event> event;
                while(!event) // note: we do this because some backend may need time to rebalance
                    REQUIRE_NOTHROW(event = consumer.pull().wait(1000));
                REQUIRE(event.has_value());
                REQUIRE(event->id() == i);
                auto& doc = event->metadata().json();
                REQUIRE(doc["event_num"].get<int64_t>() == i);
                if(i % 5 == 0)
                    REQUIRE_NOTHROW(event->acknowledge());
            }
            // Consume extra events, we should get events with NoMoreEvents as event IDs
            for(unsigned i=0; i < 10; ++i) {
                std::optional<diaspora::Event> event;
                REQUIRE_NOTHROW(event = consumer.pull().wait(100));
                REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
            }
        }

        SECTION("Consume with data")
        {
            diaspora::DataSelector data_selector =
                    [](const diaspora::Metadata& metadata, const diaspora::DataDescriptor& descriptor) {
                auto& doc = metadata.json();
                auto event_id = doc["event_num"].get<int64_t>();
                if(event_id % 2 == 0) {
                    return descriptor;
                } else {
                    return diaspora::DataDescriptor();
                }
            };
            diaspora::DataAllocator data_allocator =
                    [](const diaspora::Metadata& metadata, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto& doc = metadata.json();
                auto event_id = doc["event_num"].get<int64_t>();
                if(event_id % 2 == 0) {
                    auto data = new char[size];
                    return diaspora::DataView{data, size};
                } else {
                    return diaspora::DataView{};
                }
                return diaspora::DataView{};
            };
            auto consumer = topic.consumer(
                "myconsumer", data_selector, data_allocator);
            REQUIRE(static_cast<bool>(consumer));
            for(unsigned i=0; i < 100; ++i) {
                std::optional<diaspora::Event> event;
                while(!event) // note: we do this because some backend may need time to rebalance
                    REQUIRE_NOTHROW(event = consumer.pull().wait(1000));
                REQUIRE(event.has_value());
                REQUIRE(event->id() == i);
                auto& doc = event->metadata().json();
                REQUIRE(doc["event_num"].get<int64_t>() == i);
                if(i % 5 == 0)
                    REQUIRE_NOTHROW(event->acknowledge());
                if(i % 2 == 0) {
                    REQUIRE(event->data().segments().size() == 1);
                    auto data_str = std::string{
                        (const char*)event->data().segments()[0].ptr,
                        event->data().segments()[0].size};
                    std::string expected = std::string("This is data for event ") + std::to_string(i);
                    REQUIRE(data_str == expected);
                    delete[] static_cast<const char*>(event->data().segments()[0].ptr);
                } else {
                    REQUIRE(event->data().segments().size() == 0);
                }
            }
            auto event = consumer.pull().wait(1000);
            REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
        }
    }
}

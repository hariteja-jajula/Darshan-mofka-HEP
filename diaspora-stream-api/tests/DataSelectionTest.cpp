/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cstdlib>
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

    diaspora::Metadata options{backend_args};
    diaspora::Driver driver = diaspora::Driver::New(backend, options);
    REQUIRE(static_cast<bool>(driver));

    std::string topic_name = "my_topic_" + std::to_string(topic_num);
    topic_num += 1;

    driver.createTopic(topic_name, diaspora::Metadata{topic_args});
    auto topic = driver.openTopic(topic_name);

    std::string seg1 = "abcdefghijklmnopqrstuvwxyz";
    std::string seg2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Producer
    {
        auto producer = topic.producer();
        REQUIRE(static_cast<bool>(producer));
        auto metadata = diaspora::Metadata{};
        auto data = diaspora::DataView{{{seg1.data(), seg1.size()},{seg2.data(), seg2.size()}}};
        producer.push(metadata, data);
        producer.flush().wait(1000);
    }

    SECTION("Consume no data") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor&) {
                return diaspora::DataDescriptor();
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor&) {
                return diaspora::DataView{};
            };
        auto consumer = topic.consumer(
                "myconsumer_0", data_selector, data_allocator);
        std::optional<diaspora::Event> event;
        while(!event) event = consumer.pull().wait(1000);
        REQUIRE(event.has_value());
        REQUIRE(event->data().size() == 0);

        event = consumer.pull().wait(1000);
        REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
    }

    SECTION("Consume the whole data") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor;
            };
        diaspora::DataAllocator data_broker =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer_1", data_selector, data_broker);
        std::optional<diaspora::Event> event;
        while(!event) event = consumer.pull().wait(1000);
        REQUIRE(event.has_value());
        REQUIRE(event->data().size() == 52);
        REQUIRE(event->data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event->data().segments()[0].ptr,
            event->data().segments()[0].size};
        REQUIRE(received == seg1+seg2);
        delete[] (char*)event->data().segments()[0].ptr;

        event = consumer.pull().wait(1000);
        REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
    }

    SECTION("Consume using makeSubView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeSubView(13, 26);
            };
        diaspora::DataAllocator data_broker =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer_2", data_selector, data_broker);
        std::optional<diaspora::Event> event;
        while(!event) event = consumer.pull().wait(1000);
        REQUIRE(event.has_value());
        REQUIRE(event->data().size() == 26);
        REQUIRE(event->data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event->data().segments()[0].ptr,
            event->data().segments()[0].size};
        REQUIRE(received == "nopqrstuvwxyzABCDEFGHIJKLM");
        delete[] (char*)event->data().segments()[0].ptr;

        event = consumer.pull().wait(1000);
        REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
    }

    SECTION("Consume using makeStridedView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeStridedView(13, 3, 4, 2);
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer_3", data_selector, data_allocator);
        std::optional<diaspora::Event> event;
        while(!event) event = consumer.pull().wait(1000);
        REQUIRE(event.has_value());
        REQUIRE(event->data().size() == 12);
        REQUIRE(event->data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event->data().segments()[0].ptr,
            event->data().segments()[0].size};
        REQUIRE(received == "nopqtuvwzABC");
        delete[] (char*)event->data().segments()[0].ptr;

        event = consumer.pull().wait(1000);
        REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
    }

    SECTION("Consume using makeUnstructuredView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeUnstructuredView({
                        {3, 6},
                        {15, 4},
                        {27, 8}
                });
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer_4", data_selector, data_allocator);
        std::optional<diaspora::Event> event;
        while(!event) event = consumer.pull().wait(1000);
        REQUIRE(event.has_value());
        REQUIRE(event->data().size() == 18);
        REQUIRE(event->data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event->data().segments()[0].ptr,
            event->data().segments()[0].size};
        REQUIRE(received == "defghipqrsBCDEFGHI");
        delete[] (char*)event->data().segments()[0].ptr;

        event = consumer.pull().wait(1000);
        REQUIRE((!event || event->id() == diaspora::NoMoreEvents));
    }
}

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

TEST_CASE("Producer test", "[producer]") {

    const char* backend      = std::getenv("DIASPORA_TEST_BACKEND");
    const char* backend_args = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
    const char* topic_args   = std::getenv("DIASPORA_TEST_TOPIC_ARGS");
    backend                  = backend ? backend : "simple";
    backend_args             = backend_args ? backend_args : "{}";
    topic_args               = topic_args ? topic_args : "{}";

    SECTION("Initialize a Driver and create/open a topic") {

        diaspora::Metadata options{backend_args};
        diaspora::Driver driver = diaspora::Driver::New(backend, options);
        REQUIRE(static_cast<bool>(driver));

        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic", diaspora::Metadata{topic_args}));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a producer from the topic") {
            diaspora::Producer producer;
            REQUIRE(!static_cast<bool>(producer));
            producer = topic.producer("myproducer");
            REQUIRE(static_cast<bool>(producer));
            REQUIRE(producer.name() == "myproducer");
            REQUIRE(static_cast<bool>(producer.topic()));
            REQUIRE(producer.topic().name() == "mytopic");
        }
    }
}

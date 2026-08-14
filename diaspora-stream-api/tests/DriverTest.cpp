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

TEST_CASE("Driver test", "[driver]") {

    const char* backend      = std::getenv("DIASPORA_TEST_BACKEND");
    const char* backend_args = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
    const char* topic_args   = std::getenv("DIASPORA_TEST_TOPIC_ARGS");
    backend                  = backend ? backend : "simple";
    backend_args             = backend_args ? backend_args : "{}";
    topic_args               = topic_args ? topic_args : "{}";

    SECTION("Initialize a service handle") {
        diaspora::Driver driver;
        diaspora::Metadata options{backend_args};
        driver = diaspora::Driver::New(backend, options);
        REQUIRE(static_cast<bool>(driver));

        SECTION("Create a topic") {
            diaspora::TopicHandle topic;
            REQUIRE(!static_cast<bool>(topic));
            REQUIRE(!driver.topicExists("mytopic"));
            REQUIRE_NOTHROW(driver.createTopic("mytopic", diaspora::Metadata{topic_args}));
            REQUIRE(driver.topicExists("mytopic"));
            REQUIRE_THROWS_AS(driver.createTopic("mytopic", diaspora::Metadata{topic_args}),
                              diaspora::Exception);

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));
            REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), diaspora::Exception);

            REQUIRE(topic.partitions().size() == 1);
        }

        SECTION("List topics") {
            // Create multiple topics
            REQUIRE_NOTHROW(driver.createTopic("topic1", diaspora::Metadata{topic_args}));
            REQUIRE_NOTHROW(driver.createTopic("topic2", diaspora::Metadata{topic_args}));

            // Get list of topics
            auto topics = driver.listTopics();

            // Verify we have the expected topics
            REQUIRE(topics.size() >= 2);
            REQUIRE(topics.find("topic1") != topics.end());
            REQUIRE(topics.find("topic2") != topics.end());

            // Check metadata for topic1
            auto& topic1_metadata = topics["topic1"];
            auto& topic1_json = topic1_metadata.json();

            if (topic1_json.contains("validator")) {
                REQUIRE(topic1_json["validator"].is_object());
            }

            if (topic1_json.contains("serializer")) {
                REQUIRE(topic1_json["serializer"].is_object());
            }

            if (topic1_json.contains("partition_selector")) {
                REQUIRE(topic1_json["partition_selector"].is_object());
            }

            // Check metadata for topic2
            auto& topic2_metadata = topics["topic2"];
            auto& topic2_json = topic2_metadata.json();

            if (topic2_json.contains("validator")) {
                REQUIRE(topic2_json["validator"].is_object());
            }

            if (topic2_json.contains("serializer")) {
                REQUIRE(topic2_json["serializer"].is_object());
            }

            if (topic2_json.contains("partition_selector")) {
                REQUIRE(topic2_json["partition_selector"].is_object());
            }

        }
    }
}

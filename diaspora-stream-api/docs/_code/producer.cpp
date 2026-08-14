#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <root_path> <topic>" << std::endl;
        return -1;
    }

    auto root_path = argv[1];
    auto topic_name = argv[2];

    try {

        diaspora::Metadata driver_options;
        driver_options.json()["root_path"] = root_path;

        diaspora::Driver driver = diaspora::Driver::New("files", driver_options);

        diaspora::TopicHandle topic = driver.openTopic(topic_name);

        diaspora::Producer producer = topic.producer();
        for(size_t i = 0; i < 100; ++i) {
            auto future = producer.push(
                diaspora::Metadata{R"({"x":42,"name":"bob"})"},
                diaspora::DataView{}
            );
            // auto event_id = future.wait(-1).value();
        }
        producer.flush().wait(-1);

    } catch(const diaspora::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return 0;
}

#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <root_path> <topic_name>" << std::endl;
        return -1;
    }

    auto root_path = argv[1];
    auto topic_name = argv[2];

    try {

        diaspora::Metadata options;
        options.json()["root_path"] = root_path;

        diaspora::Driver driver = diaspora::Driver::New("files", options);

        diaspora::TopicHandle topic = driver.openTopic(topic_name);

        diaspora::Consumer consumer = topic.consumer("my_consumer");
        for(size_t i = 0; i < 100; ++i) {
            diaspora::Event event = consumer.pull().wait(-1).value();
            std::cout << event.id() << ": " << event.metadata().json().dump() << std::endl;
            if((i+1) % 10 == 0) event.acknowledge();
        }

    } catch(const diaspora::Exception& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return 0;
}

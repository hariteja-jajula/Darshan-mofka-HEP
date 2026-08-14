#include <diaspora/Driver.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/TopicHandle.hpp>
#include <iostream>

int main(int argc, char** argv) {

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <root_path>" << std::endl;
        return -1;
    }

    auto root_path = argv[1];

    try {

        diaspora::Metadata options;
        options.json()["root_path"] = root_path;

        diaspora::Driver driver = diaspora::Driver::New("files", options);

        // START CREATE TOPIC
        diaspora::Validator validator =
            diaspora::Validator::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_validator:libenergy_validator.so"},
                    {"energy_max", 100}}}
            );

        diaspora::PartitionSelector selector =
            diaspora::PartitionSelector::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_partition_selector:libenergy_partition_selector.so"},
                    {"energy_max", 100}}}
            );

        diaspora::Serializer serializer =
            diaspora::Serializer::FromMetadata(
                diaspora::Metadata{{
                    {"type","energy_serializer:libenergy_serializer.so"},
                    {"energy_max", 100}}}
            );

        driver.createTopic("collisions", diaspora::Metadata{}, validator, selector, serializer);
        // END CREATE TOPIC
        {
        // START PRODUCER
        diaspora::TopicHandle topic = driver.openTopic("collisions");

        diaspora::ThreadPool thread_pool = driver.makeThreadPool(diaspora::ThreadCount{4});
        diaspora::BatchSize  batch_size = diaspora::BatchSize::Adaptive();
        diaspora::MaxNumBatches max_num_batches = diaspora::MaxNumBatches{2};
        diaspora::Ordering   ordering = diaspora::Ordering::Loose; // or Strict

        diaspora::Producer producer = topic.producer(
                "app1", thread_pool, batch_size, max_num_batches, ordering);
        // END PRODUCER

        // START EVENT
        std::vector<char> segment1 = { 'a', 'b', 'c', 'd' };

        // expose 1 segment using its pointer and size
        diaspora::DataView data1{segment1.data(), segment1.size()};

        std::vector<char> segment2 = { 'e', 'f' };

        // expose 2 non-contiguous segments using diaspora::Data::Segment
        diaspora::DataView data2{{
            diaspora::DataView::Segment{segment1.data(), segment1.size()},
            diaspora::DataView::Segment{segment2.data(), segment2.size()}
        }};
        diaspora::Metadata metadata1{R"({"energy": 42})"};

        using json = nlohmann::json;
        auto md = json::object();
        md["energy"] = 42;
        diaspora::Metadata metadata2{md};
        // END EVENT

        // START PRODUCE EVENT
        diaspora::Future<std::optional<diaspora::EventID>> future = producer.push(metadata1, data1);
        future.completed(); // returns true if the future has completed

        producer.push(metadata2, data2);

        // the value passed to wait is a timeout in milliseconds,
        // -1 indicates that the wait call should block until the value is available
        diaspora::EventID event_id_1 = future.wait(-1).value();

        producer.flush();
        // END PRODUCE EVENT
        }

        {
        // START CONSUMER
        diaspora::TopicHandle topic = driver.openTopic("collisions");
        diaspora::BatchSize  batch_size = diaspora::BatchSize::Adaptive();
        diaspora::ThreadPool thread_pool = driver.defaultThreadPool();
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata& md, const diaspora::DataDescriptor& dd) -> diaspora::DataDescriptor {
                if(md.json()["energy"] > 20) {
                    return dd;
                } else {
                    return diaspora::DataDescriptor{};
                }
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata& md, const diaspora::DataDescriptor& dd) -> diaspora::DataView {
                (void)md;
                char* ptr = new char[dd.size()];
                return diaspora::DataView{ptr, dd.size()};
            };
        diaspora::Consumer consumer = topic.consumer(
            "app2", thread_pool, batch_size, data_selector, data_allocator);
        // END CONSUMER
        // START CONSUME EVENTS
        diaspora::Future<std::optional<diaspora::Event>> future = consumer.pull();
        future.completed(); // returns true if the future has completed

        diaspora::Event event        = future.wait(-1).value();
        diaspora::DataView data      = event.data();
        diaspora::Metadata metadata  = event.metadata();
        diaspora::EventID event_id   = event.id();

        event.acknowledge();

        delete[] static_cast<char*>(data.segments()[0].ptr);
        // END CONSUME EVENTS
        }

    } catch(const diaspora::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}

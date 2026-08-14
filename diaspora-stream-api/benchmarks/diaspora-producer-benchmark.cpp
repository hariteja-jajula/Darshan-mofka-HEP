#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <fstream>
#include <stdexcept>
#include <random>
#include <algorithm>
#include <tclap/CmdLine.h>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/DataDescriptor.hpp>
#include <diaspora/Exception.hpp>
#include <mpi.h>
#include "../tests/SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

struct Options
{
    std::string driver;
    std::string configFile;
    std::string topic;
    size_t numEvents;
    size_t dataSize;
    size_t metadataSize;
    size_t batchSize;
    size_t numThreads;
    size_t flushInterval;
};

void parse_options(int argc, char** argv, Options& options);
void print_options(const Options& options);
void run_benchmark(const Options& options);
std::string random_string(size_t length);
void init_simple_backend(const Options& options, diaspora::Driver driver);

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int ret = 0;
    try
    {
        Options options;
        parse_options(argc, argv, options);
        int rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        if (rank == 0) {
            print_options(options);
        }
        run_benchmark(options);
    }
    catch (const TCLAP::ArgException &e)
    {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        ret = -1;
    }
    catch (const diaspora::Exception& e)
    {
        std::cerr << "diaspora error: " << e.what() << std::endl;
        ret = -1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        ret = -1;
    }
    MPI_Finalize();
    return ret;
}

std::string random_string(size_t length) {
    const std::string CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);
    std::string random_string = "\"";
    for (std::size_t i = 0; i < length - 2; ++i) {
        random_string += CHARACTERS[distribution(generator)];
    }
    random_string += "\"";
    return random_string;
}

void run_benchmark(const Options& options)
{
    // Read driver configuration from file if provided
    diaspora::Metadata driver_opts;
    if (!options.configFile.empty()) {
        std::ifstream configFileStream(options.configFile);
        if (!configFileStream) {
            throw std::runtime_error("Could not open config file: " + options.configFile);
        }
        std::string configContent((std::istreambuf_iterator<char>(configFileStream)),
                                   std::istreambuf_iterator<char>());
        driver_opts = diaspora::Metadata{configContent};
    }

    // Prepare data
    std::vector<diaspora::Metadata> metadata(options.numEvents);
    for(size_t i = 0; i < options.numEvents; ++i) {
        metadata[i] = diaspora::Metadata{random_string(options.metadataSize)};
    }

    std::vector<char> data(options.dataSize*options.numEvents);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dis(0, std::numeric_limits<size_t>::max());
    size_t offset = 0;
    while(offset < data.size()) {
        size_t r = dis(gen);
        std::memcpy(data.data() + offset, &r, std::min<size_t>(sizeof(r), data.size() - offset));
        offset += sizeof(r);
    }

    // Initialize Driver
    auto driver = diaspora::Driver::New(options.driver.c_str(), driver_opts);

    // If we are using the simple backend, initialize it
    init_simple_backend(options, driver);

    // Create ThreadPool
    auto threadPool = driver.makeThreadPool(diaspora::ThreadCount{options.numThreads});

    // Open Topic
    auto topic = driver.openTopic(options.topic);

    // Create Producer
    auto producer = topic.producer("benchmark-producer",
                                   threadPool,
                                   diaspora::BatchSize{options.batchSize});

    // Run benchmark
    MPI_Barrier(MPI_COMM_WORLD);
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < options.numEvents; ++i) {
        auto& md = metadata[i];
        auto d = diaspora::DataView{data.data() + i*options.dataSize};
        producer.push(
            metadata[i],
            diaspora::DataView{data.data() + i*options.dataSize, options.dataSize}
        );
        if ((i + 1) % options.flushInterval == 0) {
            producer.flush().wait(-1);
        }
    }
    producer.flush().wait(-1); // Final flush to send any remaining events

    MPI_Barrier(MPI_COMM_WORLD);
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate and print results
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        std::chrono::duration<double> elapsed = end_time - start_time;
        double total_time_sec = elapsed.count();
        double events_per_sec = options.numEvents / total_time_sec;
        double total_data_mb = static_cast<double>((options.dataSize + options.metadataSize) * options.numEvents) / (1024.0 * 1024.0);
        double mb_per_sec = total_data_mb / total_time_sec;

        std::cout << "\n--- Benchmark Results ---" << std::endl;
        std::cout << "Total time: " << total_time_sec << " s" << std::endl;
        std::cout << "Total events: " << options.numEvents << std::endl;
        std::cout << "Throughput: " << events_per_sec << " events/s" << std::endl;
        std::cout << "Bandwidth: " << mb_per_sec << " MB/s" << std::endl;
    }
}

void parse_options(int argc, char** argv, Options& options)
{
    TCLAP::CmdLine cmd("Diaspora Producer Benchmark", ' ', "0.1");

    TCLAP::ValueArg<std::string> driverArg(
        "d", "driver", "Driver to use", true, "kafka", "string");
    TCLAP::ValueArg<std::string> configFileArg(
        "c", "driver-config", "Configuration file for the driver", false, "", "string");
    TCLAP::ValueArg<std::string> topicArg(
        "t", "topic", "Topic to produce to", true, "test-topic", "string");
    TCLAP::ValueArg<size_t> numEventsArg(
        "n", "num-events", "Number of events to send", false, 1000, "int");
    TCLAP::ValueArg<size_t> dataSizeArg(
        "s", "data-size", "Size of the data in each event in bytes", false, 0, "int");
    TCLAP::ValueArg<size_t> metadataSizeArg(
        "m", "metadata-size", "Size of the metadata part of each event in bytes", false, 0, "int");
    TCLAP::ValueArg<size_t> batchSizeArg(
        "b", "batch-size", "Number of events to batch together", false, 1, "int");
    TCLAP::ValueArg<size_t> numThreadsArg(
        "p", "num-threads", "Number of background threads for the producer to use", false, 1, "int");
    TCLAP::ValueArg<size_t> flushIntervalArg(
        "f", "flush-interval", "Number of events to push before calling flush", false, 1000, "int");

    cmd.add(driverArg);
    cmd.add(configFileArg);
    cmd.add(topicArg);
    cmd.add(numEventsArg);
    cmd.add(dataSizeArg);
    cmd.add(metadataSizeArg);
    cmd.add(batchSizeArg);
    cmd.add(numThreadsArg);
    cmd.add(flushIntervalArg);

    cmd.parse(argc, argv);

    options.driver = driverArg.getValue();
    options.configFile = configFileArg.getValue();
    options.topic = topicArg.getValue();
    options.numEvents = numEventsArg.getValue();
    options.dataSize = dataSizeArg.getValue();
    options.metadataSize = metadataSizeArg.getValue();
    options.batchSize = batchSizeArg.getValue();
    options.numThreads = numThreadsArg.getValue();
    options.flushInterval = flushIntervalArg.getValue();
}

void print_options(const Options& options)
{
    std::cout << "--- Benchmark Options ---" << std::endl;
    std::cout << "Driver: " << options.driver << std::endl;
    std::cout << "Driver config file: " << options.configFile << std::endl;
    std::cout << "Topic: " << options.topic << std::endl;
    std::cout << "Number of events: " << options.numEvents << std::endl;
    std::cout << "Data size: " << options.dataSize << std::endl;
    std::cout << "Metadata size: " << options.metadataSize << std::endl;
    std::cout << "Batch size: " << options.batchSize << std::endl;
    std::cout << "Number of threads: " << options.numThreads << std::endl;
    std::cout << "Flush interval: " << options.flushInterval << std::endl;
}

void init_simple_backend(const Options& options, diaspora::Driver driver)
{
    if(options.driver != "simple") return;
    driver.createTopic(options.topic);
}

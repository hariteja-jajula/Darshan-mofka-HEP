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
#include <diaspora/Consumer.hpp>
#include <diaspora/Exception.hpp>
#include <mpi.h>
#include "RandomBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, RandomDriver);

struct Options
{
    std::string driver;
    std::string configFile;
    std::string topic;
    size_t numEvents;
    size_t numThreads;
    double dataSelectivity;
    double dataInterest;
};

void parse_options(int argc, char** argv, Options& options);
void print_options(const Options& options);
void run_benchmark(const Options& options);
void init_simple_backend(const Options& options, diaspora::Driver driver);

struct Selector {

    double             dataInterest;
    double             dataSelectivity;
    std::random_device randomDevice;
    std::mt19937       rng;

    Selector(double interest, double selectivity)
    : dataInterest(interest)
    , dataSelectivity(selectivity)
    , rng(randomDevice())
    {}

    diaspora::DataDescriptor operator()(const diaspora::Metadata& metadata,
                                        const diaspora::DataDescriptor& descriptor) {
        (void)metadata;
        auto interested = std::binomial_distribution{1, dataInterest}(rng);
        if(!interested) return diaspora::DataDescriptor{};
        return descriptor.makeSubView(0, descriptor.size()*dataSelectivity);
    }

};

struct Allocator {

    diaspora::DataView operator()(const diaspora::Metadata& metadata,
                                  const diaspora::DataDescriptor& descriptor) {
        if(descriptor.size()) {
            auto free_cb = [](void* data) {
                delete[] static_cast<char*>(data);
            };
            auto data = new char[descriptor.size()];
            auto view = diaspora::DataView{data, descriptor.size(), data, free_cb};
            return view;
        } else {
            return diaspora::DataView{};
        }
    }

};

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

    // Initialize Driver
    auto driver = diaspora::Driver::New(options.driver.c_str(), driver_opts);

    // If we are using the simple backend, initialize it
    init_simple_backend(options, driver);

    // Create ThreadPool
    auto threadPool = driver.makeThreadPool(diaspora::ThreadCount{options.numThreads});

    // Open Topic
    auto topic = driver.openTopic(options.topic);

    // Get number of partitions
    auto numPartitions = topic.partitions().size();
    int rank, nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    if(rank == 0) {
        if(numPartitions % nprocs != 0)
            std::cerr << "WARNING: number of processes does not evenly divide number of partitions" << std::endl;
        if(numPartitions < nprocs)
            std::cerr << "WARNING: number of processes is lower than number of partitions" << std::endl;
    }

    std::vector<size_t> partitions;
    for(size_t i = rank; i < numPartitions; i += nprocs) {
        partitions.push_back(i);
    }

    // Run benchmark
    MPI_Barrier(MPI_COMM_WORLD);

    // Create Consumer
    // Note: the consumer is created after the barrier because it may start pulling
    // events in the background while the barrier is happening otherwise.
    auto consumer = topic.consumer("benchmark-consumer", threadPool, partitions);

    auto start_time = std::chrono::high_resolution_clock::now();

    size_t events_received = 0;
    while(events_received < options.numEvents) {
        auto event = consumer.pull().wait(5000);
        if(event) events_received += 1;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    auto end_time = std::chrono::high_resolution_clock::now();

    if (rank == 0) {
        std::chrono::duration<double> elapsed = end_time - start_time;
        double total_time_sec = elapsed.count();
        double events_per_sec = options.numEvents / total_time_sec;

        std::cout << "\n--- Benchmark Results ---" << std::endl;
        std::cout << "Total time: " << total_time_sec << " s" << std::endl;
        std::cout << "Total events: " << options.numEvents << " s" << std::endl;
        std::cout << "Throughput: " << events_per_sec << " events/s" << std::endl;
    }
}

void parse_options(int argc, char** argv, Options& options)
{
    TCLAP::CmdLine cmd("Diaspora Consumer Benchmark", ' ', "0.1");

    TCLAP::ValueArg<std::string> driverArg(
        "d", "driver", "Driver to use", true, "kafka", "string");
    TCLAP::ValueArg<std::string> configFileArg(
        "c", "driver-config", "Configuration file for the driver", false, "", "string");
    TCLAP::ValueArg<std::string> topicArg(
        "t", "topic", "Topic to consume from", true, "test-topic", "string");
    TCLAP::ValueArg<size_t> numEventsArg(
        "n", "num-events", "Number of events to receive", false, 1000, "int");
    TCLAP::ValueArg<size_t> numThreadsArg(
        "p", "num-threads", "Number of background threads for the consumer to use", false, 1, "int");
    TCLAP::ValueArg<double> dataSelectivityArg(
        "s", "data-selectivity", "Proportion of data to read (between 0 and 1)", false, 1, "float");
    TCLAP::ValueArg<double> dataInterestArg(
        "i", "data-interest", "Proportion of events for which to pull data (between 0 and 1)", false, 1, "float");

    cmd.add(driverArg);
    cmd.add(configFileArg);
    cmd.add(topicArg);
    cmd.add(numEventsArg);
    cmd.add(numThreadsArg);
    cmd.add(dataSelectivityArg);
    cmd.add(dataInterestArg);

    cmd.parse(argc, argv);

    options.driver = driverArg.getValue();
    options.configFile = configFileArg.getValue();
    options.topic = topicArg.getValue();
    options.numEvents = numEventsArg.getValue();
    options.numThreads = numThreadsArg.getValue();
    options.dataInterest = dataInterestArg.getValue();
    options.dataSelectivity = dataSelectivityArg.getValue();

    if(options.dataInterest < 0 || options.dataInterest > 1) {
        throw TCLAP::ArgException{
            "Invalid value (should be between 0 and 1)", "data-interest"};
    }
    if(options.dataSelectivity < 0 || options.dataSelectivity > 1) {
        throw TCLAP::ArgException{
            "Invalid value (should be between 0 and 1)", "data-selectivity"};
    }
}

void print_options(const Options& options)
{
    std::cout << "--- Benchmark Options ---" << std::endl;
    std::cout << "Driver: " << options.driver << std::endl;
    std::cout << "Driver config file: " << options.configFile << std::endl;
    std::cout << "Topic: " << options.topic << std::endl;
    std::cout << "Number of events: " << options.numEvents << std::endl;
    std::cout << "Number of threads: " << options.numThreads << std::endl;
    std::cout << "Data interest: " << options.dataInterest << std::endl;
    std::cout << "Data selectivity: " << options.dataSelectivity << std::endl;
}

void init_simple_backend(const Options& options, diaspora::Driver driver)
{
    if(options.driver != "simple") return;
    driver.createTopic(options.topic);
}

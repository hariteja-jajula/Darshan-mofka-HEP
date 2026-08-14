#include <diaspora/Consumer.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/ThreadPool.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/BufferWrapperArchive.hpp>

#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <variant>
#include <iostream>

#include "../tests/SimpleBackend.hpp"


class RandomDriver;
class RandomProducer;
class RandomConsumer;
class RandomTopicHandle;

using RandomThreadPool = SimpleThreadPool;
using RandomEvent = SimpleEvent;

class RandomConsumer final : public diaspora::ConsumerInterface {

    const std::string                        m_name;
    const diaspora::BatchSize                m_batch_size;
    const diaspora::MaxNumBatches            m_max_num_batches;
    const std::shared_ptr<RandomThreadPool>  m_thread_pool;
    const std::shared_ptr<RandomTopicHandle> m_topic;
    const diaspora::DataAllocator            m_data_allocator;
    const diaspora::DataSelector             m_data_selector;
    std::atomic<size_t>                      m_next_id = 0;

    public:

    RandomConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<RandomThreadPool> thread_pool,
        std::shared_ptr<RandomTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic))
    , m_data_allocator{std::move(data_allocator)}
    , m_data_selector{std::move(data_selector)}
    {}

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    const diaspora::DataAllocator& dataAllocator() const override {
        return m_data_allocator;
    }

    const diaspora::DataSelector& dataSelector() const override {
        return m_data_selector;
    }

    void process(diaspora::EventProcessor processor,
                 int timeout_ms,
                 diaspora::NumEvents maxEvents,
                 std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) override;

    void unsubscribe() override;

    diaspora::Future<std::optional<diaspora::Event>> pull() override;

};


class RandomTopicHandle final : public diaspora::TopicHandleInterface,
                                public std::enable_shared_from_this<RandomTopicHandle> {

    friend class RandomProducer;
    friend class RandomConsumer;

    const std::string                          m_name;
    const std::vector<diaspora::PartitionInfo> m_pinfo = {diaspora::PartitionInfo("{}")};
    const std::shared_ptr<RandomDriver>        m_driver;

    public:

    RandomTopicHandle(
        std::string name,
        std::shared_ptr<RandomDriver> driver)
    : m_name{std::move(name)}
    , m_driver{std::move(driver)}
    {}

    const std::string& name() const override {
        return m_name;
    }

    std::shared_ptr<diaspora::DriverInterface> driver() const override;

    const std::vector<diaspora::PartitionInfo>& partitions() const override {
        return m_pinfo;
    }

    diaspora::Validator validator() const override {
        return diaspora::Validator{};
    }

    diaspora::PartitionSelector selector() const override {
        return diaspora::PartitionSelector{};
    }

    diaspora::Serializer serializer() const override {
        return diaspora::Serializer{};
    }

    std::shared_ptr<diaspora::ProducerInterface>
        makeProducer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     diaspora::Ordering ordering,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::Metadata options) override {
        throw diaspora::Exception{"Not implemented for this backend"};
    }

    std::shared_ptr<diaspora::ConsumerInterface>
        makeConsumer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::DataAllocator data_allocator,
                     diaspora::DataSelector data_selector,
                     const std::vector<size_t>& targets,
                     diaspora::Metadata options) override;
};

inline std::shared_ptr<diaspora::TopicHandleInterface> RandomConsumer::topic() const {
    return m_topic;
}

class RandomDriver : public diaspora::DriverInterface,
                     public std::enable_shared_from_this<RandomDriver> {

    std::shared_ptr<diaspora::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<RandomThreadPool>(diaspora::ThreadCount{0});
    std::unordered_map<std::string, std::shared_ptr<RandomTopicHandle>> m_topics;

    public:

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override {
        (void)options;
        (void)validator;
        (void)selector;
        (void)serializer;
        if(m_topics.count(std::string{name})) throw diaspora::Exception{"Topic already exists"};
        std::vector<diaspora::PartitionInfo> pinfo{diaspora::PartitionInfo{"{}"}};
        m_topics.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::string{name}),
            std::forward_as_tuple(
                std::make_shared<RandomTopicHandle>(
                    std::string{name},
                    shared_from_this()
                )
            )
        );
    }

    std::shared_ptr<diaspora::TopicHandleInterface> openTopic(std::string_view name) const override {
        auto it = m_topics.find(std::string{name});
        if(it == m_topics.end())
            throw diaspora::Exception{"Could not find topic \"" + std::string{name} + "\""};
        return it->second;
    }

    bool topicExists(std::string_view name) const override {
        return m_topics.count(std::string{name});
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override {
        return m_default_thread_pool;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override {
        return std::make_shared<RandomThreadPool>(count);
    }

    std::unordered_map<std::string, diaspora::Metadata> listTopics() const override {
        std::unordered_map<std::string, diaspora::Metadata> result;
        for(auto& p : m_topics) {
            result[p.first] = diaspora::Metadata{}; // TODO
        }
        return result;
    }

    static inline std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata&) {
        return std::make_shared<RandomDriver>();
    }
};


inline std::shared_ptr<diaspora::DriverInterface> RandomTopicHandle::driver() const {
    return m_driver;
}

inline std::shared_ptr<diaspora::ConsumerInterface>
    RandomTopicHandle::makeConsumer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        const std::vector<size_t>& targets,
        diaspora::Metadata options) {
    (void)options;
    (void)targets;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<RandomThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of RandomThreadPool"};
    return std::make_shared<RandomConsumer>(
            std::string{name}, batch_size, max_batch, simple_thread_pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector));
}

void RandomConsumer::unsubscribe() {}

diaspora::Future<std::optional<diaspora::Event>> RandomConsumer::pull() {
    auto event = std::make_shared<RandomEvent>(
        diaspora::Metadata{},
        diaspora::DataView{},
        diaspora::PartitionInfo{},
        m_next_id++);
    return diaspora::Future<std::optional<diaspora::Event>>{
        [event=std::move(event)](int) -> diaspora::Event {
            return diaspora::Event{event};
        },
        []() {
            return true;
        }};
}

inline void RandomConsumer::process(
        diaspora::EventProcessor processor,
        int timeout_ms,
        diaspora::NumEvents maxEvents,
        std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) {
    if(!threadPool) threadPool = m_topic->driver()->defaultThreadPool();
    size_t                  pending_events = 0;
    std::mutex              pending_mutex;
    std::condition_variable pending_cv;
    try {
        for(size_t i = 0; i < maxEvents.value; ++i) {
            auto event = pull().wait(5000);
            {
                std::unique_lock lock{pending_mutex};
                pending_events += 1;
            }
            threadPool->pushWork([&, event=std::move(event)]() {
                processor(event);
                std::unique_lock lock{pending_mutex};
                pending_events -= 1;
                if(pending_events == 0)
                    pending_cv.notify_all();
            });
        }
    } catch(const diaspora::StopEventProcessor&) {}
    std::unique_lock lock{pending_mutex};
    while(pending_events) pending_cv.wait(lock);
}

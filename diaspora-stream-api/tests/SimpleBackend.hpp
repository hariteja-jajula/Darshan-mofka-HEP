#include <diaspora/Consumer.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/ThreadPool.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/BufferWrapperArchive.hpp>

#include <nlohmann/json.hpp>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <variant>
#include <iostream>


class SimpleDriver;
class SimpleProducer;
class SimpleConsumer;
class SimpleTopicHandle;


template<typename T>
struct FutureState {

    std::mutex                           mutex;
    std::condition_variable              cv;
    std::variant<T, diaspora::Exception> value;
    bool                                 is_set = false;

    template<typename U>
    void set(U u) {
        if(is_set) throw diaspora::Exception{"Promise already set"};
        {
            std::unique_lock lock{mutex};
            value = std::move(u);
            is_set = true;
        }
        cv.notify_all();
    }

    T wait(int timeout_ms) {
        std::unique_lock lock{mutex};
        if(!is_set) {
            if(timeout_ms > 0)
                cv.wait_for(lock, std::chrono::milliseconds{timeout_ms});
            else
                while(!is_set) cv.wait(lock);
        }
        if(std::holds_alternative<T>(value))
            return std::get<T>(value);
        else
            throw std::get<diaspora::Exception>(value);
    }

    bool test() {
        std::unique_lock lock{mutex};
        return is_set;
    }
};


class SimpleThreadPool final : public diaspora::ThreadPoolInterface {

    struct Work {

        uint64_t              priority;
        std::function<void()> func;

        Work(uint64_t p, std::function<void()> f)
        : priority(p)
        , func{std::move(f)} {}

        friend bool operator<(const Work& lhs, const Work& rhs) {
            return lhs.priority < rhs.priority;
        }
    };

    std::vector<std::thread>  m_threads;
    std::priority_queue<Work> m_queue;
    mutable std::mutex        m_queue_mtx;
    std::condition_variable   m_queue_cv;
    std::atomic<bool>         m_must_stop = false;

    public:

    SimpleThreadPool(diaspora::ThreadCount count) {
        m_threads.reserve(count.count);
        for(size_t i = 0; i < count.count; ++i) {
            m_threads.emplace_back([this]() {
                while(!m_must_stop) {
                    std::unique_lock<std::mutex> lock{m_queue_mtx};
                    if(m_queue.empty()) m_queue_cv.wait(lock);
                    if(m_queue.empty()) continue;
                    auto work = m_queue.top();
                    m_queue.pop();
                    lock.unlock();
                    work.func();
                }
            });
        }
    }

    ~SimpleThreadPool() {
        m_must_stop = true;
        m_queue_cv.notify_all();
        for(auto& th : m_threads) th.join();
    }

    diaspora::ThreadCount threadCount() const override {
        return diaspora::ThreadCount{m_threads.size()};
    }

    void pushWork(std::function<void()> func,
                  uint64_t priority = std::numeric_limits<uint64_t>::max()) override {
        if(m_threads.size() == 0) {
            func();
        } else {
            {
                std::unique_lock<std::mutex> lock{m_queue_mtx};
                m_queue.emplace(priority, std::move(func));
            }
            m_queue_cv.notify_one();
        }
    }

    size_t size() const override {
        std::unique_lock<std::mutex> lock{m_queue_mtx};
        return m_queue.size();
    }
};


class SimpleEvent : public diaspora::EventInterface {

    diaspora::Metadata      m_metadata;
    diaspora::DataView      m_data;
    diaspora::PartitionInfo m_partition;
    diaspora::EventID       m_id;

    public:

    SimpleEvent(diaspora::Metadata metadata,
                diaspora::DataView data,
                diaspora::PartitionInfo partition,
                diaspora::EventID id)
    : m_metadata(std::move(metadata))
    , m_data(std::move(data))
    , m_partition(std::move(partition))
    , m_id(id) {}

    const diaspora::Metadata& metadata() const override {
        return m_metadata;
    }

    const diaspora::DataView& data() const override {
        return m_data;
    }

    diaspora::PartitionInfo partition() const override {
        return m_partition;
    }

    diaspora::EventID id() const override {
        return m_id;
    }

    void acknowledge() const override {}

};


class SimpleProducer final : public diaspora::ProducerInterface {

    const std::string                                  m_name;
    const diaspora::BatchSize                          m_batch_size;
    const diaspora::MaxNumBatches                      m_max_num_batches;
    const diaspora::Ordering                           m_ordering;
    const std::shared_ptr<SimpleThreadPool>            m_thread_pool;
    const std::shared_ptr<SimpleTopicHandle>           m_topic;
    diaspora::Future<std::optional<diaspora::EventID>> m_last_event_pushed;

    public:

    SimpleProducer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        diaspora::Ordering ordering,
        std::shared_ptr<SimpleThreadPool> thread_pool,
        std::shared_ptr<SimpleTopicHandle> topic)
    : m_name{std::move(name)}
    , m_batch_size(batch_size)
    , m_max_num_batches(max_num_batches)
    , m_ordering(ordering)
    , m_thread_pool(std::move(thread_pool))
    , m_topic(std::move(topic)) {}

    const std::string& name() const override {
        return m_name;
    }

    diaspora::BatchSize batchSize() const override {
        return m_batch_size;
    }

    diaspora::MaxNumBatches maxNumBatches() const override {
        return m_max_num_batches;
    }

    diaspora::Ordering ordering() const override {
        return m_ordering;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> threadPool() const override {
        return m_thread_pool;
    }

    std::shared_ptr<diaspora::TopicHandleInterface> topic() const override;

    diaspora::Future<std::optional<diaspora::EventID>> push(
            diaspora::Metadata metadata,
            diaspora::DataView data,
            std::optional<size_t> partition) override;

    diaspora::Future<std::optional<diaspora::Flushed>> flush() override {
        return diaspora::Future<std::optional<diaspora::Flushed>>{
            [ev=m_last_event_pushed](int timeout_ms) -> std::optional<diaspora::Flushed> {
                if(ev.wait(timeout_ms).has_value()) return  diaspora::Flushed{};
                else return std::nullopt;
            },
            [ev=m_last_event_pushed]() {
                return ev.completed();
            }};
    }
};


class SimpleConsumer final : public diaspora::ConsumerInterface {

    const std::string                        m_name;
    const diaspora::BatchSize                m_batch_size;
    const diaspora::MaxNumBatches            m_max_num_batches;
    const std::shared_ptr<SimpleThreadPool>  m_thread_pool;
    const std::shared_ptr<SimpleTopicHandle> m_topic;
    const diaspora::DataAllocator            m_data_allocator;
    const diaspora::DataSelector             m_data_selector;
    size_t                                   m_next_offset = 0;

    public:

    SimpleConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<SimpleThreadPool> thread_pool,
        std::shared_ptr<SimpleTopicHandle> topic,
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


class SimpleTopicHandle final : public diaspora::TopicHandleInterface,
                                public std::enable_shared_from_this<SimpleTopicHandle> {

    friend class SimpleProducer;
    friend class SimpleConsumer;

    struct Partition {
        std::vector<std::vector<char>> metadata;
        std::vector<std::vector<char>> data;
    };

    const std::string                       m_name;
    const std::vector<diaspora::PartitionInfo> m_pinfo = {diaspora::PartitionInfo("{}")};
    const diaspora::Validator                  m_validator;
    const diaspora::PartitionSelector          m_partition_selector;
    const diaspora::Serializer                 m_serializer;
    const std::shared_ptr<SimpleDriver>     m_driver;

    mutable std::mutex                      m_partition_mtx;
    Partition                               m_partition;

    public:

    SimpleTopicHandle(
        std::string name,
        diaspora::Validator validator,
        diaspora::PartitionSelector partition_selector,
        diaspora::Serializer serializer,
        std::shared_ptr<SimpleDriver> driver)
    : m_name{std::move(name)}
    , m_validator(std::move(validator))
    , m_partition_selector(std::move(partition_selector))
    , m_serializer(std::move(serializer))
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
        return m_validator;
    }

    diaspora::PartitionSelector selector() const override {
        return m_partition_selector;
    }

    diaspora::Serializer serializer() const override {
        return m_serializer;
    }

    std::shared_ptr<diaspora::ProducerInterface>
        makeProducer(std::string_view name,
                     diaspora::BatchSize batch_size,
                     diaspora::MaxNumBatches max_batch,
                     diaspora::Ordering ordering,
                     std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
                     diaspora::Metadata options) override;

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

inline std::shared_ptr<diaspora::TopicHandleInterface> SimpleProducer::topic() const {
    return m_topic;
}

inline std::shared_ptr<diaspora::TopicHandleInterface> SimpleConsumer::topic() const {
    return m_topic;
}

inline diaspora::Future<std::optional<diaspora::EventID>> SimpleProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto state = std::make_shared<FutureState<std::optional<diaspora::EventID>>>();
    m_thread_pool->pushWork(
        [topic=m_topic, state,
         metadata=std::move(metadata),
         data=std::move(data),
         partition]() {
            try {
                // validation
                topic->validator().validate(metadata, data);
                // serialization
                std::vector<char> metadata_buffer, data_buffer;
                diaspora::BufferWrapperOutputArchive archive(metadata_buffer);
                topic->serializer().serialize(archive, metadata);
                data_buffer.resize(data.size());
                data.read(data_buffer.data(), data_buffer.size());
                // partition selection
                auto index = topic->m_partition_selector.selectPartitionFor(metadata, partition);
                if(index != 0) throw diaspora::Exception{"Invalid index returned by PartitionSelector"};
                std::lock_guard<std::mutex> lock{topic->m_partition_mtx};
                auto& metadata_vector = topic->m_partition.metadata;
                auto& data_vector = topic->m_partition.data;
                metadata_vector.push_back(std::move(metadata_buffer));
                data_vector.push_back(std::move(data_buffer));
                auto event_id = metadata_vector.size()-1;
                // set the ID
                state->set(event_id);
            } catch(const diaspora::Exception& ex) {
                state->set(ex);
            }
        });
    auto future = diaspora::Future<std::optional<diaspora::EventID>>{
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
    m_last_event_pushed = future;
    return future;
}

class SimpleDriver : public diaspora::DriverInterface,
                     public std::enable_shared_from_this<SimpleDriver> {

    std::shared_ptr<diaspora::ThreadPoolInterface> m_default_thread_pool =
        std::make_shared<SimpleThreadPool>(diaspora::ThreadCount{0});
    std::unordered_map<std::string, std::shared_ptr<SimpleTopicHandle>> m_topics;

    public:

    void createTopic(std::string_view name,
                     const diaspora::Metadata& options,
                     std::shared_ptr<diaspora::ValidatorInterface> validator,
                     std::shared_ptr<diaspora::PartitionSelectorInterface> selector,
                     std::shared_ptr<diaspora::SerializerInterface> serializer) override {
        (void)options;
        if(m_topics.count(std::string{name})) throw diaspora::Exception{"Topic already exists"};
        std::vector<diaspora::PartitionInfo> pinfo{diaspora::PartitionInfo{"{}"}};
        if(selector) selector->setPartitions(pinfo);
        m_topics.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::string{name}),
            std::forward_as_tuple(
                std::make_shared<SimpleTopicHandle>(
                    std::string{name},
                    std::move(validator),
                    std::move(selector),
                    std::move(serializer),
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

    std::unordered_map<std::string, diaspora::Metadata> listTopics() const override {
        std::unordered_map<std::string, diaspora::Metadata> result;
        for (const auto& [topic_name, topic_handle] : m_topics) {
            nlohmann::json topic_info = nlohmann::json::object();
            topic_info["name"] = topic_name;
            topic_info["num_partitions"] = topic_handle->partitions().size();
            result[topic_name] = diaspora::Metadata{std::move(topic_info)};
        }
        return result;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> defaultThreadPool() const override {
        return m_default_thread_pool;
    }

    std::shared_ptr<diaspora::ThreadPoolInterface> makeThreadPool(diaspora::ThreadCount count) const override {
        return std::make_shared<SimpleThreadPool>(count);
    }

    static inline std::shared_ptr<diaspora::DriverInterface> create(const diaspora::Metadata&) {
        return std::make_shared<SimpleDriver>();
    }
};


inline std::shared_ptr<diaspora::DriverInterface> SimpleTopicHandle::driver() const {
    return m_driver;
}

inline std::shared_ptr<diaspora::ProducerInterface>
    SimpleTopicHandle::makeProducer(std::string_view name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_batch,
        diaspora::Ordering ordering,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        diaspora::Metadata options) {
    (void)options;
    if(!thread_pool) thread_pool = m_driver->makeThreadPool(diaspora::ThreadCount{0});
    auto simple_thread_pool = std::dynamic_pointer_cast<SimpleThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of SimpleThreadPool"};
    return std::make_shared<SimpleProducer>(
            std::string{name}, batch_size, max_batch, ordering, simple_thread_pool,
            shared_from_this());
}

inline std::shared_ptr<diaspora::ConsumerInterface>
    SimpleTopicHandle::makeConsumer(std::string_view name,
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
    auto simple_thread_pool = std::dynamic_pointer_cast<SimpleThreadPool>(thread_pool);
    if(!simple_thread_pool)
        throw diaspora::Exception{"ThreadPool should be an instance of SimpleThreadPool"};
    return std::make_shared<SimpleConsumer>(
            std::string{name}, batch_size, max_batch, simple_thread_pool,
            shared_from_this(), std::move(data_allocator),
            std::move(data_selector));
}

void SimpleConsumer::unsubscribe() {}

diaspora::Future<std::optional<diaspora::Event>> SimpleConsumer::pull() {
    std::lock_guard<std::mutex> lock{m_topic->m_partition_mtx};
    if(m_next_offset == m_topic->m_partition.metadata.size()) {
        return diaspora::Future<std::optional<diaspora::Event>>{
            [](int) {
                return diaspora::Event(std::make_shared<SimpleEvent>(
                        diaspora::Metadata{}, diaspora::DataView{}, diaspora::PartitionInfo{},
                        diaspora::NoMoreEvents));},
            []() { return true; }
        };
    } else {
        // get the metadata and data from the topic
        auto& metadata_buffer = m_topic->m_partition.metadata[m_next_offset];
        auto& data_buffer     = m_topic->m_partition.data[m_next_offset];
        diaspora::Metadata metadata;
        // deserialize metadata
        diaspora::BufferWrapperInputArchive archive(
            std::string_view{metadata_buffer.data(), metadata_buffer.size()});
        m_topic->m_serializer.deserialize(archive, metadata);
        // invoke the data selector
        auto data_descriptor = m_data_selector ?
            m_data_selector(metadata, diaspora::DataDescriptor("", data_buffer.size()))
            : diaspora::DataDescriptor{};
        // invoke the data allocator
        auto data_view = m_data_allocator ?
            m_data_allocator(metadata, data_descriptor)
            : diaspora::DataView{};

        // copy the data to target destination
        auto segments = data_descriptor.flatten();
        size_t dest_offset = 0;
        for(auto& s : segments) {
            data_view.write(data_buffer.data() + s.offset, s.size, dest_offset);
            dest_offset += s.size;
        }
        m_next_offset += 1;
        return diaspora::Future<std::optional<diaspora::Event>>{
            [metadata=std::move(metadata), data=std::move(data_view), event_id=m_next_offset-1](int) {
                return diaspora::Event(std::make_shared<SimpleEvent>(
                        std::move(metadata), std::move(data), diaspora::PartitionInfo{},
                        event_id));},
            []() { return true; }
        };
    }
}

inline void SimpleConsumer::process(
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
            auto event = pull().wait(timeout_ms);
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

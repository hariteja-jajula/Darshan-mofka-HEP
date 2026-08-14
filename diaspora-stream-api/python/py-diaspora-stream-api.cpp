#define PYBIND11_DETAILED_ERROR_MESSAGES
#define PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "pybind11_json/pybind11_json.hpp"
#include <diaspora/DataView.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/ThreadPool.hpp>
#include <diaspora/Driver.hpp>
#include "../src/JsonUtil.hpp"

#include <iostream>
#include <numeric>

namespace py = pybind11;
using namespace pybind11::literals;

namespace diaspora {

struct PythonBindingHelper {

    template<typename T>
    static auto GetSelf(const T& x) {
        return x.self;
    }

    template<typename T, typename I>
    static auto FromInterface(std::shared_ptr<I> impl) {
        return T{std::move(impl)};
    }
};

}

static auto get_buffer_info(const py::buffer& buf) {
    return buf.request();
}

static void check_buffer_is_contiguous(const py::buffer_info& buf_info) {
    if (!(PyBuffer_IsContiguous((buf_info).view(), 'C')
       || PyBuffer_IsContiguous((buf_info).view(), 'F')))
        throw diaspora::Exception("Non-contiguous Python buffers are not yet supported");
}

static void check_buffer_is_writable(const py::buffer_info& buf_info) {
    if(buf_info.readonly) throw diaspora::Exception("Python buffer is read-only");
}

static auto data_helper(const py::buffer& buffer) {
    auto buffer_info = get_buffer_info(buffer);
    check_buffer_is_contiguous(buffer_info);
    auto owner = new py::object(std::move(buffer));
    auto free_cb = [owner](diaspora::DataView::UserContext) { delete owner; };
    return diaspora::DataView(buffer_info.ptr, buffer_info.size, owner, std::move(free_cb));
}

static auto data_helper(const py::list& buffers) {
    std::vector<diaspora::DataView::Segment> segments;
    segments.reserve(buffers.size());
    for (auto& buff : buffers){
        auto buff_info = get_buffer_info(buff.cast<py::buffer>());
        check_buffer_is_contiguous(buff_info);
        segments.push_back(
            diaspora::DataView::Segment{
                buff_info.ptr,
                static_cast<size_t>(buff_info.size)});
    }
    auto owner = new py::object(std::move(buffers));
    auto free_cb = [owner](diaspora::DataView::UserContext) { delete owner; };
    return diaspora::DataView(std::move(segments), owner, std::move(free_cb));
}

using PythonDataSelector = std::function<std::optional<diaspora::DataDescriptor>(const nlohmann::json&, const diaspora::DataDescriptor&)>;
using PythonDataAllocator = std::function<py::list(const nlohmann::json&, const diaspora::DataDescriptor&)>;

struct AbstractDataOwner {
    virtual py::object toPythonObject() const = 0;
    virtual ~AbstractDataOwner() = default;
};

struct __attribute__ ((visibility("hidden"))) PythonDataOwner : public AbstractDataOwner {
    py::object m_obj;

    PythonDataOwner(py::object obj)
    : m_obj{std::move(obj)} {}

    py::object toPythonObject() const override {
        return m_obj;
    }
};

struct BufferDataOwner : public AbstractDataOwner {
    std::vector<char> m_data;

    BufferDataOwner(size_t size)
    : m_data(size) {}

    py::object toPythonObject() const override {
        py::list result;
        result.append(py::memoryview::from_memory(m_data.data(), m_data.size()));
        return result;
    }
};

PYBIND11_MODULE(pydiaspora_stream_api, m) {
    m.doc() = "Python binding for the Diaspora Stream API library";

    py::register_exception<diaspora::Exception>(m, "Exception", PyExc_RuntimeError);

    m.attr("AdaptiveBatchSize") = py::int_(diaspora::BatchSize::Adaptive().value);

    py::class_<diaspora::ValidatorInterface,
               std::shared_ptr<diaspora::ValidatorInterface>>(m, "Validator")
        .def_static("from_metadata",
            [](const py::kwargs& kwargs){
                auto md = py::cast<nlohmann::json>((const py::dict&)kwargs);
                return diaspora::PythonBindingHelper::GetSelf(
                    diaspora::Validator::FromMetadata(md));
            }, R"(
            Create a Validator instance from some Metadata, provided as kwargs.
            The metadata is expected to be provide at least a "type" field.
            The type can be in the form "name:library.so" if library.so must be
            loaded to access the Validator. If the "type" field is not provided,
            it is assumed to be "default".

            Parameters
            ----------

            kwargs: Arguments to pass to the Validator factory. Must include a
                    type argument (string).

            Returns
            -------

            New Validator instance.
            )")
    ;

    py::class_<diaspora::ThreadPoolInterface,
               std::shared_ptr<diaspora::ThreadPoolInterface>>(m, "ThreadPool")
        .def_property_readonly("thread_count",
            [](const diaspora::ThreadPoolInterface& thread_pool) -> std::size_t {
                return thread_pool.threadCount().count;
            }, "Returns the number of underlying threads.")
    ;

    py::enum_<diaspora::Ordering>(m, "Ordering")
        .value("Strict", diaspora::Ordering::Strict)
        .value("Loose", diaspora::Ordering::Loose)
    ;

    py::class_<diaspora::SerializerInterface,
               std::shared_ptr<diaspora::SerializerInterface>>(m, "Serializer")
        .def_static("from_metadata",
            [](const py::kwargs& kwargs){
                auto md = py::cast<nlohmann::json>((const py::dict&)kwargs);
                return diaspora::PythonBindingHelper::GetSelf(
                    diaspora::Serializer::FromMetadata( md));
            }, R"(
            Create a Serializer instance from some Metadata, provided as kwargs.
            The metadata argument is expected to be a dictionary object with at
            least a "type" field. The type can be in the form "name:library.so"
            if library.so must be loaded to access the Serializer. If the "type"
            field is not provided, it is assumed to be "default".

            Parameters
            ----------

            kwargs (dict): Arguments to pass to the Serializer factory.

            Returns
            -------

            New Serializer instance.
            )")
    ;

    py::class_<diaspora::PartitionSelectorInterface,
               std::shared_ptr<diaspora::PartitionSelectorInterface>>(m, "PartitionSelector")
        .def_static("from_metadata",
            [](const py::kwargs& kwargs){
                auto md = py::cast<nlohmann::json>((const py::dict&)kwargs);
                return diaspora::PythonBindingHelper::GetSelf(
                    diaspora::PartitionSelector::FromMetadata(md));
            }, R"(
            Create a PartitionSelector instance from some Metadata, provided as kwargs.
            The metadata argument is expected to be a dictionary object with at
            least a "type" field. The type can be in the form "name:library.so"
            if library.so must be loaded to access the Serializer. If the "type"
            field is not provided, it is assumed to be "default".

            Parameters
            ----------

            kwargs (dict): Arguments to pass to the PartitionSelector factory.

            Returns
            -------

            New PartitionSelector instance.
            )")
    ;

    py::class_<diaspora::DriverInterface,
               std::shared_ptr<diaspora::DriverInterface>>(m, "Driver")
        .def(py::init(
            [](const std::string& name, const nlohmann::json& md) {
                return diaspora::DriverFactory::create(name, diaspora::Metadata{md});
            }),
            R"(
            Create a new Driver from a name an some backend-specific metadata.
            The name can be in the "backend" if the backend has already been
            loaded into the process' memory. Using "backend:library.so" will cause
            the factory to dlopen library.so before searching for the "backend".

            Parameters
            ----------

            name (str): Name of the backend.
            options (dict): Backend-specific configuration.

            Returns
            -------

            A Driver instance.
            )",
            py::kw_only(), "backend"_a, "options"_a=nlohmann::json::object())
        .def("create_topic",
             [](diaspora::DriverInterface& driver,
                std::string_view name,
                const nlohmann::json& options,
                std::shared_ptr<diaspora::ValidatorInterface> validator,
                std::shared_ptr<diaspora::PartitionSelectorInterface> partition_selector,
                std::shared_ptr<diaspora::SerializerInterface> serializer) {
                    driver.createTopic(name, options, validator, partition_selector, serializer);
             },
             R"(
             Create a topic with a given name, if it does not exist yet.

             Parameters
             ----------

             name (str): Name of the topic to create.
             options (dict): Backend-specific options.
             validator (Validator): Validator instance to call when pushing events.
             partition_selector (PartitionSelector): PartitionSelector to call to choose a partition.
             serializer (Serializer): Serializer to call to serialize events metadata.
             )",
             "name"_a, py::kw_only(), "options"_a=nlohmann::json::object(),
             "validator"_a=diaspora::PythonBindingHelper::GetSelf(diaspora::Validator{}),
             "partition_selector"_a=diaspora::PythonBindingHelper::GetSelf(diaspora::PartitionSelector{}),
             "serializer"_a=diaspora::PythonBindingHelper::GetSelf(diaspora::Serializer{}))
        .def("open_topic",
             &diaspora::DriverInterface::openTopic,
             R"(
             Open a topic with a given its name.

             Parameters
             ----------

             name (str): Name of the topic to open.

             Returns
             -------

             A TopicHandle instance representing the opened topic.
             )",
             "name"_a)
        .def("topic_exists",
             &diaspora::DriverInterface::topicExists,
             R"(
             Check if a topic with a given name exists.

             Parameters
             ----------

             name (str): Name of the topic.

             Returns
             -------

             True if the topic exists, False otherwise.
             )",
             "name"_a)
        .def("list_topics",
             [](const diaspora::DriverInterface& driver) {
                auto topics = driver.listTopics();
                std::unordered_map<std::string, nlohmann::json> result;
                for (const auto& [name, metadata] : topics) {
                    result[name] = metadata.json();
                }
                return result;
             },
             R"(
             List all topics and their metadata.

             Returns
             -------

             A dictionary mapping topic names to their metadata (as dictionaries).
             Each metadata dictionary contains fields such as:
             - name: The topic name
             - path: The filesystem path to the topic
             - num_partitions: The number of partitions
             - validator: The validator JSON configuration (if present)
             - serializer: The serializer JSON configuration (if present)
             - partition_selector: The partition selector JSON configuration (if present)
             )")
        .def("make_thread_pool",
             [](const diaspora::DriverInterface& driver, size_t count) {
                return driver.makeThreadPool(diaspora::ThreadCount{count});
                }, R"(
             Create a ThreadPool instance with the given number of threads.

             Parameters
             ----------

             count (int): Number of threads.

             Returns
             -------

             A ThreadPool instance.
             )",
             "count"_a)
        .def_property_readonly("default_thread_pool",
             &diaspora::DriverInterface::defaultThreadPool,
             R"(
             Get the default ThreadPool of the Driver.

             Returns
             -------

             A ThreadPool instance.
             )")
    ;

    py::class_<diaspora::TopicHandleInterface,
               std::shared_ptr<diaspora::TopicHandleInterface>>(m, "TopicHandle")
        .def_property_readonly(
                "name",
                &diaspora::TopicHandleInterface::name,
                "Name of the topic.")
        .def_property_readonly("partitions",
            [](const diaspora::TopicHandleInterface& topic) {
                std::vector<nlohmann::json> result;
                for(auto& p : topic.partitions()) {
                    result.push_back(p.json());
                }
                return result;
            }, "List of partitions of the topic.")
        .def("producer",
            [](diaspora::TopicHandleInterface& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               diaspora::Ordering ordering,
               std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
               const nlohmann::json& options) {
                return topic.makeProducer(
                    name, diaspora::BatchSize(batch_size),
                    diaspora::MaxNumBatches{max_batch}, ordering, thread_pool,
                    diaspora::Metadata{options});
            },
            R"(
            Create a Producer instance to produce events in this topic.

            Parameters
            ----------

            name (str): Name of the producer.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of pending batches before push becomes blocking.
            ordering (Ordering): type of ordering (strict or loose).
            thread_pool (ThreadPool): ThreadPool to use for any work needed for producing the events.
            options (dict): Backend-specific options.

            Returns
            -------

            A Producer instance.
            )",
            "name"_a="", py::kw_only(),
            "batch_size"_a=diaspora::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "ordering"_a=diaspora::Ordering::Strict,
            "thread_pool"_a=std::shared_ptr<diaspora::ThreadPoolInterface>{},
            "options"_a=nlohmann::json::object())
        .def("consumer",
            [](diaspora::TopicHandleInterface& topic,
               std::string_view name,
               PythonDataSelector selector,
               PythonDataAllocator allocator,
               std::size_t batch_size,
               std::size_t max_batch,
               std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
               std::optional<std::vector<size_t>> targets,
               const nlohmann::json& options) {
                auto cpp_allocator = allocator ?
                    [allocator=std::move(allocator)]
                    (const diaspora::Metadata& metadata,
                     const diaspora::DataDescriptor& descriptor) -> diaspora::DataView {
                        py::gil_scoped_acquire gil;
                        auto segments = allocator(metadata.json(), descriptor);
                        std::vector<diaspora::DataView::Segment> cpp_segments;
                        cpp_segments.reserve(segments.size());
                        for(auto& segment : segments) {
                            auto buf_info = get_buffer_info(segment.cast<py::buffer>());
                            check_buffer_is_writable(buf_info);
                            check_buffer_is_contiguous(buf_info);
                            cpp_segments.push_back({buf_info.ptr, (size_t)buf_info.size});
                        }
                        auto owner = new PythonDataOwner{std::move(segments)};
                        auto free_cb = [owner](diaspora::DataView::UserContext) { delete owner; };
                        auto data = diaspora::DataView{std::move(cpp_segments), owner, std::move(free_cb)};
                        return data;
                }
                : diaspora::DataAllocator{};
                auto cpp_selector = selector ?
                    [selector=std::move(selector)]
                    (const diaspora::Metadata& metadata,
                     const diaspora::DataDescriptor& descriptor) -> diaspora::DataDescriptor {
                        py::gil_scoped_acquire gil;
                        std::optional<diaspora::DataDescriptor> result = selector(metadata.json(), descriptor);
                        if(result) return result.value();
                        else return diaspora::DataDescriptor();
                    }
                : diaspora::DataSelector{};
                std::vector<size_t> default_targets;
                return topic.makeConsumer(
                    name, diaspora::BatchSize(batch_size),
                    diaspora::MaxNumBatches{max_batch},
                    thread_pool,
                    diaspora::DataAllocator{cpp_allocator},
                    diaspora::DataSelector{cpp_selector},
                    targets.value_or(default_targets),
                    diaspora::Metadata{options});
               },
            R"(
            Create a Consumer instance to consume events from this topic.

            The data_selector argument must be a callable taking a dictionary (an event's metadata)
            and a DataDescriptor as arguments, and returning a DataDescriptor representing the subset
            of the event's data to load.

            The data_allocator argument must be a callable taking a dictionary (an event's metadata)
            and a DataDescriptor as arguments, and returning a list of object satisfying the buffer
            protocol (e.g. bytearray, memoryview, numpy array, etc.).

            Parameters
            ----------

            name (str): Name of the consumer.
            data_selector (Callable[Optional[DataDescriptor], [dict, DataDescriptor]]): data selector.
            data_allocator (Callable[list, [dict, DataDescriptor]]): data allocator.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of batches to prefetch at any time.
            thread_pool (ThreadPool): ThreadPool to use for any work needed for consuming the events.
            targets (Optional[list[int]): List of indices of partitions to consume from.
            options (dict): Backend-specific options.

            Returns
            -------

            A Consumer instance.
            )",
            "name"_a, py::kw_only(),
            "data_selector"_a, "data_allocator"_a,
            "batch_size"_a=diaspora::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "thread_pool"_a=diaspora::PythonBindingHelper::GetSelf(diaspora::ThreadPool{}),
            "targets"_a=std::optional<std::vector<size_t>>{},
            "options"_a=nlohmann::json::object())
        .def("consumer",
            [](diaspora::TopicHandleInterface& topic,
               std::string_view name,
               std::size_t batch_size,
               std::size_t max_batch,
               std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
               std::optional<std::vector<size_t>> targets,
               const nlohmann::json& options) {
                auto cpp_allocator = [](const diaspora::Metadata& metadata,
                                     const diaspora::DataDescriptor& descriptor) -> diaspora::DataView {
                        (void)metadata;
                        auto owner = new BufferDataOwner{descriptor.size()};
                        std::vector<diaspora::DataView::Segment> cpp_segment{
                            diaspora::DataView::Segment{owner->m_data.data(), owner->m_data.size()}
                        };
                        auto free_cb = [owner](diaspora::DataView::UserContext) { delete owner; };
                        auto data = diaspora::DataView{std::move(cpp_segment), owner, std::move(free_cb)};
                        return data;
                };
                auto cpp_selector = [](const diaspora::Metadata& metadata,
                                       const diaspora::DataDescriptor& descriptor) -> diaspora::DataDescriptor {
                        (void)metadata;
                        return descriptor;
                };
                std::vector<size_t> default_targets;
                return topic.makeConsumer(
                    name, diaspora::BatchSize(batch_size),
                    diaspora::MaxNumBatches{max_batch}, thread_pool,
                    diaspora::DataAllocator{cpp_allocator},
                    diaspora::DataSelector{cpp_selector},
                    targets.value_or(default_targets),
                    diaspora::Metadata{options});
               },
            R"(
            Create a Consumer instance to consume events from this topic.
            With version of the consumer function returns a consumer that always loads all
            of an event's data, and returns it as an internal buffer, of which the returned
            event's data method will return a memoryview.

            Parameters
            ----------

            name (str): Name of the consumer.
            batch_size (int): Batch size.
            max_num_batches (int): Maximum number of batches to prefetch at any time.
            thread_pool (ThreadPool): ThreadPool to use for any work needed for consuming the events.
            targets (Optional[list[int]): List of indices of partitions to consume from.
            options (dict): Backend-specific options.

            Returns
            -------

            A Consumer instance.
            )",
            "name"_a, py::kw_only(),
            "batch_size"_a=diaspora::BatchSize::Adaptive().value,
            "max_num_batches"_a=2,
            "thread_pool"_a=diaspora::PythonBindingHelper::GetSelf(diaspora::ThreadPool{}),
            "targets"_a=std::optional<std::vector<size_t>>{},
            "options"_a=nlohmann::json::object())
    ;

    py::class_<diaspora::ProducerInterface,
               std::shared_ptr<diaspora::ProducerInterface>>(m, "Producer")
        .def_property_readonly("name", &diaspora::ProducerInterface::name, "Name of the producer.")
        .def_property_readonly("thread_pool", &diaspora::ProducerInterface::threadPool,
                               "ThreadPool used by the producer.")
        .def_property_readonly("topic", &diaspora::ProducerInterface::topic,
                               "TopicHandle of the topic this producer produces to.")
        .def("push",
            [](diaspora::ProducerInterface& producer,
               std::string metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> diaspora::Future<std::optional<diaspora::EventID>> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a memoryview or any object satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a string.
            data (memoryview): Data part of the event (single memoryview).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](diaspora::ProducerInterface& producer,
               nlohmann::json metadata,
               py::buffer b_data,
               std::optional<size_t> part) -> diaspora::Future<std::optional<diaspora::EventID>> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a memoryview or any object satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (dict): Metadata part of the event, as a dictionary.
            data (memoryview): Data part of the event (single memoryview).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](diaspora::ProducerInterface& producer,
               std::string metadata,
               const py::list& b_data,
               std::optional<size_t> part) -> diaspora::Future<std::optional<diaspora::EventID>> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a list of memoryviews or any objects satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a string.
            data (list[memoryview]): Data part of the event (list of memoryviews).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=std::vector<py::memoryview>{},
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("push",
            [](diaspora::ProducerInterface& producer,
               nlohmann::json metadata,
               py::list b_data,
               std::optional<size_t> part) -> diaspora::Future<std::optional<diaspora::EventID>> {
                return producer.push(std::move(metadata), data_helper(b_data), part);
            }, R"(
            Push an event into the topic.

            The data part may be a list of memoryviews or any objects satisfying the buffer protocol.

            If partition is provided, the PartitionSelector may or may not choose
            to send the event to this requested partition.

            Parameters
            ----------

            metadata (str): Metadata part of the event, as a dictionary.
            data (list[memoryview]): Data part of the event (list of memoryviews).
            partition (int): Request that the event be sent to a specific partition.
            )",
            "metadata"_a,
            "data"_a=py::memoryview::from_memory(nullptr, 0, true),
            py::kw_only(),
            "partition"_a=std::nullopt)
        .def("flush", &diaspora::ProducerInterface::flush,
             "Flush the producer, i.e. ensure all the events previously pushed have been sent.")
        .def_property_readonly("batch_size",
            [](const diaspora::ProducerInterface& producer) -> std::size_t {
                return producer.batchSize().value;
            }, "Return the batch size of the producer.")
        .def_property_readonly("max_num_batches",
            [](const diaspora::ProducerInterface& producer) -> std::size_t {
                return producer.maxNumBatches().value;
            }, "Return the maximum number of pending batches in the producer.")
    ;

    py::class_<diaspora::ConsumerInterface,
               std::shared_ptr<diaspora::ConsumerInterface>>(m, "Consumer")
        .def_property_readonly("name", &diaspora::ConsumerInterface::name,
                               "Name of the consumer.")
        .def_property_readonly("thread_pool", &diaspora::ConsumerInterface::threadPool,
                               "ThreadPool used by the consumer.")
        .def_property_readonly("topic", &diaspora::ConsumerInterface::topic,
                               "TopicHandle of the topic from which this consumer receives events.")
        .def_property_readonly("data_allocator", &diaspora::ConsumerInterface::dataAllocator,
                               "Callable used to allocate memory for the data part of events.")
        .def_property_readonly("data_selector", &diaspora::ConsumerInterface::dataSelector,
                               "Callable used to select the part of the data to load.")
        .def_property_readonly("batch_size",
            [](const diaspora::ConsumerInterface& consumer) -> std::size_t {
                return consumer.batchSize().value;
            }, "Bath size used by the consumer.")
        .def_property_readonly("max_num_batches",
            [](const diaspora::ConsumerInterface& consumer) -> std::size_t {
                return consumer.maxNumBatches().value;
            }, "Maximum number of bathes used by the consumer internally.")
        .def("pull", &diaspora::ConsumerInterface::pull, R"(
            Pull an event. This function will immediately return a FutureEvent.
            Calling wait() on this future will block until an event is available.

            Returns
            -------

            A FutureEvent instance.
        )")
        .def("process",
            [](diaspora::ConsumerInterface& consumer,
               diaspora::EventProcessor processor,
               int timeout_ms,
               std::size_t maxEvents,
               std::shared_ptr<diaspora::ThreadPoolInterface> threadPool) {
                return consumer.process(
                    processor, timeout_ms, diaspora::NumEvents{maxEvents}, threadPool);
               },
            R"(
                Process the incoming events using the provided callable.

                Parameters
                ----------

                processor (Callable[None, Event]): Callable to use to process the events.
                timeout_ms (int): Timeout to pass to wait calls.
                max_events (int): Maximum number of events to process.
                thread_pool (ThreadPool): ThreadPool to use to submit calls to the processor.
            )",
            "processor"_a, py::kw_only(),
            "timeout_ms"_a=-1,
            "max_events"_a=std::numeric_limits<size_t>::max(),
            "thread_pool"_a=std::shared_ptr<diaspora::ThreadPoolInterface>{}
            )
        .def("__iter__",
             [](const py::object& self) -> py::object { return self; },
             "Return self as an iterator.")
        .def("__next__",
             [](diaspora::ConsumerInterface& consumer) -> py::object {
                std::optional<diaspora::Event> result;
                std::exception_ptr eptr;
                Py_BEGIN_ALLOW_THREADS
                try {
                    result = consumer.pull().wait(5000);
                } catch(...) {
                    eptr = std::current_exception();
                }
                Py_END_ALLOW_THREADS
                if(eptr) std::rethrow_exception(eptr);
                if(!result || result.value().id() == diaspora::NoMoreEvents) {
                    throw py::stop_iteration();
                }
                return py::cast(diaspora::PythonBindingHelper::GetSelf(result.value()));
             }, R"(
                Return the next event from the topic.

                Blocks until an event is available, a NoMoreEvents signal is
                received, or a 5-second timeout elapses (StopIteration is raised
                in all terminal cases).

                Returns
                -------

                The next Event.
            )")
        .def("unsubscribe", &diaspora::ConsumerInterface::unsubscribe, R"(
            Unsubscribe this consumer from the topic, sending a removeConsumer
            RPC to the server so that pending feedConsumer operations are
            unblocked. Call this before deleting the consumer to allow the
            server to shut down cleanly.
        )")
    ;

    py::class_<diaspora::DataDescriptor>(m, "DataDescriptor")
        .def(py::init<>())
        .def(py::init<std::string_view, size_t>())
        .def_property_readonly("size", &diaspora::DataDescriptor::size,
                               "Amount of data (in bytes) this DataDescriptor represents.")
        .def_property_readonly("location",
            py::overload_cast<>(&diaspora::DataDescriptor::location, py::const_),
            "Opaque (backend-specific) location string.")
        .def("flatten",
            [](const diaspora::DataDescriptor& dd) {
                std::vector<std::pair<size_t,size_t>> result;
                auto f = dd.flatten();
                for(auto& s : f) result.emplace_back(s.offset, s.size);
                return result;
            }, "Returns a flattened view (list of <offset, size> pairs) of the descriptor.")
        .def("make_stride_view",
            [](const diaspora::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t numblocks,
               std::size_t blocksize,
               std::size_t gapsize) -> diaspora::DataDescriptor {
                return data_descriptor.makeStridedView(offset, numblocks, blocksize, gapsize);
            }, R"(
            Construct a new DataDescriptor by taking a strided selection of the current DataDescriptor.

            Parameters
            ----------

            offset (int): Offset at which to start the selection.
            num_blocks (int): Number of blocks.
            block_size (int): Size of each block.
            gat_size (int): Number of bytes to leave out between each block.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "offset"_a,
            "num_blocks"_a,
            "block_size"_a,
            "gap_size"_a)
        .def("make_sub_view",
            [](const diaspora::DataDescriptor& data_descriptor,
               std::size_t offset,
               std::size_t size) -> diaspora::DataDescriptor {
                return data_descriptor.makeSubView(offset, size);
            }, R"(
            Construct a new DataDescriptor by taking a sub-selection of the current DataDescriptor.

            Parameters
            ----------

            offset (int): Offset at which to start the selection.
            size (int): Size of the selection.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "offset"_a, "size"_a)
        .def("make_unstructured_view",
            [](const diaspora::DataDescriptor& data_descriptor,
               const std::vector<std::pair<std::size_t, std::size_t>> segments) -> diaspora::DataDescriptor {
                std::vector<diaspora::DataDescriptor::Segment> seg;
                seg.reserve(segments.size());
                for(auto& s : segments) seg.push_back(diaspora::DataDescriptor::Segment{s.first, s.second});
                return data_descriptor.makeUnstructuredView(seg);
            }, R"(
            Construct a new DataDescriptor by taking a selection of segments from the current DataDescriptor.

            Parameters
            ----------

            segments (list[tuple[int,int]]): list of segments.

            Returns
            -------

            A new DataDescriptor representing the selection.
            )",
            py::kw_only(),
            "segments"_a)
    ;

    py::class_<diaspora::EventInterface,
               std::shared_ptr<diaspora::EventInterface>>(m, "Event")
        .def_property_readonly("metadata",
                [](diaspora::EventInterface& event) { return event.metadata().json(); },
                "Metadata of the event.")
        .def_property_readonly("data",
                [](diaspora::EventInterface& event) {
                    auto owner = static_cast<AbstractDataOwner*>(event.data().context());
                    return owner->toPythonObject();
                },
                "Data attached to the event.")
        .def_property_readonly("event_id", [](const diaspora::EventInterface& event) -> py::object {
                if(event.id() == diaspora::NoMoreEvents)
                    return py::none();
                else
                    return py::cast(event.id());
                }, "Event ID in its partition.")
        .def_property_readonly("partition",
                [](const diaspora::EventInterface& event) {
                    return event.partition().json();
                }, "Backend-specific information on the partition this event comes from.")
        .def("acknowledge",
             [](const diaspora::EventInterface& event){
                event.acknowledge();
             }, "Acknowledge the event so it is not re-consumed if the consumer restarts.")
    ;

    py::class_<diaspora::Future<std::optional<diaspora::Flushed>>,
               std::shared_ptr<diaspora::Future<std::optional<diaspora::Flushed>>>>(m, "FutureFlushed")
        .def("wait", [](diaspora::Future<std::optional<diaspora::Flushed>>& future, int timeout_ms) {
                std::optional<diaspora::Flushed> result;
                std::exception_ptr eptr;
                Py_BEGIN_ALLOW_THREADS
                try {
                    result = future.wait(timeout_ms);
                } catch (...) {
                    eptr = std::current_exception();
                }
                Py_END_ALLOW_THREADS
                if (eptr) std::rethrow_exception(eptr);
                return result.has_value() ? true : false;
        }, "Wait for the future to complete, returning true if it has completed, false if it timed out.",
        py::kw_only(),
        "timeout_ms"_a=-1)
        .def_property_readonly(
            "completed",
            &diaspora::Future<std::optional<diaspora::Flushed>>::completed,
            "Checks whether the future has completed.")
    ;

    py::class_<diaspora::Future<std::optional<std::uint64_t>>,
               std::shared_ptr<diaspora::Future<std::optional<std::uint64_t>>>>(m, "FutureUint")
        .def("wait", [](diaspora::Future<std::optional<std::uint64_t>>& future, int timeout_ms) {
                std::optional<std::uint64_t> result;
                std::exception_ptr eptr;
                Py_BEGIN_ALLOW_THREADS
                try {
                    result = future.wait(timeout_ms);
                } catch (...) {
                    eptr = std::current_exception();
                }
                Py_END_ALLOW_THREADS
                if (eptr) std::rethrow_exception(eptr);
                return result;
        }, "Wait for the future to complete, returning its value (int) when it does.",
        py::kw_only(),
        "timeout_ms"_a=-1)
        .def_property_readonly(
            "completed",
            &diaspora::Future<std::optional<std::uint64_t>>::completed,
            "Checks whether the future has completed.")
    ;

    py::class_<diaspora::Future<std::optional<diaspora::Event>>,
               std::shared_ptr<diaspora::Future<std::optional<diaspora::Event>>>>(m, "FutureEvent")
        .def("wait", [](diaspora::Future<std::optional<diaspora::Event>>& future, int timeout_ms) {
                std::optional<diaspora::Event> result;
                std::exception_ptr eptr;
            Py_BEGIN_ALLOW_THREADS
            try {
                result = future.wait(timeout_ms);
            } catch (...) {
                eptr = std::current_exception();
            }
            Py_END_ALLOW_THREADS
            if (eptr) std::rethrow_exception(eptr);
            return result.has_value() ? diaspora::PythonBindingHelper::GetSelf(*result) : nullptr;
        }, "Wait for the future to complete, returning its value (Event) when it does.",
        py::kw_only(),
        "timeout_ms"_a=-1)
        .def("completed", &diaspora::Future<std::optional<diaspora::Event>>::completed,
             "Checks whether the future has completed.")
    ;

    PythonDataSelector select_full_data =
        [](const nlohmann::json&, const diaspora::DataDescriptor& d) -> std::optional<diaspora::DataDescriptor> {
            return d;
        };
    m.attr("FullDataSelector") = py::cast(select_full_data);

    PythonDataAllocator bytes_data_allocator =
        [](const nlohmann::json&, const diaspora::DataDescriptor& d) -> py::list {
            auto buffer = py::bytearray();
            auto ret = PyByteArray_Resize(buffer.ptr(), d.size());
            py::list result;
            result.append(std::move(buffer));
            return result;
        };
    m.attr("ByteArrayAllocator") = py::cast(bytes_data_allocator);
}

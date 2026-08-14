#include "Consumer.hpp"
#include "Driver.hpp"
#include "TopicHandle.hpp"
#include "diaspora/ThreadPool.hpp"
#include "Event.hpp"
#include "FutureState.hpp"

#include <diaspora/BufferWrapperArchive.hpp>
#include <condition_variable>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <thread>

namespace files_driver {

PfsConsumer::PfsConsumer(
        std::string name,
        diaspora::BatchSize batch_size,
        diaspora::MaxNumBatches max_num_batches,
        std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
        std::shared_ptr<PfsTopicHandle> topic,
        diaspora::DataAllocator data_allocator,
        diaspora::DataSelector data_selector,
        std::vector<size_t> targets)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_data_allocator{std::move(data_allocator)}
, m_data_selector{std::move(data_selector)}
, m_partition_offsets(m_topic->m_partitions.size(), 0)
, m_prefetch_positions(m_topic->m_partitions.size(), 0)
{
    // If no targets specified, consume from all partitions.
    if (targets.empty()) {
        for (size_t i = 0; i < m_topic->m_partitions.size(); ++i)
            m_targets.push_back(i);
    } else {
        m_targets = std::move(targets);
    }
    m_poll_thread = std::thread([this]() { pollLoop(); });
}

PfsConsumer::~PfsConsumer() {
    {
        std::lock_guard<std::mutex> lock{m_requests_mutex};
        m_stop_polling = true;
    }
    m_requests_cv.notify_one();
    m_poll_thread.join();
    // Reset thread pool after poll thread exits to wait for any in-flight
    // data-processing tasks before member destructors run.
    m_thread_pool.reset();
}

std::shared_ptr<diaspora::TopicHandleInterface> PfsConsumer::topic() const {
      return m_topic;
}

void PfsConsumer::unsubscribe() {}

void PfsConsumer::pollLoop() {
    while (true) {
        std::shared_ptr<FutureState<std::optional<diaspora::Event>>> state;
        {
            std::unique_lock<std::mutex> lock{m_requests_mutex};
            m_requests_cv.wait(lock, [this] {
                return !m_requests.empty() || m_stop_polling;
            });
            if (m_requests.empty()) break;  // stop requested with no pending requests
            state = std::move(m_requests.front());
            m_requests.pop();
        }

        // Poll partitions until an event is found or stop is requested.
        bool found = false;
        while (!m_stop_polling && !found) {
            for (size_t i = 0; i < m_targets.size() && !found; ++i) {
                size_t target_slot = (m_current_target + i) % m_targets.size();
                size_t partition_idx = m_targets[target_slot];
                auto& partition = m_topic->getPartition(partition_idx);

                partition.refreshEventCount();

                if (m_partition_offsets[partition_idx] < partition.numEvents()) {
                    size_t event_offset = m_partition_offsets[partition_idx];

                    // Advance offsets in the poll thread, before dispatching, so that
                    // the next request sees the correct position immediately.
                    m_partition_offsets[partition_idx]++;
                    m_current_target = (target_slot + 1) % m_targets.size();

                    if (m_partition_offsets[partition_idx] >= m_prefetch_positions[partition_idx]) {
                        size_t prefetch_start = m_partition_offsets[partition_idx];
                        partition.prefetchData(prefetch_start, PREFETCH_WINDOW);
                        m_prefetch_positions[partition_idx] = prefetch_start + PREFETCH_WINDOW;
                    }

                    // Dispatch data reading and user callbacks (selector/allocator) to the
                    // thread pool. Capture by value so this lambda has no dependency on
                    // the consumer's lifetime.
                    m_thread_pool->pushWork([state,
                                             topic          = m_topic,
                                             data_selector  = m_data_selector,
                                             data_allocator = m_data_allocator,
                                             partition_idx,
                                             event_offset]() {
                        try {
                            auto& partition = topic->getPartition(partition_idx);

                            auto metadata_buffer = partition.readMetadata(event_offset);
                            diaspora::Metadata metadata;
                            std::string_view metadata_view(metadata_buffer.data(), metadata_buffer.size());
                            diaspora::BufferWrapperInputArchive archive(metadata_view);
                            topic->serializer().deserialize(archive, metadata);

                            auto index_entry = partition.getIndexEntry(event_offset);
                            diaspora::DataDescriptor full_descriptor("", index_entry.data_size);

                            diaspora::DataDescriptor selected_descriptor = full_descriptor;
                            if (data_selector)
                                selected_descriptor = data_selector(metadata, full_descriptor);

                            diaspora::DataView allocated_view;
                            if (data_allocator) {
                                allocated_view = data_allocator(metadata, selected_descriptor);
                                partition.readData(event_offset, selected_descriptor, allocated_view);
                            }

                            auto event = diaspora::Event(std::make_shared<PfsEvent>(
                                std::move(metadata),
                                allocated_view,
                                topic->partitions()[partition_idx],
                                event_offset
                            ));
                            state->set(event);
                        } catch (const diaspora::Exception& ex) {
                            state->set(ex);
                        }
                    });

                    found = true;
                }
            }

            if (!found)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (!found)
            state->set(std::nullopt);  // stop was requested before an event was found
    }

    // Drain requests that were enqueued before stop was noticed.
    std::unique_lock<std::mutex> lock{m_requests_mutex};
    while (!m_requests.empty()) {
        m_requests.front()->set(std::nullopt);
        m_requests.pop();
    }
}

void PfsConsumer::process(
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

diaspora::Future<std::optional<diaspora::Event>> PfsConsumer::pull() {
    auto state = std::make_shared<FutureState<std::optional<diaspora::Event>>>();
    {
        std::lock_guard<std::mutex> lock{m_requests_mutex};
        m_requests.push(state);
    }
    m_requests_cv.notify_one();
    return {
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
}

}

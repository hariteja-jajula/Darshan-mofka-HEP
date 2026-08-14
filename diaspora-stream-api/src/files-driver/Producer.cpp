#include "FutureState.hpp"
#include "Producer.hpp"
#include "TopicHandle.hpp"
#include <spdlog/spdlog.h>

namespace files_driver {

PfsProducer::PfsProducer(
    std::string name,
    diaspora::BatchSize batch_size,
    diaspora::MaxNumBatches max_num_batches,
    diaspora::Ordering ordering,
    std::shared_ptr<diaspora::ThreadPoolInterface> thread_pool,
    std::shared_ptr<PfsTopicHandle> topic)
: m_name{std::move(name)}
, m_batch_size(batch_size)
, m_max_num_batches(max_num_batches)
, m_ordering(ordering)
, m_thread_pool(std::move(thread_pool))
, m_topic(std::move(topic))
, m_partition_batches(m_topic->m_partitions.size())
, m_batch_mutexes(m_topic->m_partitions.size())
, m_partition_pending(m_topic->m_partitions.size())
{
    if (m_batch_size == diaspora::BatchSize::Adaptive())
        spdlog::warn("[files-driver] Producer \"{}\" created with BatchSize::Adaptive(),"
                     " which is unsupported; treating as batch_size=1.", m_name);
}

PfsProducer::~PfsProducer() {
    try {
        for (size_t i = 0; i < m_partition_batches.size(); ++i) {
            std::lock_guard<std::mutex> lock(m_batch_mutexes[i]);
            if (!m_partition_batches[i].empty()) {
                auto pending = std::move(m_partition_pending[i]);
                m_partition_pending[i].clear();
                auto& partition = m_topic->getPartition(i);
                partition.writeBatch(std::move(m_partition_batches[i]));
                for (auto& [eid, s] : pending) s->set(eid);
            }
        }
    } catch (...) {
        // Ignore errors in destructor
    }
}

std::shared_ptr<diaspora::TopicHandleInterface> PfsProducer::topic() const {
    return m_topic;
}

diaspora::Future<std::optional<diaspora::Flushed>> PfsProducer::flush() {
    auto state = std::make_shared<FutureState<std::optional<diaspora::Flushed>>>();

    m_thread_pool->pushWork([this, topic=m_topic, state]() {
        try {
            // Write all pending batches and resolve their push futures.
            std::vector<std::vector<PendingPush>> all_pending(m_partition_batches.size());
            for (size_t i = 0; i < m_partition_batches.size(); ++i) {
                std::lock_guard<std::mutex> lock(m_batch_mutexes[i]);
                if (!m_partition_batches[i].empty()) {
                    all_pending[i] = std::move(m_partition_pending[i]);
                    m_partition_pending[i].clear();
                    topic->getPartition(i).writeBatch(std::move(m_partition_batches[i]));
                    m_partition_batches[i].clear();
                }
            }

            // Then fsync all partitions.
            for (size_t i = 0; i < topic->m_partitions.size(); ++i) {
                topic->getPartition(i).flush();
            }

            // Resolve push futures now that data is on disk.
            for (auto& partition_pending : all_pending)
                for (auto& [eid, s] : partition_pending) s->set(eid);

            state->set(diaspora::Flushed{});
        } catch(const diaspora::Exception& ex) {
            state->set(ex);
        }
    });

    return {
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
}

diaspora::Future<std::optional<diaspora::EventID>> PfsProducer::push(
        diaspora::Metadata metadata,
        diaspora::DataView data,
        std::optional<size_t> partition) {
    auto state = std::make_shared<FutureState<std::optional<diaspora::EventID>>>();

    m_thread_pool->pushWork(
        [this, topic=m_topic, state,
         metadata=std::move(metadata),
         data=std::move(data),
         partition]() {
        try {
            // Validation
            topic->validator().validate(metadata, data);

            // Partition selection
            auto partition_index = topic->m_partition_selector.selectPartitionFor(metadata, partition);

            uint64_t event_id;
            {
                std::lock_guard<std::mutex> lock(m_batch_mutexes[partition_index]);

                // Calculate what the event ID will be
                auto& partition_files = topic->getPartition(partition_index);
                event_id = partition_files.numEvents() + m_partition_batches[partition_index].size();

                // Add to batch. The future is stored as pending and resolved only
                // when the batch is written to disk (batch full here, or via flush()).
                m_partition_batches[partition_index].addEvent(
                    metadata,
                    topic->serializer(),
                    data
                );
                m_partition_pending[partition_index].push_back({event_id, state});

                bool is_adaptive = (m_batch_size == diaspora::BatchSize::Adaptive());
                if (is_adaptive || m_partition_batches[partition_index].size() >= m_batch_size.value) {
                    auto pending = std::move(m_partition_pending[partition_index]);
                    m_partition_pending[partition_index].clear();
                    partition_files.writeBatch(std::move(m_partition_batches[partition_index]));
                    m_partition_batches[partition_index].clear();
                    for (auto& [eid, s] : pending) s->set(eid);
                }
            }
            // Push future is resolved above (batch full) or deferred until flush().

        } catch(const diaspora::Exception& ex) {
            state->set(ex);
        }
    });

    return {
        [state](int timeout_ms) { return state->wait(timeout_ms); },
        [state] { return state->test(); }
    };
}

}

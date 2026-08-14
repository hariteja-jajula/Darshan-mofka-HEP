/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DEFAULT_PARTITION_SELECTOR_H
#define DIASPORA_API_DEFAULT_PARTITION_SELECTOR_H

#include "diaspora/Metadata.hpp"
#include "diaspora/PartitionSelector.hpp"
#include "diaspora/Json.hpp"
#include "JsonUtil.hpp"
#include <atomic>

namespace diaspora {

class DefaultPartitionSelector : public PartitionSelectorInterface {

    public:

    void setPartitions(const std::vector<PartitionInfo>& targets) override {
        m_targets = targets;
    }

    size_t selectPartitionFor(const Metadata& metadata, std::optional<size_t> requested) override {
        (void)metadata;
        if(m_targets.size() == 0)
            throw Exception("PartitionSelector has no target to select from");
        if(requested.has_value()) {
            size_t req = requested.value();
            return req % m_targets.size();
        }
        return m_index.fetch_add(1) % m_targets.size();
    }

    Metadata metadata() const override {
        return Metadata{"{\"type\":\"default\"}"};
    }

    static std::unique_ptr<PartitionSelectorInterface> create(const Metadata& metadata) {
        (void)metadata;
        return std::make_unique<DefaultPartitionSelector>();
    }

    std::atomic<size_t>        m_index{0};
    std::vector<PartitionInfo> m_targets;
};

}

#endif

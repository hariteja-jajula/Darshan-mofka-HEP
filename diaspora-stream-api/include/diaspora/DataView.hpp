/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_DATA_VIEW_HPP
#define DIASPORA_API_DATA_VIEW_HPP

#include <diaspora/ForwardDcl.hpp>
#include <diaspora/Exception.hpp>

#include <functional>
#include <memory>
#include <vector>
#include <numeric>
#include <cstring>
#include <utility>

namespace diaspora {

/**
 * @brief A DataView is an object that encapsulates the data of an event.
 */
class DataView {

    public:

    struct Segment {
        void*  ptr;
        size_t size;
    };

    using UserContext = void*;
    using FreeCallback = std::function<void(UserContext)>;

    /**
     * @brief Constructor. The resulting Data handle will represent NULL.
     */
    inline DataView(UserContext ctx = nullptr,
                    FreeCallback free_cb = FreeCallback{})
    : DataView(std::vector<Segment>(), ctx, std::move(free_cb)) {}

    /**
     * @brief Creates a Data object with a single segment.
     */
    inline DataView(void* ptr, size_t size,
                    UserContext ctx = nullptr,
                    FreeCallback free_cb = FreeCallback{})
    : DataView(std::vector{Segment{ptr, size}}, ctx, std::move(free_cb)) {}

    /**
     * @brief Creates a Data object from a list of Segments.
     */
    inline DataView(std::vector<Segment> segments,
                    UserContext ctx = nullptr,
                    FreeCallback free_cb = FreeCallback{})
    : m_segments(std::move(segments))
    , m_size(std::accumulate(m_segments.begin(), m_segments.end(), (size_t)0,
                             [](size_t sum, const Segment& seg) { return sum + seg.size; }))
    , m_context(ctx)
    , m_free(std::make_shared<FreeCallback>(std::move(free_cb))) {}

    /**
     * @brief Copy-constructor.
     */
    inline DataView(const DataView&) = default; // LCOV_EXCL_LINE

    /**
     * @brief Move-constructor.
     */
    inline DataView(DataView&& other)
    : m_segments(std::move(other.m_segments))
    , m_size{std::exchange(other.m_size, 0)}
    , m_context(std::move(other.m_context))
    , m_free(std::exchange(other.m_free, nullptr))
    {}

    /**
     * @brief Copy-assignment operator.
     */
    inline DataView& operator=(const DataView& other) {
        if(this == &other) return *this;
        if(m_free.use_count() == 1 && (*m_free)) {
            (*m_free)(m_context);
        }
        m_segments = other.m_segments;
        m_context = other.m_context;
        m_free = other.m_free;
        m_size = other.m_size;
        return *this;
    }

    /**
     * @brief Move-assignment operator.
     */
    inline DataView& operator=(DataView&& other) {
        if(this == &other) return *this;
        if(m_free.use_count() == 1 && (*m_free)) {
            (*m_free)(m_context);
        }
        m_segments = std::move(other.m_segments);
        m_context = std::move(other.m_context);
        m_free = std::exchange(other.m_free, nullptr);
        m_size = std::exchange(other.m_size, 0);
        return *this;
    }

    /**
     * @brief Free.
     */
    inline ~DataView() {
        if(m_free.use_count() == 1 && (*m_free)) {
            (*m_free)(m_context);
        }
    }

    /**
     * @brief Returns the list of memory segments
     * this Data object refers to.
     */
    inline const std::vector<Segment>& segments() const {
        return m_segments;
    }

    /**
     * @brief Return the total size of the Data.
     */
    inline size_t size() const {
        return m_size;
    }

    /**
     * @brief Write the content of target at the specified offset
     * into the memory represented by this Data object.
     *
     * @param data Data to write.
     * @param size Size of the data.
     * @param from_offset Offset from which to write.
     */
    inline size_t write(const char* source, size_t size, size_t offset = 0) {
        size_t source_offset = 0;
        for(auto& seg : segments()) {
            if(offset >= seg.size) {
                offset -= seg.size;
                continue;
            }
            // current segment needs to be copied
            auto size_to_copy = std::min(size, seg.size);
            std::memcpy((char*)seg.ptr + offset, source + source_offset, size_to_copy);
            offset = 0;
            source_offset += size_to_copy;
            size -= size_to_copy;
            if(size == 0) break;
        }
        return source_offset;
    }

    /**
     * @brief Read the content of the DataView into the provided buffer.
     *
     * @param data Destination.
     * @param size Size to read.
     * @param offset Offset at which to start in the DataView.
     */
    inline size_t read(char* dest, size_t size, size_t offset = 0) const {
        size_t dest_offset = 0;
        for(auto& seg : segments()) {
            if(offset >= seg.size) {
                offset -= seg.size;
                continue;
            }
            // current segment needs to be copied
            auto size_to_copy = std::min(size, seg.size);
            std::memcpy(dest + dest_offset, (char*)seg.ptr + offset, size_to_copy);
            offset = 0;
            dest_offset += size_to_copy;
            size -= size_to_copy;
            if(size == 0) break;
        }
        return dest_offset;
    }

    /**
     * @brief Return the context of this Data object.
     */
    inline UserContext context() const {
        return m_context;
    }

    private:

    std::vector<DataView::Segment>          m_segments;
    size_t                                  m_size = 0;
    DataView::UserContext                   m_context = nullptr;
    std::shared_ptr<DataView::FreeCallback> m_free;

};

}

#endif

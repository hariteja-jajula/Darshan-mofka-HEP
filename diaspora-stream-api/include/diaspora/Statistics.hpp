/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef DIASPORA_API_STATISTICS_HPP
#define DIASPORA_API_STATISTICS_HPP

#include <ostream>

namespace diaspora {

/**
 * @brief Statistics is a simple helper class to compute running statistics.
 *
 * @tparam Number
 * @tparam Double
 */
template<typename Number, typename Double=double>
struct Statistics {

    size_t num = 0;
    Number max = 0;
    Number min = 0;
    Double avg = 0;
    Double var = 0;

    void updateWith(Number value) {
        if(num == 0) min = value;
        auto n = num;
        if(max < value) max = value;
        if(min > value) min = value;
        Double wn = ((Double)n)/((Double)(n+1));
        Double w1 = 1.0/((Double)(n+1));
        Double avg_n = avg;
        Double var_n = var;
        avg = wn*avg_n + w1*value;
        var = wn*(var_n + avg_n*avg_n)
            + w1*value*value
            - avg*avg;
        num += 1;
    }

    template<typename Archive>
    void serialize(Archive& ar) {
        ar(num);
        ar(max);
        ar(min);
        ar(avg);
        ar(var);
    }
};

}

template<typename Number, typename Double=double>
inline std::ostream& operator<<(std::ostream& os, const diaspora::Statistics<Number, Double>& stats)
{
    return os << "{ \"num\" : " << stats.num
              << ", \"max\" : " << stats.max
              << ", \"min\" : " << stats.min
              << ", \"avg\" : " << stats.avg
              << ", \"var\" : " << stats.var
              << " }";
}

#endif

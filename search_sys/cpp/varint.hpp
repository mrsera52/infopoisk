#ifndef VARINT_HPP
#define VARINT_HPP

#include "vec.hpp"



#include <cstdint>
#include <utility>

namespace VarInt {

inline void encode(uint32_t n, DynArray<uint8_t>& out) {
    while (n >= 128u) {
        out.append(static_cast<uint8_t>((n & 0x7F) | 0x80));
        n >>= 7;
    }
    out.append(static_cast<uint8_t>(n & 0x7F));
}

inline std::pair<uint32_t, size_t> decode(const uint8_t* data, size_t pos) {
    uint32_t val   = 0;
    unsigned shift = 0;
    for (;;) {
        uint8_t b = data[pos++];
        val |= static_cast<uint32_t>(b & 0x7F) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return {val, pos};
}


inline void encode_deltas(const DynArray<int>& sorted, DynArray<uint8_t>& out) {
    int prev = 0;
    for (size_t i = 0; i < sorted.len; ++i) {
        encode(static_cast<uint32_t>(sorted[i] - prev), out);
        prev = sorted[i];
    }
}







}

#endif

#ifndef VEC_HPP
#define VEC_HPP

#include <cstdlib>
#include <cstring>
#include <stdexcept>




#include <string>
#include <utility>

template <typename T>
class DynArray {
public:
    T*     buf;
    size_t len;
    size_t cap;

    DynArray() : buf(nullptr), len(0), cap(0) {}

    DynArray(const DynArray& o) : buf(nullptr), len(0), cap(0) {
        if (o.len > 0) {
            cap = o.len;
            buf = static_cast<T*>(std::malloc(cap * sizeof(T)));
            if (!buf) throw std::bad_alloc();
            for (size_t i = 0; i < o.len; ++i)
                new (buf + i) T(o.buf[i]);
            len = o.len;
        }
    }

    DynArray(DynArray&& o) noexcept
        : buf(o.buf), len(o.len), cap(o.cap) {
        o.buf = nullptr;
        o.len = 0;
        o.cap = 0;
    }

    DynArray& operator=(const DynArray& o) {
        if (this != &o) {
            dispose();
            if (o.len > 0) {
                cap = o.len;
                buf = static_cast<T*>(std::malloc(cap * sizeof(T)));
                if (!buf) throw std::bad_alloc();
                for (size_t i = 0; i < o.len; ++i)
                    new (buf + i) T(o.buf[i]);
                len = o.len;
            }
        }
        return *this;
    }

    DynArray& operator=(DynArray&& o) noexcept {
        if (this != &o) {
            dispose();
            buf = o.buf; len = o.len; cap = o.cap;
            o.buf = nullptr; o.len = 0; o.cap = 0;
        }
        return *this;
    }

    ~DynArray() { dispose(); }

    void append(const T& v) {
        ensure_space();
        new (buf + len) T(v);
        ++len;
    }

    void append(T&& v) {
        ensure_space();
        new (buf + len) T(std::move(v));
        ++len;
    }

    T& operator[](size_t i) { return buf[i]; }
    const T& operator[](size_t i) const { return buf[i]; }

    T& at(size_t i) {
        if (i >= len) throw std::out_of_range("DynArray::at");
        return buf[i];
    }

    void dispose() {
        if (buf) {
            for (size_t i = 0; i < len; ++i) buf[i].~T();


            std::free(buf);
            buf = nullptr;
        }
        len = 0;
        cap = 0;
    }

    T* begin() { return buf; }
    T* end()   { return buf + len; }
    const T* begin() const { return buf; }
    const T* end()   const { return buf + len; }

private:
    void ensure_space() {
        if (len < cap) return;
        size_t nc = (cap == 0) ? 16 : cap * 2;
        T* nb = static_cast<T*>(std::malloc(nc * sizeof(T)));
        if (!nb) throw std::bad_alloc();
        if (buf) {
            for (size_t i = 0; i < len; ++i) {
                new (nb + i) T(std::move(buf[i]));
                buf[i].~T();
            }
            std::free(buf);
        }
        buf = nb;
        cap = nc;
    }
};

#endif

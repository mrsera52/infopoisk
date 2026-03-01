#ifndef MAP_HPP
#define MAP_HPP

#include "vec.hpp"
#include <string>
#include <utility>

template <typename V>
class HashDict {
private:
    struct Slot {
        std::string key;
        V           val;
        bool        used    = false;
        bool        erased  = false;

        Slot() = default;
        ~Slot() = default;

        Slot(const Slot&) = delete;
        Slot& operator=(const Slot&) = delete;

        Slot(Slot&& o) noexcept
            : key(std::move(o.key)), val(std::move(o.val)),
              used(o.used), erased(o.erased) {
            o.used = false;
        }

        Slot& operator=(Slot&& o) noexcept {
            if (this != &o) {
                key    = std::move(o.key);
                val    = std::move(o.val);
                used   = o.used;
                erased = o.erased;
                o.used = false;
            }
            return *this;
        }
    };

    Slot*  slots;
    size_t cap;
    size_t cnt;

    size_t fnv(const std::string& k) const {
        size_t h = 14695981039346656037ULL;
        for (unsigned char c : k) {
            h ^= c;
            
            h *= 1099511628211ULL;
        }
        return h;
    }

    void grow() {
        size_t old_cap = cap;
        Slot*  old     = slots;
        cap  = (cap == 0) ? 32 : cap * 2;
        slots = new Slot[cap];
        cnt   = 0;
        for (size_t i = 0; i < old_cap; ++i) {
            if (old[i].used && !old[i].erased)
                put_internal(std::move(old[i].key), std::move(old[i].val));
        }
        delete[] old;
    }

    void put_internal(std::string&& k, V&& v) {
        size_t idx = fnv(k) % cap;
        while (slots[idx].used)
            idx = (idx + 1) % cap;


        slots[idx].key    = std::move(k);
        slots[idx].val    = std::move(v);
        slots[idx].used   = true;
        slots[idx].erased = false;
        ++cnt;
    }

public:
    HashDict() : slots(nullptr), cap(0), cnt(0) {}

    ~HashDict() { if (slots) delete[] slots; }

    void preallocate(size_t n) {
        if (cap > 0 && n <= static_cast<size_t>(cap * 0.7)) return;
        size_t nc = cap == 0 ? 32 : cap;
        while (nc < static_cast<size_t>(n / 0.7) + 1) nc *= 2;
        if (nc <= cap) return;
        size_t old_cap = cap;
        Slot*  old     = slots;
        cap   = nc;
        slots = new Slot[cap];
        cnt   = 0;
        for (size_t i = 0; i < old_cap; ++i) {
            if (old[i].used && !old[i].erased)
                put_internal(std::move(old[i].key), std::move(old[i].val));
        }
        if (old) delete[] old;
    }

    void put(const std::string& k, const V& v) {

        if (cnt >= static_cast<size_t>(cap * 0.7)) grow();
        size_t idx = fnv(k) % cap;
        while (slots[idx].used) {
            if (slots[idx].key == k && !slots[idx].erased) {
                slots[idx].val = v;
                return;
            }
            idx = (idx + 1) % cap;




                     }
        slots[idx].key    = k;
        slots[idx].val    = v;
        slots[idx].used   = true;
        slots[idx].erased = false;
        ++cnt;
    }

    void put(const std::string& k, V&& v) {
        if (cnt >= static_cast<size_t>(cap * 0.7)) grow();
        size_t idx = fnv(k) % cap;
        while (slots[idx].used) {
            if (slots[idx].key == k && !slots[idx].erased) {
                slots[idx].val = std::move(v);
                return;
            }
            idx = (idx + 1) % cap;
        }
        slots[idx].key    = k;
        slots[idx].val    = std::move(v);
        slots[idx].used   = true;
        slots[idx].erased = false;
        ++cnt;
    }

    V* lookup(const std::string& k) {
        if (cap == 0) return nullptr;
        size_t idx   = fnv(k) % cap;
        size_t start = idx;
        while (slots[idx].used) {
            if (slots[idx].key == k && !slots[idx].erased)
                return &slots[idx].val;
            idx = (idx + 1) % cap;
            if (idx == start) break;
        }
        return nullptr;
    }

    V& operator[](const std::string& k) {
        V* p = lookup(k);
        if (p) return *p;
        if (cnt >= static_cast<size_t>(cap * 0.7)) grow();
        size_t idx = fnv(k) % cap;
        while (slots[idx].used)
            idx = (idx + 1) % cap;
        slots[idx].key    = k;
        slots[idx].used   = true;
        slots[idx].erased = false;
        ++cnt;
        return slots[idx].val;
    }

    class Iter {
    public:
        Slot* p;
        Slot* e;
        Iter(Slot* pp, Slot* ee) : p(pp), e(ee) {
            while (p < e && (!p->used || p->erased)) ++p;
        }
        bool operator!=(const Iter& o) const { return p != o.p; }
        void operator++() {
            do { ++p; } while (p < e && (!p->used || p->erased));
        }
        Slot& operator*()  { return *p; }
        Slot* operator->() { return p;  }
    };

    Iter begin() { return Iter(slots, slots + cap); }
    Iter end()   { return Iter(slots + cap, slots + cap); }
};

#endif

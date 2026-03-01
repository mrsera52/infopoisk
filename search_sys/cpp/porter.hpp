#ifndef PORTER_HPP
#define PORTER_HPP

#include <string>
#include <cctype>

#include "vec.hpp"

namespace Porter {

static bool vowel_at(const std::string& w, int i) {
    char c = w[static_cast<size_t>(i)];
    switch (c) {
        case 'a': case 'e': case 'i': case 'o': case 'u': return true;
        
    case 'y': return (i == 0) ? false : !vowel_at(w, i - 1);
        default: return false;
    }
}

static int calc_m(const std::string& w) {
    int n = 0, i = 0, sz = static_cast<int>(w.size());
    while (i < sz&& !vowel_at(w, i)) ++i;
    while (i < sz) {
        while (i < sz && vowel_at(w, i)) ++i;
        if (i >= sz) break;
        while (i < sz && !vowel_at(w, i)) ++i;
        ++n;
    }
    return n;
}

static bool has_vowel(const std::string& w) {
    for (int i = 0; i < static_cast<int>(w.size()); ++i)
        if (vowel_at(w, i)) return true;
    return false;
}

static bool tail_eq(const std::string& w, const std::string& s) {
    if (w.size() < s.size()) return false;
    return w.compare(w.size() - s.size(), s.size(), s) == 0;
}

static std::string chop(const std::string& w, const std::string& s) {
    return w.substr(0, w.size() - s.size());
}

static bool dbl_cons(const std::string& w) {
    if (w.size() < 2) return false;
    if (w.back() != w[w.size() - 2]) return false;
    return !vowel_at(w, static_cast<int>(w.size()) - 1);
}

static bool ends_cvc(const std::string& w) {
    if (w.size() < 3) return false;
    int n = static_cast<int>(w.size());
    if (vowel_at(w, n-1) || !vowel_at(w, n-2) || vowel_at(w, n-3)) return false;
    char last = w.back();
    return last != 'w' && last != 'x' && last != 'y';
}

static void swap_tail(std::string& w, const std::string& old_s, const std::string& new_s) {
    w.erase(w.size() - old_s.size());
    w += new_s;
}

static void phase1a(std::string& w) {
    if (tail_eq(w, "sses"))      swap_tail(w, "sses", "ss");
    else if (tail_eq(w, "ies"))  swap_tail(w, "ies", "i");
    else if (tail_eq(w, "ss"))   {}
    else if (tail_eq(w, "s"))    w.pop_back();
}

static void phase1b(std::string& w) {
    if (tail_eq(w, "eed")) {
        std::string s = chop(w, "eed");
        if (calc_m(s) > 0) swap_tail(w, "eed", "ee");
        return;
    }


    bool cut = false;
    std::string s;
    if (tail_eq(w, "ed")) {
        s = chop(w, "ed");
        if (has_vowel(s)) { w = s; cut = true; }
    } else if (tail_eq(w, "ing")) {
        s = chop(w, "ing");
        if (has_vowel(s)) { w = s; cut = true; }
    }
    if (cut) {
        if (tail_eq(w, "at") || tail_eq(w, "bl") || tail_eq(w, "iz"))
            w += "e";
        else if (dbl_cons(w)) {
            char c = w.back();
            if (c != 'l' && c != 's' && c != 'z') w.pop_back();
        }
        else if (calc_m(w) == 1 && ends_cvc(w))
            w += "e";
    }
}

static void phase1c(std::string& w) {
    if (tail_eq(w, "y")) {
        std::string s = chop(w, "y");
        if (has_vowel(s)) w.back() = 'i';
    }
}

static void phase2(std::string& w) {
    struct R { const char* from; const char* to; };
    static const R rules[] = {
        {"ational","ate"}, {"tional","tion"}, {"enci","ence"}, {"anci","ance"},
        {"izer","ize"}, {"abli","able"}, {"alli","al"}, {"entli","ent"},
        {"eli","e"}, {"ousli","ous"}, {"ization","ize"}, {"ation","ate"},
        {"ator","ate"}, {"alism","al"}, {"iveness","ive"}, {"fulness","ful"},
        {"ousness","ous"}, {"aliti","al"}, {"iviti","ive"}, {"biliti","ble"}
    };
    for (auto& r : rules) {
        if (tail_eq(w, r.from)) {
            std::string s = chop(w, r.from);
            if (calc_m(s) > 0) swap_tail(w, r.from, r.to);
            return;
        }
    }
}

static void phase3(std::string& w) {
    struct R { const char* from; const char* to; };
    static const R rules[] = {
        {"icate","ic"}, {"ative",""}, {"alize","al"}, {"iciti","ic"},
        {"ical","ic"}, {"ful",""}, {"ness",""}
    };

    for (auto& r : rules) {
        if (tail_eq(w, r.from)) {
            std::string s = chop(w, r.from);
            if (calc_m(s) > 0) swap_tail(w, r.from, r.to);
            return;
        }
    }
}

static void phase4(std::string& w) {
    static const char* suffixes[] = {
        "al","ance","ence","er","ic","able","ible","ant","ement","ment","ent",
        "ou","ism","ate","iti","ous","ive","ize"
    };
    for (const char* s : suffixes) {
        if (tail_eq(w, s)) {
            std::string base = chop(w, s);
            if (calc_m(base) > 1) w = base;
            return;
        }
    }
    if (tail_eq(w, "ion")) {
        std::string base = chop(w, "ion");
        if (!base.empty()) {
            char prev = base.back();
            if ((prev == 's' || prev == 't') && calc_m(base) > 1) w = base;
        }
    }
}

static void phase5(std::string& w) {
    if (tail_eq(w, "e")) {
        std::string base = chop(w, "e");
        int m = calc_m(base);

        if (m >1 || (m == 1 && !ends_cvc(base))) w = base;
    }
    if (calc_m(w) > 1 && tail_eq(w, "ll")) w.pop_back();
}

inline std::string stem(std::string w) {
    if (w.size() <= 2) return w;
        phase1a(w); phase1b(w); phase1c(w);
     phase2(w);  phase3(w);  phase4(w); phase5(w);
    return w;
}

inline void tokenize(const std::string& text, DynArray<std::string>& out) {
    std::string tok;
    for (char ch : text) {
        unsigned char uc = static_cast<unsigned char>(ch);
        if (std::isalnum(uc)) {

            tok.push_back(static_cast<char>(std::tolower(uc)));
        } else if (!tok.empty()) {
            out.append(stem(tok));
            tok.clear();
        }
    }
    if (!tok.empty())
        out.append(stem(tok));
}

}

#endif

#include <cctype>
#include <iostream>
#include <string>

namespace {

bool is_vowel(const std::string& w, int i) {
    char c = w[static_cast<size_t>(i)];
    switch (c) {
        case 'a': case 'e': case 'i': case 'o': case 'u': return true;
        case 'y': return (i > 0) && !is_vowel(w, i - 1);
        default: return false;
    }
}






//
int m_value(const std::string& w) {
    int n = 0, i = 0, sz = static_cast<int>(w.size());
    while (i < sz && !is_vowel(w, i)) ++i;
    while (i < sz) {
        while (i < sz && is_vowel(w, i)) ++i;
        if (i >= sz) break;
        while (i < sz && !is_vowel(w, i)) ++i;
        ++n;
    }
    return n;
}

bool vowel_exists(const std::string& w) {
    for (int i = 0; i < static_cast<int>(w.size()); ++i)

        if (is_vowel(w, i)) return true;
    return false;
}

bool str_ends(const std::string& w, const std::string& sfx) {
    if (w.size() < sfx.size()) return false;
    return w.compare(w.size() - sfx.size(), sfx.size(), sfx) == 0;
}

std::string cut_suffix(const std::string& w, const std::string& sfx) {
    return w.substr(0, w.size() - sfx.size());
}

bool double_c(const std::string& w) {
    if (w.size() < 2) return false;
    return w.back() == w[w.size()-2] && !is_vowel(w, static_cast<int>(w.size())-1);
}

bool o_form(const std::string& w) {
    int n = static_cast<int>(w.size());
    if (n < 3) return false;
    if (is_vowel(w, n-1) || !is_vowel(w, n-2) || is_vowel(w, n-3)) return false;
    char c = w.back();
    return c != 'w' && c != 'x' && c != 'y';
}

void set_end(std::string& w, const std::string& old_s, const std::string& new_s) {
    w.erase(w.size() - old_s.size());
    w += new_s;
}

void s1a(std::string& w) {
    if (str_ends(w, "sses"))     set_end(w, "sses", "ss");
    else if (str_ends(w, "ies")) set_end(w, "ies", "i");
    else if (str_ends(w, "ss"))  {}
    else if (str_ends(w, "s"))   w.pop_back();
}

void s1b(std::string& w) {
    if (str_ends(w, "eed")) {
        if (m_value(cut_suffix(w, "eed")) > 0) set_end(w, "eed", "ee");
        return;
    }
    bool did = false;
    std::string base;
    if (str_ends(w, "ed")) {
        base = cut_suffix(w, "ed");
        if (vowel_exists(base)) { w = base; did = true; }
    } else if (str_ends(w, "ing")) {
        base = cut_suffix(w, "ing");
        if (vowel_exists(base)) { w = base; did = true; }
    }
    if (did) {
        if (str_ends(w, "at") || str_ends(w, "bl") || str_ends(w, "iz"))
            w += "e";
        else if (double_c(w)) {
            char c = w.back();
                if (c != 'l' && c != 's' && c != 'z') w.pop_back();


        } else if (m_value(w) == 1 && o_form(w))
            w += "e";
    }
}

void s1c(std::string& w) {
    if (str_ends(w, "y") && vowel_exists(cut_suffix(w, "y")))
        
    w.back() = 'i';

}

void s2(std::string& w) {
    struct P { const char* a; const char* b; };
    static const P tbl[] = {
        {"ational","ate"},{"tional","tion"},{"enci","ence"},{"anci","ance"},
        {"izer","ize"},{"abli","able"},{"alli","al"},{"entli","ent"},
        {"eli","e"},{"ousli","ous"},{"ization","ize"},{"ation","ate"},
        {"ator","ate"},{"alism","al"},{"iveness","ive"},{"fulness","ful"},
        {"ousness","ous"},{"aliti","al"},{"iviti","ive"},{"biliti","ble"}
    };
    for (auto& p : tbl) {
        if (str_ends(w, p.a)) {
            if (m_value(cut_suffix(w, p.a)) > 0) set_end(w, p.a, p.b);
            return;
        }
    }
}

void s3(std::string& w) {
    struct P { const char* a; const char* b; };
    static const P tbl[] = {
        {"icate","ic"},{"ative",""},{"alize","al"},{"iciti","ic"},
        {"ical","ic"},{"ful",""},{"ness",""}
    };
    for (auto& p : tbl) {
        if (str_ends(w, p.a)) {
            if (m_value(cut_suffix(w, p.a)) > 0) set_end(w, p.a, p.b);
            return;
        }
    }
}

void s4(std::string& w) {
    static const char* list[] = {
        "al","ance","ence","er","ic","able","ible","ant","ement","ment","ent",
        "ou","ism","ate","iti","ous","ive","ize"
    };
    for (const char* s : list) {
        if (str_ends(w, s)) {
            if (m_value(cut_suffix(w, s)) > 1) w = cut_suffix(w, s);
            return;
        }
    }
    if (str_ends(w, "ion")) {
        std::string base = cut_suffix(w, "ion");
        if (!base.empty()) {
            char p = base.back();
            if ((p == 's' || p == 't') && m_value(base) > 1) w = base;
        }
    }
}

void s5(std::string& w) {
    if (str_ends(w, "e")) {
        std::string base = cut_suffix(w, "e");
        int m = m_value(base);
        if (m > 1 || (m == 1 && !o_form(base))) w = base;
    }
    if (m_value(w) > 1 && str_ends(w, "ll")) w.pop_back();
}

std::string do_stem(std::string w) {
    if (w.size() <= 2) return w;
    s1a(w); s1b(w); s1c(w); s2(w); s3(w); s4(w); s5(w);
    return w;
}

bool is_alnum(unsigned char c) { return std::isalnum(c) != 0; }

void emit(std::string& tok) {
    if (tok.empty()) return;
    std::string out = do_stem(tok);
    if (!out.empty()) std::cout << out << '\n';
    tok.clear();
}

}

int main() {
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    std::string line;
    while (std::getline(std::cin, line)) {
        std::string tok;
        for (char ch : line) {
            unsigned char uc = static_cast<unsigned char>(ch);
            if (is_alnum(uc))
                tok.push_back(static_cast<char>(std::tolower(uc)));
            else
                emit(tok);
        }
        emit(tok);
        std::cout << "__END_DOC__" << std::endl;
    }
    return 0;
}

#include <iostream>
#include <string>
#include <fstream>
#include <cmath>
#include <algorithm>

#include "vec.hpp"
#include "map.hpp"
#include "varint.hpp"
#include "porter.hpp"

struct TermMeta {
    uint64_t offset;
    uint32_t doc_freq;
};

struct DocRecord {
    std::string url;
    std::string title;
    uint32_t    length;
};

static HashDict<TermMeta> dict;
static DynArray<DocRecord> docs;
static std::string blob;
static uint32_t total_docs = 0;
static double   avg_doc_len = 0;

void load_forward(const std::string& dir) {
    std::string path = dir + "/index.fwd";
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("Cannot open " + path);

    char magic[4]; f.read(magic, 4);
    uint16_t ver;  f.read(reinterpret_cast<char*>(&ver), 2);
    uint32_t n;    f.read(reinterpret_cast<char*>(&n), 4);

    DynArray<uint64_t> offs;
    for (uint32_t i = 0; i < n; ++i) {
        uint64_t o; f.read(reinterpret_cast<char*>(&o), 8);
        offs.append(o);
    }

    double sum_len = 0;
    for (uint32_t i = 0; i < n; ++i) {
        f.seekg(offs[i]);
        uint16_t ul; f.read(reinterpret_cast<char*>(&ul), 2);
        std::string url(ul, '\0'); f.read(&url[0], ul);

        uint16_t tl; f.read(reinterpret_cast<char*>(&tl), 2);
        std::string title(tl, '\0'); f.read(&title[0], tl);

        uint32_t dl; f.read(reinterpret_cast<char*>(&dl), 4);
        sum_len += dl;

        DocRecord dr;
        dr.url = std::move(url);
        dr.title = std::move(title);
        dr.length = dl;
        docs.append(std::move(dr));
    }
    total_docs = n;
    avg_doc_len = (n > 0) ? sum_len / n : 1.0;
}

void load_terms(const std::string& dir) {
    std::string dpath = dir + "/index.term";
    std::ifstream fd(dpath, std::ios::binary);
    if (!fd) throw std::runtime_error("Cannot open " + dpath);

    char magic[4]; fd.read(magic, 4);
    uint16_t ver;  fd.read(reinterpret_cast<char*>(&ver), 2);
    uint32_t cnt;  fd.read(reinterpret_cast<char*>(&cnt), 4);

    dict.preallocate(cnt);
    for (uint32_t i = 0; i < cnt; ++i) {
        uint8_t len; fd.read(reinterpret_cast<char*>(&len), 1);
        std::string term(len, '\0'); fd.read(&term[0], len);
        uint64_t off; fd.read(reinterpret_cast<char*>(&off), 8);
        uint32_t df;  fd.read(reinterpret_cast<char*>(&df), 4);
        TermMeta tm; tm.offset = off; tm.doc_freq = df;
        dict.put(term, std::move(tm));
    }
    fd.close();

    std::string bpath = dir + "/index.data";
    std::ifstream fb(bpath, std::ios::binary | std::ios::ate);
    if (!fb) throw std::runtime_error("Cannot open " + bpath);
    size_t sz = static_cast<size_t>(fb.tellg());
    fb.seekg(0);
    blob.resize(sz);
    fb.read(&blob[0], sz);
    fb.close();
}

void load_index(const std::string& dir) {
    load_forward(dir);
    load_terms(dir);
}

struct Posting {
    int doc_id;
    DynArray<int> positions;
};

DynArray<Posting> decode_postings(const std::string& term) {
    DynArray<Posting> result;
    TermMeta* tm = dict.lookup(term);
    if (!tm) return result;

    const uint8_t* d = reinterpret_cast<const uint8_t*>(blob.data());
    size_t pos = tm->offset;

    auto [df, p1] = VarInt::decode(d, pos); pos = p1;

    auto [num_skips, p2] = VarInt::decode(d, pos); pos = p2;
    for (uint32_t s = 0; s < num_skips; ++s) {
        auto [_a, p3] = VarInt::decode(d, pos); pos = p3;
        auto [_b, p4] = VarInt::decode(d, pos); pos = p4;
    }

    int cur_doc = 0;
    for (uint32_t i = 0; i < df; ++i) {
        auto [delta, pa] = VarInt::decode(d, pos); pos = pa;
        cur_doc += static_cast<int>(delta);

        auto [freq, pb] = VarInt::decode(d, pos); pos = pb;

        Posting pt;
        pt.doc_id = cur_doc;
        int cur_pos = 0;
        for (uint32_t j = 0; j < freq; ++j) {
            auto [pd, pc] = VarInt::decode(d, pos); pos = pc;
            cur_pos += static_cast<int>(pd);
            pt.positions.append(cur_pos);
        }
        result.append(std::move(pt));
    }
    return result;
}

struct SkipEntry {
    int      doc_id;
    uint32_t byte_off;
};

struct TermPostingStream {
    uint32_t df;
    DynArray<SkipEntry> skips;
    size_t entries_start;
};

TermPostingStream get_posting_meta(const std::string& term) {
    TermPostingStream tps;
    tps.df = 0;
    tps.entries_start = 0;
    TermMeta* tm = dict.lookup(term);
    if (!tm) return tps;

    const uint8_t* d = reinterpret_cast<const uint8_t*>(blob.data());
    size_t pos = tm->offset;

    auto [df, p1] = VarInt::decode(d, pos); pos = p1;
    tps.df = df;

    auto [num_skips, p2] = VarInt::decode(d, pos); pos = p2;
    for (uint32_t s = 0; s < num_skips; ++s) {
        auto [sdoc, p3] = VarInt::decode(d, pos); pos = p3;
        auto [soff, p4] = VarInt::decode(d, pos); pos = p4;
        SkipEntry se;
        se.doc_id  = static_cast<int>(sdoc);
        se.byte_off = soff;
        tps.skips.append(se);
    }
    tps.entries_start = pos;
    return tps;
}

DynArray<int> get_doc_ids(const std::string& term) {
    DynArray<int> res;
    TermMeta* tm = dict.lookup(term);
    if (!tm) return res;

    const uint8_t* d = reinterpret_cast<const uint8_t*>(blob.data());
    size_t pos = tm->offset;

    auto [df, p1] = VarInt::decode(d, pos); pos = p1;

    auto [num_skips, p2] = VarInt::decode(d, pos); pos = p2;
    for (uint32_t s = 0; s < num_skips; ++s) {
        auto [_a, p3] = VarInt::decode(d, pos); pos = p3;
        auto [_b, p4] = VarInt::decode(d, pos); pos = p4;
    }

    int cur_doc = 0;
    for (uint32_t i = 0; i < df; ++i) {
        auto [delta, pa] = VarInt::decode(d, pos); pos = pa;
        cur_doc += static_cast<int>(delta);
        res.append(cur_doc);

        auto [freq, pb] = VarInt::decode(d, pos); pos = pb;
        for (uint32_t j = 0; j < freq; ++j) {
            auto [_c, pc] = VarInt::decode(d, pos); pos = pc;
        }
    }
    return res;
}

DynArray<int> merge_or(const DynArray<int>& a, const DynArray<int>& b) {
    DynArray<int> r;
    size_t i = 0, j = 0;
    while (i < a.len && j < b.len) {
        if (a[i] < b[j])      r.append(a[i++]);
        else if (a[i] > b[j]) r.append(b[j++]);
        else { r.append(a[i]); ++i; ++j; }
    }
    while (i < a.len) r.append(a[i++]);
    while (j < b.len) r.append(b[j++]);
    return r;
}

DynArray<int> merge_and(const DynArray<int>& a, const DynArray<int>& b) {
    DynArray<int> r;
    size_t i = 0, j = 0;
    while (i < a.len && j < b.len) {
        if (a[i] < b[j])      ++i;
        else if (a[i] > b[j]) ++j;
        else { r.append(a[i]); ++i; ++j; }
    }
    return r;
}

DynArray<int> merge_and_skip(const DynArray<int>& a,
                             const TermPostingStream& b_meta) {
    DynArray<int> result;
    if (a.len == 0 || b_meta.df == 0) return result;

    const uint8_t* d = reinterpret_cast<const uint8_t*>(blob.data());
    size_t a_idx = 0;

    size_t b_pos = b_meta.entries_start;
    int b_doc = 0;
    uint32_t b_read = 0;

    while (a_idx < a.len && b_read < b_meta.df) {
        int target = a[a_idx];

        if (b_doc < target && b_meta.skips.len > 0) {
            int best_skip = -1;
            for (size_t s = 0; s < b_meta.skips.len; ++s) {
                uint32_t skip_index = static_cast<uint32_t>((s + 1) * 128);
                if (skip_index > b_read && b_meta.skips[s].doc_id <= target) {
                    best_skip = static_cast<int>(s);
                }
            }
            if (best_skip >= 0) {
                uint32_t skip_index = static_cast<uint32_t>((best_skip + 1) * 128);
                b_pos = b_meta.entries_start + b_meta.skips[best_skip].byte_off;
                b_doc = b_meta.skips[best_skip].doc_id;
                b_read = skip_index;

                auto [delta, pa] = VarInt::decode(d, b_pos); b_pos = pa;
                auto [freq, pb] = VarInt::decode(d, b_pos); b_pos = pb;
                for (uint32_t k = 0; k < freq; ++k) {
                    auto [_p, pc] = VarInt::decode(d, b_pos); b_pos = pc;
                }

                if (b_doc == target) {
                    result.append(b_doc);
                    ++a_idx;
                    ++b_read;
                    continue;
                }
                ++b_read;
            }
        }

        while (b_read < b_meta.df && b_doc < target) {
            auto [delta, pa] = VarInt::decode(d, b_pos); b_pos = pa;
            b_doc += static_cast<int>(delta);
            auto [freq, pb] = VarInt::decode(d, b_pos); b_pos = pb;
            for (uint32_t k = 0; k < freq; ++k) {
                auto [_p, pc] = VarInt::decode(d, b_pos); b_pos = pc;
            }
            ++b_read;
        }

        if (b_doc == target) {
            result.append(b_doc);
            ++a_idx;
        } else if (b_doc > target) {
            while (a_idx < a.len && a[a_idx] < b_doc) ++a_idx;
            if (a_idx < a.len && a[a_idx] == b_doc) {
                result.append(b_doc);
                ++a_idx;
            }
        } else {
            break;
        }
    }
    return result;
}

DynArray<int> merge_not(const DynArray<int>& a, const DynArray<int>& b) {
    DynArray<int> r;
    size_t i = 0, j = 0;
    while (i < a.len && j < b.len) {
        if (a[i] < b[j])      { r.append(a[i]); ++i; }
        else if (a[i] > b[j]) ++j;
        else { ++i; ++j; }
    }
    while (i < a.len) r.append(a[i++]);
    return r;
}

DynArray<int> all_doc_ids() {
    DynArray<int> r;
    for (uint32_t i = 0; i < total_docs; ++i)
        r.append(static_cast<int>(i));
    return r;
}

bool find_sequence(DynArray<int>* pos_lists, int count, int idx,
                   int prev_pos, int first_pos, int max_dist, bool exact) {
    if (idx == count) return true;
    for (size_t k = 0; k < pos_lists[idx].len; ++k) {
        int p = pos_lists[idx][k];
        if (idx == 0) {
            if (find_sequence(pos_lists, count, 1, p, p, max_dist, exact))
                return true;
        } else if (p > prev_pos) {
            if (exact && p != prev_pos + 1) continue;
            if (p - first_pos > max_dist) continue;
            if (find_sequence(pos_lists, count, idx + 1, p, first_pos, max_dist, exact))
                return true;
        }
    }
    return false;
}

DynArray<int> phrase_search(DynArray<std::string>& terms, int max_dist) {
    DynArray<int> result;
    if (terms.len == 0) return result;

    DynArray<DynArray<Posting>> all_postings;
    for (size_t i = 0; i < terms.len; ++i)
        all_postings.append(decode_postings(terms[i]));

    if (all_postings[0].len == 0) return result;

    DynArray<int> common;
    for (size_t i = 0; i < all_postings[0].len; ++i)
        common.append(all_postings[0][i].doc_id);

    for (size_t t = 1; t < terms.len; ++t) {
        DynArray<int> other;
        for (size_t i = 0; i < all_postings[t].len; ++i)
            other.append(all_postings[t][i].doc_id);
        common = merge_and(common, other);
    }

    bool exact = (max_dist == static_cast<int>(terms.len));
    for (size_t ci = 0; ci < common.len; ++ci) {
        int doc = common[ci];
        DynArray<int>* pos_arrays = new DynArray<int>[terms.len];

        for (size_t t = 0; t < terms.len; ++t) {
            for (size_t k = 0; k < all_postings[t].len; ++k) {
                if (all_postings[t][k].doc_id == doc) {
                    pos_arrays[t] = all_postings[t][k].positions;
                    break;
                }
            }
        }

        if (find_sequence(pos_arrays, static_cast<int>(terms.len),
                          0, -1, -1, max_dist, exact))
            result.append(doc);

        delete[] pos_arrays;
    }
    return result;
}

struct ScoredDoc {
    int    doc_id;
    double score;
};

void rank_by_tfidf(DynArray<std::string>& terms, DynArray<ScoredDoc>& scored) {
    HashDict<double> doc_scores;

    for (size_t t = 0; t < terms.len; ++t) {
        DynArray<Posting> postings = decode_postings(terms[t]);
        double idf = 0.0;
        if (postings.len > 0)
            idf = std::log(static_cast<double>(total_docs) / postings.len);

        for (size_t i = 0; i < postings.len; ++i) {
            int did = postings[i].doc_id;
            double tf = static_cast<double>(postings[i].positions.len);
            double norm_tf = (tf > 0) ? (1.0 + std::log(tf)) : 0.0;
            std::string key = std::to_string(did);
            doc_scores[key] += norm_tf * idf;
        }
    }

    for (auto it = doc_scores.begin(); it != doc_scores.end(); ++it) {
        ScoredDoc sd;
        sd.doc_id = std::stoi(it->key);
        sd.score  = it->val;
        scored.append(sd);
    }

    for (size_t i = 1; i < scored.len; ++i) {
        ScoredDoc tmp = scored[i];
        size_t j = i;
        while (j > 0 && scored[j-1].score < tmp.score) {
            scored[j] = scored[j-1];
            --j;
        }
        scored[j] = tmp;
    }
}

enum TokKind { T_WORD, T_AND, T_OR, T_NOT, T_LP, T_RP, T_END };

struct QTok {
    TokKind     kind;
    std::string text;
};

DynArray<QTok> lex(const std::string& q) {
    DynArray<QTok> out;
    size_t i = 0, n = q.size();
    while (i < n) {
        if (q[i] == ' ' || q[i] == '\t') { ++i; continue; }
        if (q[i] == '(') { QTok t; t.kind = T_LP; out.append(t); ++i; continue; }
        if (q[i] == ')') { QTok t; t.kind = T_RP; out.append(t); ++i; continue; }
        if (q[i] == '!') { QTok t; t.kind = T_NOT; out.append(t); ++i; continue; }
        if (i+1 < n && q[i] == '&' && q[i+1] == '&') {
            QTok t; t.kind = T_AND; out.append(t); i += 2; continue;
        }
        if (i+1 < n && q[i] == '|' && q[i+1] == '|') {
            QTok t; t.kind = T_OR; out.append(t); i += 2; continue;
        }
        if (q[i] == '"' || (i+1 < n && (unsigned char)q[i] == 0xC2 && (unsigned char)q[i+1] == 0xAB)) {
            size_t start;
            if (q[i] == '"') { start = i + 1; }
            else { start = i + 2; }
            size_t end_pos = start;
            while (end_pos < n) {
                if (q[end_pos] == '"') break;
                if (end_pos + 1 < n && (unsigned char)q[end_pos] == 0xC2 && (unsigned char)q[end_pos+1] == 0xBB) break;
                ++end_pos;
            }
            QTok t; t.kind = T_WORD;
            t.text = "QUOTE:" + q.substr(start, end_pos - start);
            out.append(t);
            i = end_pos;
            if (i < n && q[i] == '"') ++i;
            else if (i+1 < n && (unsigned char)q[i] == 0xC2 && (unsigned char)q[i+1] == 0xBB) i += 2;
            while (i < n && q[i] == ' ') ++i;
            if (i < n && q[i] == '/') {
                ++i;
                while (i < n && q[i] == ' ') ++i;
                size_t ns = i;
                while (i < n && q[i] >= '0' && q[i] <= '9') ++i;
                if (i > ns) {
                    out[out.len - 1].text += "/" + q.substr(ns, i - ns);
                }
            }
            continue;
        }
        size_t ws = i;
        while (i < n && q[i] != ' ' && q[i] != '\t' && q[i] != '(' && q[i] != ')' &&
               q[i] != '!' && !(i+1 < n && q[i] == '&' && q[i+1] == '&') &&
               !(i+1 < n && q[i] == '|' && q[i+1] == '|'))
            ++i;
        QTok t; t.kind = T_WORD; t.text = q.substr(ws, i - ws); out.append(t);
    }
    QTok e; e.kind = T_END; out.append(e);
    return out;
}

bool has_bool_ops(const DynArray<QTok>& tokens) {
    for (size_t i = 0; i < tokens.len; ++i)
        if (tokens[i].kind == T_AND || tokens[i].kind == T_OR || tokens[i].kind == T_NOT)
            return true;
    return false;
}

class BoolParser {
    DynArray<QTok>* toks;
    size_t pos;

    DynArray<int> parse_atom() {
        if ((*toks)[pos].kind == T_NOT) {
            ++pos;
            DynArray<int> inner = parse_atom();
            return merge_not(all_doc_ids(), inner);
        }
        if ((*toks)[pos].kind == T_LP) {
            ++pos;
            DynArray<int> inner = parse_or_expr();
            if ((*toks)[pos].kind == T_RP) ++pos;
            return inner;
        }
        if ((*toks)[pos].kind == T_WORD) {
            std::string w = (*toks)[pos].text;
            ++pos;

            if (w.rfind("QUOTE:", 0) == 0) {
                std::string content = w.substr(6);
                int max_dist = -1;
                size_t slash = content.rfind('/');
                if (slash != std::string::npos) {
                    std::string num_str = content.substr(slash + 1);
                    max_dist = 0;
                    for (char c : num_str) max_dist = max_dist * 10 + (c - '0');
                    content = content.substr(0, slash);
                }
                DynArray<std::string> terms;
                Porter::tokenize(content, terms);
                if (terms.len == 0) return DynArray<int>();
                if (max_dist < 0) max_dist = static_cast<int>(terms.len);
                return phrase_search(terms, max_dist);
            }

            DynArray<std::string> stems;
            Porter::tokenize(w, stems);
            if (stems.len > 0)
                return get_doc_ids(stems[0]);
            return DynArray<int>();
        }
        return DynArray<int>();
    }

    DynArray<int> parse_and_expr() {
        DynArray<int> left = parse_atom();
        while ((*toks)[pos].kind == T_AND ||
               ((*toks)[pos].kind == T_WORD || (*toks)[pos].kind == T_LP ||
                (*toks)[pos].kind == T_NOT)) {
            if ((*toks)[pos].kind == T_AND) ++pos;

            if ((*toks)[pos].kind == T_WORD &&
                (*toks)[pos].text.rfind("QUOTE:", 0) != 0) {
                std::string w = (*toks)[pos].text;
                ++pos;
                DynArray<std::string> stems;
                Porter::tokenize(w, stems);
                if (stems.len > 0) {
                    TermPostingStream tps = get_posting_meta(stems[0]);
                    if (tps.skips.len > 0 && left.len < tps.df) {
                        left = merge_and_skip(left, tps);
                    } else {
                        DynArray<int> right = get_doc_ids(stems[0]);
                        left = merge_and(left, right);
                    }
                }
            } else {
                DynArray<int> right = parse_atom();
                left = merge_and(left, right);
            }
        }
        return left;
    }

    DynArray<int> parse_or_expr() {
        DynArray<int> left = parse_and_expr();
        while ((*toks)[pos].kind == T_OR) {
            ++pos;
            DynArray<int> right = parse_and_expr();
            left = merge_or(left, right);
        }
        return left;
    }

public:
    DynArray<int> run(DynArray<QTok>& tokens) {
        toks = &tokens;
        pos  = 0;
        return parse_or_expr();
    }
};

void handle_query(const std::string& query) {
    DynArray<QTok> tokens = lex(query);

    if (has_bool_ops(tokens)) {
        BoolParser bp;
        DynArray<int> ids = bp.run(tokens);

        DynArray<std::string> all_terms;
        for (size_t i = 0; i < tokens.len; ++i) {
            if (tokens[i].kind == T_WORD) {
                DynArray<std::string> stems;
                std::string w = tokens[i].text;
                if (w.rfind("QUOTE:", 0) == 0) w = w.substr(6);
                size_t slash = w.rfind('/');
                if (slash != std::string::npos) w = w.substr(0, slash);
                Porter::tokenize(w, stems);
                for (size_t j = 0; j < stems.len; ++j)
                    all_terms.append(stems[j]);
            }
        }

        DynArray<ScoredDoc> scored;
        rank_by_tfidf(all_terms, scored);

        DynArray<ScoredDoc> filtered;
        for (size_t i = 0; i < scored.len; ++i) {
            bool found = false;
            for (size_t j = 0; j < ids.len; ++j) {
                if (ids[j] == scored[i].doc_id) { found = true; break; }
            }
            if (found) filtered.append(scored[i]);
        }
        for (size_t j = 0; j < ids.len; ++j) {
            bool already = false;
            for (size_t i = 0; i < filtered.len; ++i) {
                if (filtered[i].doc_id == ids[j]) { already = true; break; }
            }
            if (!already) {
                ScoredDoc sd; sd.doc_id = ids[j]; sd.score = 0;
                filtered.append(sd);
            }
        }

        std::cout << "Found " << filtered.len << " docs." << std::endl;
        size_t limit = filtered.len < 50 ? filtered.len : 50;
        for (size_t i = 0; i < limit; ++i) {
            int id = filtered[i].doc_id;
            if (id >= 0 && static_cast<uint32_t>(id) < total_docs)
                std::cout << docs[id].title << " (" << docs[id].url << ")" << std::endl;
        }
    } else {
        bool has_quotes = false;
        for (size_t i = 0; i < tokens.len; ++i) {
            if (tokens[i].kind == T_WORD && tokens[i].text.rfind("QUOTE:", 0) == 0) {
                has_quotes = true; break;
            }
        }

        if (has_quotes) {
            BoolParser bp;
            DynArray<int> ids = bp.run(tokens);
            std::cout << "Found " << ids.len << " docs." << std::endl;
            size_t limit = ids.len < 50 ? ids.len : 50;
            for (size_t i = 0; i < limit; ++i) {
                int id = ids[i];
                if (id >= 0 && static_cast<uint32_t>(id) < total_docs)
                    std::cout << docs[id].title << " (" << docs[id].url << ")" << std::endl;
            }
        } else {
            DynArray<std::string> terms;
            for (size_t i = 0; i < tokens.len; ++i) {
                if (tokens[i].kind == T_WORD) {
                    DynArray<std::string> stems;
                    Porter::tokenize(tokens[i].text, stems);
                    for (size_t j = 0; j < stems.len; ++j)
                        terms.append(stems[j]);
                }
            }
            DynArray<ScoredDoc> scored;
            rank_by_tfidf(terms, scored);

            std::cout << "Found " << scored.len << " docs." << std::endl;
            size_t limit = scored.len < 50 ? scored.len : 50;
            for (size_t i = 0; i < limit; ++i) {
                int id = scored[i].doc_id;
                if (id >= 0 && static_cast<uint32_t>(id) < total_docs)
                    std::cout << docs[id].title << " (" << docs[id].url << ")" << std::endl;
            }
        }
    }
    std::cout << "__END_QUERY__" << std::endl;
}

int main(int argc, char* argv[]) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    if (argc < 2) {
        std::cerr << "Usage: qry <index_dir>" << std::endl;
        return 1;
    }

    try {
        load_index(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "Ready" << std::endl;
    std::cerr << "Loaded " << total_docs << " docs. Awaiting queries." << std::endl;

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        if (line == "exit") break;
        handle_query(line);
    }
    return 0;
}

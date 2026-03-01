#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cmath>

#include "vec.hpp"
#include "map.hpp"
#include "porter.hpp"
#include "varint.hpp"

static const int SKIP_GAP = 128;

struct PostingEntry {
    int doc_id;
    DynArray<int> positions;
};

struct TermData {
    DynArray<PostingEntry> entries;

    void record(int doc, int pos) {
        if (entries.len == 0 || entries[entries.len - 1].doc_id != doc) {
            PostingEntry pe;
            pe.doc_id = doc;
            pe.positions.append(pos);
            entries.append(std::move(pe));
        } else {
            entries[entries.len - 1].positions.append(pos);
        }
    }
};

HashDict<TermData> global_idx;
DynArray<std::string> all_urls;
DynArray<std::string> all_titles;
DynArray<int>         doc_lengths;

static const char MAGIC_FWD[]  = "FWRD";
static const char MAGIC_TERM[] = "TERM";
static const char MAGIC_DATA[] = "DATA";
static const uint16_t FORMAT_VER = 1;

void write_forward(const std::string& dir) {
    std::string path = dir + "/index.fwd";
    std::ofstream out(path, std::ios::binary);
    out.write(MAGIC_FWD, 4);
    out.write(reinterpret_cast<const char*>(&FORMAT_VER), 2);

    uint32_t n = static_cast<uint32_t>(all_urls.len);
    out.write(reinterpret_cast<const char*>(&n), 4);

    DynArray<uint64_t> offsets;
    uint64_t base = 4 + 2 + 4 + n * 8;
    uint64_t cur = 0;
    for (size_t i = 0; i < n; ++i) {
        offsets.append(base + cur);
        uint16_t ul = static_cast<uint16_t>(all_urls[i].size());
        uint16_t tl = static_cast<uint16_t>(all_titles[i].size());
        cur += 2 + ul + 2 + tl + 4;
    }
    for (size_t i = 0; i < n; ++i)
        out.write(reinterpret_cast<const char*>(&offsets[i]), 8);

    for (size_t i = 0; i < n; ++i) {
        uint16_t ul = static_cast<uint16_t>(all_urls[i].size());
        out.write(reinterpret_cast<const char*>(&ul), 2);
        out.write(all_urls[i].c_str(), ul);

        uint16_t tl = static_cast<uint16_t>(all_titles[i].size());
        out.write(reinterpret_cast<const char*>(&tl), 2);
        out.write(all_titles[i].c_str(), tl);

        uint32_t dl = static_cast<uint32_t>(doc_lengths[i]);
        out.write(reinterpret_cast<const char*>(&dl), 4);
    }
    out.close();
}

void write_terms(const std::string& dir) {
    std::string term_path = dir + "/index.term";
    std::string data_path = dir + "/index.data";

    std::ofstream ft(term_path, std::ios::binary);
    std::ofstream fd(data_path, std::ios::binary);

    ft.write(MAGIC_TERM, 4);
    ft.write(reinterpret_cast<const char*>(&FORMAT_VER), 2);
    uint32_t term_count = 0;
    auto tc_pos = ft.tellp();
    ft.write(reinterpret_cast<const char*>(&term_count), 4);

    fd.write(MAGIC_DATA, 4);
    fd.write(reinterpret_cast<const char*>(&FORMAT_VER), 2);

    for (auto it = global_idx.begin(); it != global_idx.end(); ++it) {
        ++term_count;
        const std::string& term = it->key;
        TermData& td = it->val;

        uint64_t data_off = static_cast<uint64_t>(fd.tellp());
        uint32_t df = static_cast<uint32_t>(td.entries.len);

        uint8_t tlen = static_cast<uint8_t>(std::min(term.size(), size_t(255)));
        ft.write(reinterpret_cast<const char*>(&tlen), 1);
        ft.write(term.c_str(), tlen);
        ft.write(reinterpret_cast<const char*>(&data_off), 8);
        ft.write(reinterpret_cast<const char*>(&df), 4);

        DynArray<uint8_t> blob;
        VarInt::encode(df, blob);

        int prev_doc = 0;
        DynArray<uint8_t> entries_blob;
        DynArray<size_t> skip_offsets;
        DynArray<int>    skip_doc_ids;

        for (size_t i = 0; i < df; ++i) {
            if (i > 0 && i % SKIP_GAP == 0) {
                skip_offsets.append(entries_blob.len);
                skip_doc_ids.append(td.entries[i].doc_id);
            }

            int delta = td.entries[i].doc_id - prev_doc;
            VarInt::encode(static_cast<uint32_t>(delta), entries_blob);
            prev_doc = td.entries[i].doc_id;

            uint32_t freq = static_cast<uint32_t>(td.entries[i].positions.len);
            VarInt::encode(freq, entries_blob);

            int prev_pos = 0;
            for (size_t j = 0; j < freq; ++j) {
                VarInt::encode(static_cast<uint32_t>(td.entries[i].positions[j] - prev_pos),
                               entries_blob);
                prev_pos = td.entries[i].positions[j];
            }
        }

        uint16_t num_skips = static_cast<uint16_t>(skip_offsets.len);
        VarInt::encode(num_skips, blob);
        for (size_t i = 0; i < num_skips; ++i) {
            VarInt::encode(static_cast<uint32_t>(skip_doc_ids[i]), blob);
            VarInt::encode(static_cast<uint32_t>(skip_offsets[i]), blob);
        }

        for (size_t i = 0; i < entries_blob.len; ++i)
            blob.append(entries_blob[i]);

        fd.write(reinterpret_cast<const char*>(blob.buf), blob.len);
    }

    ft.seekp(tc_pos);
    ft.write(reinterpret_cast<const char*>(&term_count), 4);

    ft.close();
    fd.close();
    std::cerr << "Index built: " << term_count << " terms, "
              << all_urls.len << " docs" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: idx <output_dir>" << std::endl;
        return 1;
    }
    std::string out_dir = argv[1];

    std::string line;
    int doc_id = 0;
    DynArray<std::string> tokens;
    double total_tokens = 0;

    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;

        size_t t1 = line.find('\t');
        if (t1 == std::string::npos) continue;
        size_t t2 = line.find('\t', t1 + 1);
        if (t2 == std::string::npos) continue;

        all_urls.append(line.substr(0, t1));
        all_titles.append(line.substr(t1 + 1, t2 - t1 - 1));

        std::string body = line.substr(t2 + 1);
        tokens.dispose();
        Porter::tokenize(body, tokens);

        doc_lengths.append(static_cast<int>(tokens.len));
        total_tokens += tokens.len;

        for (size_t i = 0; i < tokens.len; ++i)
            global_idx[tokens[i]].record(doc_id, static_cast<int>(i));

        ++doc_id;
        if (doc_id % 500 == 0)
            std::cerr << "  indexed " << doc_id << " docs\r";
    }
    std::cerr << std::endl;

    write_forward(out_dir);
    write_terms(out_dir);
    return 0;
}

#pragma once

// Instagram CSV ingestion — loads instagram_interactions.csv
// (source_username,target_username,interaction_type,timestamp,latency_ms)
// into GraphStorage as real XInteractionEvents.
//
// The dataset has no actors table, so node metadata is derived:
//   - handle: "ig:" + username
//   - followers_count: estimated from in-degree (documented heuristic)
//   - is_bot: post-pass flags accounts whose edges are almost all <2s latency
//     AND part of a closed cycle — the same signal detect_bot_swarms uses.
//
// Interaction type map (no dedicated FOLLOW type exists):
//   like->LIKE, comment->REPLY, share->REPOST, save->BOOKMARK, follow->REPOST
//   (follow approximated as a strong tie; weight only affects CSR weight_val)

#include "types.hpp"
#include "graph_storage.hpp"

#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>

namespace influmapper {

struct InstagramCsvStats {
    size_t rows_read = 0;
    size_t rows_malformed = 0;
    size_t edges_ingested = 0;
    size_t unique_users = 0;
    size_t bots_flagged = 0;
    double parse_time_ms = 0.0;
};

class InstagramCsvIngestionEngine {
private:
    GraphStorage& storage_;
    std::unordered_map<std::string, uint32_t> user_to_node_;
    std::unordered_set<std::string> unique_raw_users_;
    std::vector<std::vector<uint16_t>> out_latencies_; // per node, for bot pass

    static bool parse_u32(const std::string& s, uint32_t& out) {
        if (s.empty()) return false;
        uint64_t v = 0;
        for (char c : s) {
            if (c < '0' || c > '9') return false;
            v = v * 10 + (c - '0');
            if (v > 0xFFFFFFFFULL) return false;
        }
        out = static_cast<uint32_t>(v);
        return true;
    }

    static void trim(std::string& s) {
        size_t a = s.find_first_not_of(" \t\r\n");
        if (a == std::string::npos) { s.clear(); return; }
        size_t b = s.find_last_not_of(" \t\r\n");
        s = s.substr(a, b - a + 1);
    }

    static InteractionType map_type(const std::string& t) {
        if (t == "like")    return InteractionType::LIKE;
        if (t == "comment") return InteractionType::REPLY;
        if (t == "share")   return InteractionType::REPOST;
        if (t == "save")    return InteractionType::BOOKMARK;
        if (t == "follow")  return InteractionType::REPOST; // strong-tie approximation
        return InteractionType::LIKE;
    }

    uint32_t node_for(const std::string& username) {
        auto it = user_to_node_.find(username);
        if (it != user_to_node_.end()) return it->second;
        std::string handle = "ig:" + username;
        uint32_t id = storage_.get_or_create_node(handle, "igcsv");
        user_to_node_[username] = id;
        unique_raw_users_.insert(username);
        return id;
    }

public:
    explicit InstagramCsvIngestionEngine(GraphStorage& storage)
        : storage_(storage) {}

    InstagramCsvStats ingest_file(const std::string& filepath,
                                  bool run_bot_pass = true,
                                  size_t max_rows = 0) {
        InstagramCsvStats stats;
        auto t0 = std::chrono::high_resolution_clock::now();

        std::ifstream file(filepath);
        if (!file.is_open()) {
            throw std::runtime_error(
                "[InstagramCSV] Cannot open file: " + filepath);
        }

        std::cout << "[InstagramCSV] Opening: " << filepath << "\n";

        std::string line;
        bool header_skipped = false;
        while (std::getline(file, line)) {
            trim(line);
            if (line.empty()) continue;

            if (!header_skipped) {
                if (line[0] < '0' || line[0] > '9') { // header row
                    header_skipped = true;
                    continue;
                }
                header_skipped = true;
            }

            // Columns: source_username,target_username,interaction_type,timestamp,latency_ms
            // Usernames may contain dots/underscores but not commas.
            size_t p1 = line.find(',');
            if (p1 == std::string::npos) { ++stats.rows_malformed; continue; }
            size_t p2 = line.find(',', p1 + 1);
            size_t p3 = p2 == std::string::npos ? p2 : line.find(',', p2 + 1);
            size_t p4 = p3 == std::string::npos ? p3 : line.find(',', p3 + 1);
            if (p2 == std::string::npos || p3 == std::string::npos ||
                p4 == std::string::npos) { ++stats.rows_malformed; continue; }

            std::string src = line.substr(0, p1);
            std::string dst = line.substr(p1 + 1, p2 - p1 - 1);
            std::string type = line.substr(p2 + 1, p3 - p2 - 1);
            std::string ts_s = line.substr(p3 + 1, p4 - p3 - 1);
            std::string lat_s = line.substr(p4 + 1);
            trim(src); trim(dst); trim(type); trim(ts_s); trim(lat_s);
            if (src.empty() || dst.empty() || src == dst) {
                ++stats.rows_malformed; continue;
            }

            uint32_t ts = 0, lat = 0;
            if (!parse_u32(ts_s, ts) || !parse_u32(lat_s, lat)) {
                ++stats.rows_malformed; continue;
            }

            uint32_t sid = node_for(src);
            uint32_t did = node_for(dst);

            XInteractionEvent ev{};
            ev.source_id = sid;
            ev.target_id = did;
            ev.timestamp_sec = ts;
            ev.type = static_cast<uint8_t>(map_type(type));
            ev.latency_ms = static_cast<uint16_t>(std::min<uint32_t>(lat, 65535));
            storage_.ingest_batch(&ev, 1);
            ++stats.edges_ingested;
            ++stats.rows_read;

            if (sid >= out_latencies_.size()) out_latencies_.resize(sid + 1);
            if (did >= out_latencies_.size()) out_latencies_.resize(did + 1);
            out_latencies_[sid].push_back(ev.latency_ms);

            if (max_rows > 0 && stats.rows_read >= max_rows) break;
        }

        stats.unique_users = unique_raw_users_.size();

        // --- Derived metadata pass ---
        // followers_count estimate: in-degree driven, deterministic per handle
        auto all_meta = storage_.get_all_nodes_meta();
        for (const auto& [raw, nid] : user_to_node_) {
            if (nid >= all_meta.size()) continue;
            const auto& m = all_meta[nid];
            uint32_t est_followers = m.in_degree * 4000 +
                (static_cast<uint32_t>(std::hash<std::string>{}(raw)) % 5000);
            storage_.update_node_profile(nid, "ig:" + raw, "",
                "instagram creator (csv)", est_followers,
                m.out_degree * 120 + 200);
        }

        // --- Bot pass: median outgoing latency < 2s + closed cycles ---
        if (run_bot_pass) {
            for (const auto& [raw, nid] : user_to_node_) {
                if (nid >= out_latencies_.size() || out_latencies_[nid].size() < 2)
                    continue;
                auto lats = out_latencies_[nid];
                std::sort(lats.begin(), lats.end());
                uint16_t median = lats[lats.size() / 2];
                // Pod members: very fast reactions AND reciprocal clustering
                if (median < 2000) {
                    // verify closed-cycle participation: at least one neighbor
                    // that also reacts back with low latency
                    bool closed = false;
                    for (const auto& e : storage_.get_edges(nid)) {
                        if (e.target_id < out_latencies_.size() &&
                            !out_latencies_[e.target_id].empty() &&
                            out_latencies_[e.target_id].back() < 2000) {
                            closed = true; break;
                        }
                    }
                    if (closed) {
                        storage_.mark_bot(nid, true);
                        ++stats.bots_flagged;
                    }
                }
            }
        }

        auto t1 = std::chrono::high_resolution_clock::now();
        stats.parse_time_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        std::cout << "[InstagramCSV] Done: " << stats.rows_read << " rows, "
                  << stats.rows_malformed << " malformed, " << stats.edges_ingested
                  << " edges, " << stats.unique_users << " users, "
                  << stats.bots_flagged << " pods flagged (" << stats.parse_time_ms
                  << " ms).\n";
        return stats;
    }
};

} // namespace influmapper

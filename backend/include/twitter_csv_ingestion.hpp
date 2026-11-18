
#pragma once

/*
 * twitter_csv_ingestion.hpp
 * ──────────────────────────────────────────────────────────────────────────
 * FR-1: High-Performance Local X (Twitter) Dataset Ingestion Pipeline
 *
 * Dataset Schema:  user_index, author_index, time_chunk
 *   - user_index   -> follower / source user (interacting)
 *   - author_index -> followee / target user (being followed / replied to)
 *   - time_chunk   -> interaction slot (integer tick, 0-based; treated as
 *                     seconds offset from an epoch for delta calculations)
 *
 * Design:
 *   - Line-by-line streaming via std::ifstream to keep peak heap usage low.
 *   - Integer user IDs are converted to "tw:<id>" namespaced handle strings
 *     and inserted into the shared RobinHoodHandleMap for a contiguous
 *     uint32_t assignment, unifying the Twitter and Bluesky namespaces.
 *   - Events are packed into alignas(16) XInteractionEvent structs (16 bytes,
 *     4 per 64-byte cache line) and flushed via GraphStorage::ingest_batch()
 *     every CSV_BATCH_SIZE records.
 *   - Time deltas (Delta-t) between consecutive interactions FOR EACH USER
 *     are accumulated in a per-source hash map; users with >= BOT_MIN_EDGE_COUNT
 *     edges and median Delta-t < BOT_DELTA_THRESHOLD_TICKS are flagged as bots.
 *   - Dynamic edge weights W_e:
 *       FOLLOW / LIKE  -> 1.0   (baseline interaction)
 *       REPLY          -> 2.0   (stronger signal)
 *       RETWEET        -> 3.0   (amplification)
 *       QUOTE          -> 2.5   (contextual amplification)
 *   - Follow-graph edges are classified as REPOST (weight 3.0).
 *
 * Usage:
 *   TwitterCSVIngestionEngine engine(storage);
 *   TwitterCSVIngestionStats  stats = engine.ingest_file(
 *       "C:\\path\\to\\TwitterFollowGraph.csv");
 * ──────────────────────────────────────────────────────────────────────────
 */

#include "types.hpp"
#include "graph_storage.hpp"
#include "robin_hood_map.hpp"

#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <iostream>
#include <cstdint>
#include <chrono>
#include <limits>
#include <stdexcept>

namespace influmapper {

// ---------------------------------------------------------------------------
// Configuration constants
// ---------------------------------------------------------------------------

// Number of XInteractionEvent structs accumulated before a single
// ingest_batch() call.  4096 events x 16 bytes = 64 KiB per flush,
// fitting neatly into most L2 caches.
static constexpr size_t CSV_BATCH_SIZE = 4096;

// Users with at least this many recorded edges are eligible for bot analysis.
static constexpr uint32_t BOT_MIN_EDGE_COUNT = 8;

// Median inter-event Delta-t <= this many time-chunk units => bot-suspect.
// (Assuming 1 tick ~= 1 hour: 3 ticks = 3 h)
static constexpr uint32_t BOT_DELTA_THRESHOLD_TICKS = 3;

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

struct TwitterCSVIngestionStats {
    size_t   rows_read       = 0;   // Total CSV data rows parsed
    size_t   rows_malformed  = 0;   // Rows skipped due to parse errors
    size_t   edges_ingested  = 0;   // Directed edges committed to GraphStorage
    size_t   unique_users    = 0;   // Distinct user IDs mapped
    size_t   bots_flagged    = 0;   // Nodes marked is_bot=true
    double   parse_time_ms   = 0.0; // Wall-clock parsing time (ms)
    double   ingest_time_ms  = 0.0; // Wall-clock bot-detection time (ms)
    uint32_t max_time_chunk  = 0;
    uint32_t min_time_chunk  = std::numeric_limits<uint32_t>::max();
};

// ---------------------------------------------------------------------------
// Per-user delta accumulator for bot detection
// ---------------------------------------------------------------------------

struct UserTimeSeries {
    std::vector<uint32_t> timestamps; // Raw time_chunk values, insertion order
};

// ---------------------------------------------------------------------------
// Ingestion Engine
// ---------------------------------------------------------------------------

class TwitterCSVIngestionEngine {
private:
    GraphStorage& storage_;

    // Per-source user time-series for Delta-t / bot detection
    std::unordered_map<uint32_t, UserTimeSeries> time_series_;

    // Distinct raw user IDs seen in this CSV (for stats.unique_users)
    std::unordered_set<uint32_t> unique_raw_ids_;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    // Trim leading/trailing ASCII whitespace in-place
    static void trim(std::string& s) noexcept {
        const char* ws = " \t\r\n";
        s.erase(0, s.find_first_not_of(ws));
        auto last = s.find_last_not_of(ws);
        if (last != std::string::npos) s.erase(last + 1);
        else s.clear();
    }

    // Fast string -> uint32_t with basic validation.
    // Returns false on failure (non-numeric, empty, overflow).
    [[nodiscard]] static bool parse_u32(const std::string& token,
                                        uint32_t& out) noexcept {
        if (token.empty()) return false;
        uint64_t v = 0;
        for (char c : token) {
            if (c < '0' || c > '9') return false;
            v = v * 10 + static_cast<uint32_t>(c - '0');
            if (v > 0xFFFFFFFFULL) return false; // overflow guard
        }
        out = static_cast<uint32_t>(v);
        return true;
    }

    // All follow-graph edges classified as REPOST (strongest social signal).
    static constexpr InteractionType EDGE_TYPE = InteractionType::REPOST;

    // Compute median Delta-t from a sorted copy of the timestamp vector.
    [[nodiscard]] static uint32_t median_delta(std::vector<uint32_t> ts) noexcept {
        if (ts.size() < 2) return std::numeric_limits<uint32_t>::max();
        std::sort(ts.begin(), ts.end());
        std::vector<uint32_t> deltas;
        deltas.reserve(ts.size() - 1);
        for (size_t i = 1; i < ts.size(); ++i) {
            deltas.push_back(ts[i] - ts[i - 1]);
        }
        std::sort(deltas.begin(), deltas.end());
        return deltas[deltas.size() / 2];
    }

    // Escape a raw integer ID into a namespaced Twitter handle string
    // e.g. 12345 -> "tw:12345" to avoid collision with Bluesky handles.
    [[nodiscard]] static std::string make_handle(uint32_t raw_id) {
        return "tw:" + std::to_string(raw_id);
    }

    // -----------------------------------------------------------------------
    // Bot detection pass (called after all rows are ingested)
    // -----------------------------------------------------------------------
    size_t run_bot_detection() {
        size_t flagged = 0;
        for (auto& [node_id, ts] : time_series_) {
            if (ts.timestamps.size() < BOT_MIN_EDGE_COUNT) continue;
            uint32_t med_dt = median_delta(ts.timestamps);
            if (med_dt <= BOT_DELTA_THRESHOLD_TICKS) {
                storage_.mark_bot(node_id, true);
                ++flagged;
            }
        }
        return flagged;
    }

public:
    explicit TwitterCSVIngestionEngine(GraphStorage& storage)
        : storage_(storage) {}

    // -----------------------------------------------------------------------
    // Main ingestion entry point
    // Streams the CSV from disk line-by-line.
    // Returns filled TwitterCSVIngestionStats on success.
    // Throws std::runtime_error if the file cannot be opened.
    // -----------------------------------------------------------------------
    TwitterCSVIngestionStats ingest_file(const std::string& filepath,
                                          bool run_bot_pass = true,
                                          size_t max_rows = 0) {
        TwitterCSVIngestionStats stats;
        time_series_.clear();
        unique_raw_ids_.clear();

        std::ifstream file(filepath);
        if (!file.is_open()) {
            throw std::runtime_error(
                "[TwitterCSVIngestion] Cannot open file: " + filepath);
        }

        std::cout << "[TwitterCSV] Opening: " << filepath << "\n";
        std::cout << "[TwitterCSV] Streaming into 16-byte XInteractionEvent "
                     "batch buffer (batch_size=" << CSV_BATCH_SIZE << ")...\n";

        // Batch buffer: 4096 x 16 bytes = 64 KiB, stack-allocated
        alignas(64) XInteractionEvent batch[CSV_BATCH_SIZE];
        size_t batch_pos = 0;

        auto parse_start = std::chrono::high_resolution_clock::now();

        std::string line;
        bool header_skipped = false;

        while (std::getline(file, line)) {
            trim(line);
            if (line.empty()) continue;

            // Skip header row: detect if first token is not a digit
            if (!header_skipped) {
                if (!line.empty() && (line[0] < '0' || line[0] > '9')) {
                    header_skipped = true;
                    std::cout << "[TwitterCSV] Header skipped: " << line << "\n";
                    continue;
                }
                header_skipped = true;
            }

            // Parse CSV columns: user_index, author_index, time_chunk
            std::string tok_user, tok_author, tok_time;
            {
                size_t p1 = line.find(',');
                if (p1 == std::string::npos) { ++stats.rows_malformed; continue; }
                size_t p2 = line.find(',', p1 + 1);
                tok_user   = line.substr(0, p1);
                tok_author = (p2 != std::string::npos)
                                 ? line.substr(p1 + 1, p2 - p1 - 1)
                                 : line.substr(p1 + 1);
                tok_time   = (p2 != std::string::npos)
                                 ? line.substr(p2 + 1)
                                 : "0";
                trim(tok_user);
                trim(tok_author);
                trim(tok_time);
            }

            uint32_t raw_user = 0, raw_author = 0, raw_time = 0;
            if (!parse_u32(tok_user, raw_user)    ||
                !parse_u32(tok_author, raw_author) ||
                !parse_u32(tok_time, raw_time)) {
                ++stats.rows_malformed;
                continue;
            }

            // Self-loops are meaningless for social graph analytics
            if (raw_user == raw_author) {
                ++stats.rows_malformed;
                continue;
            }

            unique_raw_ids_.insert(raw_user);
            unique_raw_ids_.insert(raw_author);

            // Map raw integer IDs -> contiguous uint32_t node IDs
            uint32_t src_id = storage_.get_or_create_node(make_handle(raw_user),   "tw");
            uint32_t dst_id = storage_.get_or_create_node(make_handle(raw_author), "tw");

            // Accumulate time-series for bot Delta-t detection
            time_series_[src_id].timestamps.push_back(raw_time);

            // Track time-chunk range
            if (raw_time > stats.max_time_chunk) stats.max_time_chunk = raw_time;
            if (raw_time < stats.min_time_chunk) stats.min_time_chunk = raw_time;

            // Build 16-byte XInteractionEvent (alignas(16) guaranteed)
            auto& ev        = batch[batch_pos];
            ev.source_id    = src_id;
            ev.target_id    = dst_id;
            ev.timestamp_sec = raw_time; // time_chunk as timestamp proxy
            ev.type         = static_cast<uint8_t>(EDGE_TYPE);
            ev.flags        = 0;
            // latency_ms: encode low 16-bits of time_chunk for now;
            // true reply-latency requires paired lookup in a second pass.
            ev.latency_ms   = static_cast<uint16_t>(raw_time & 0xFFFF);

            ++batch_pos;
            ++stats.rows_read;

            // Flush batch when full
            if (batch_pos == CSV_BATCH_SIZE) {
                storage_.ingest_batch(batch, batch_pos);
                stats.edges_ingested += batch_pos;
                batch_pos = 0;

                // Progress indicator every 500k rows
                if (stats.rows_read % 500000 == 0) {
                    std::cout << "[TwitterCSV] ...processed " << stats.rows_read
                              << " rows, " << stats.edges_ingested << " edges.\n";
                }
            }

            // Optional early exit for testing
            if (max_rows > 0 && stats.rows_read >= max_rows) break;
        }

        // Flush remaining partial batch
        if (batch_pos > 0) {
            storage_.ingest_batch(batch, batch_pos);
            stats.edges_ingested += batch_pos;
        }

        auto parse_end = std::chrono::high_resolution_clock::now();
        stats.parse_time_ms =
            std::chrono::duration<double, std::milli>(parse_end - parse_start).count();
        stats.unique_users = unique_raw_ids_.size();

        std::cout << "[TwitterCSV] Parse complete: " << stats.rows_read
                  << " rows, " << stats.rows_malformed << " malformed, "
                  << stats.parse_time_ms << " ms.\n";
        std::cout << "[TwitterCSV] Unique nodes: " << stats.unique_users
                  << " | Edges committed: " << stats.edges_ingested << "\n";

        // Bot detection pass
        if (run_bot_pass) {
            auto bot_start = std::chrono::high_resolution_clock::now();
            stats.bots_flagged = run_bot_detection();
            auto bot_end = std::chrono::high_resolution_clock::now();
            stats.ingest_time_ms =
                std::chrono::duration<double, std::milli>(bot_end - bot_start).count();
            std::cout << "[TwitterCSV] Bot detection: " << stats.bots_flagged
                      << " users flagged (median Dt <= "
                      << BOT_DELTA_THRESHOLD_TICKS << " ticks, min-edges >= "
                      << BOT_MIN_EDGE_COUNT << "). Time: "
                      << stats.ingest_time_ms << " ms.\n";
        }

        print_delta_summary();
        return stats;
    }

    // Print top-5 rapid-fire users (debug output)
    void print_delta_summary() const {
        std::vector<std::pair<uint32_t, uint32_t>> rapid;
        rapid.reserve(32);
        for (const auto& [node_id, ts] : time_series_) {
            if (ts.timestamps.size() < BOT_MIN_EDGE_COUNT) continue;
            uint32_t med_dt = median_delta(ts.timestamps);
            if (med_dt <= BOT_DELTA_THRESHOLD_TICKS * 2) {
                rapid.emplace_back(node_id, med_dt);
            }
        }
        if (rapid.empty()) return;
        std::sort(rapid.begin(), rapid.end(),
                  [](const auto& a, const auto& b){ return a.second < b.second; });
        std::cout << "[TwitterCSV] Top rapid-fire accounts (smallest median Dt):\n";
        for (size_t i = 0; i < std::min(rapid.size(), size_t(5)); ++i) {
            std::cout << "  node_id=" << rapid[i].first
                      << " median_Dt=" << rapid[i].second << " ticks\n";
        }
    }

    // Free auxiliary memory (time-series + ID set) after the bot pass
    void clear_time_series() noexcept {
        time_series_.clear();
        unique_raw_ids_.clear();
    }
};

// ---------------------------------------------------------------------------
// Convenience free function
// ---------------------------------------------------------------------------

/// Ingest a local Twitter/X CSV follow-graph file into GraphStorage.
/// Throws std::runtime_error if the file cannot be opened.
inline TwitterCSVIngestionStats ingest_twitter_csv(
    GraphStorage&      storage,
    const std::string& filepath,
    bool               run_bot_pass = true,
    size_t             max_rows     = 0)
{
    TwitterCSVIngestionEngine engine(storage);
    auto stats = engine.ingest_file(filepath, run_bot_pass, max_rows);
    engine.clear_time_series();
    return stats;
}

} // namespace influmapper

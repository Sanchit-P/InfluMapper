#pragma once

#include "types.hpp"
#include "robin_hood_map.hpp"

#include <vector>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <memory>
#include <algorithm>
#include <optional>
#include <fstream>
#include <ctime>
#include <cstdint>

namespace influmapper {

// FR-2: Graph Storage Engine
// Maintains dynamic multigraph vectors for incoming live interactions, and periodically
// publishes double-buffered read-only Compressed Sparse Row (CSR) projections without
// lock contention on parallel algorithmic sweeps.
class GraphStorage {
private:
    // Dynamic multigraph storage
    mutable std::shared_mutex graph_mutex_;
    std::vector<std::vector<XEdge>> adj_list_;
    std::vector<NodeMetadata> nodes_meta_;
    RobinHoodHandleMap handle_to_id_;
    std::vector<std::string> id_to_handle_;

    // Double-buffered read-only CSR projections
    std::atomic<std::shared_ptr<const CSRGraph>> active_csr_;
    std::atomic<uint64_t> snapshot_counter_{0};
    std::atomic<uint64_t> total_edges_ingested_{0};

    // Calculate edge weight based on interaction type
    static inline float get_weight(uint8_t type) noexcept {
        switch (static_cast<InteractionType>(type)) {
            case InteractionType::REPOST:   return 3.0f;
            case InteractionType::QUOTE:    return 2.5f;
            case InteractionType::REPLY:    return 2.0f;
            case InteractionType::LIKE:     return 1.0f;
            case InteractionType::BOOKMARK: return 1.5f;
            default:                        return 1.0f;
        }
    }

public:
    GraphStorage() {
        active_csr_.store(std::make_shared<CSRGraph>());
    }

    // Register or retrieve contiguous ID for handle/DID
    uint32_t get_or_create_node(const std::string& handle, const std::string& did = "") {
        std::unique_lock lock(graph_mutex_);
        uint32_t id = handle_to_id_.get_or_create(handle);
        if (id >= adj_list_.size()) {
            adj_list_.resize(id + 1);
            nodes_meta_.resize(id + 1);
            id_to_handle_.resize(id + 1);
            nodes_meta_[id].id = id;
            nodes_meta_[id].handle = handle;
            nodes_meta_[id].did = did.empty() ? handle : did;
            id_to_handle_[id] = handle;
        }
        return id;
    }

    // Update real profile metadata
    void update_node_profile(uint32_t id, const std::string& display_name, const std::string& avatar_url,
                             const std::string& description = "", uint32_t followers = 0, uint32_t following = 0) {
        std::unique_lock lock(graph_mutex_);
        if (id < nodes_meta_.size()) {
            if (!display_name.empty()) nodes_meta_[id].display_name = display_name;
            if (!avatar_url.empty()) nodes_meta_[id].avatar_url = avatar_url;
            if (!description.empty()) nodes_meta_[id].description = description;
            if (followers > 0) nodes_meta_[id].followers_count = followers;
            if (following > 0) nodes_meta_[id].following_count = following;
        }
    }

    // Ingest a batch of 16-byte packed interaction events.
    // When dedup=true, edges identical on (source, target, type) are skipped —
    // used by live ingestion sources that may be re-run (refresh, restart).
    void ingest_batch(const XInteractionEvent* events, size_t count, bool dedup = false) {
        if (!events || count == 0) return;

        std::unique_lock lock(graph_mutex_);
        for (size_t i = 0; i < count; ++i) {
            const auto& ev = events[i];
            size_t max_id = std::max(ev.source_id, ev.target_id);
            if (max_id >= adj_list_.size()) {
                adj_list_.resize(max_id + 1);
                nodes_meta_.resize(max_id + 1);
                id_to_handle_.resize(max_id + 1);
            }

            if (dedup) {
                const auto& out = adj_list_[ev.source_id];
                bool exists = false;
                for (const auto& e : out) {
                    if (e.target_id == ev.target_id && e.type == ev.type) { exists = true; break; }
                }
                if (exists) continue;
            }

            // Insert directed edge source -> target
            adj_list_[ev.source_id].push_back(XEdge{
                ev.target_id,
                ev.timestamp_sec,
                ev.type,
                ev.latency_ms
            });

            nodes_meta_[ev.source_id].out_degree++;
            nodes_meta_[ev.target_id].in_degree++;
            total_edges_ingested_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Single edge ingestion helper
    void add_interaction(uint32_t source_id, uint32_t target_id, InteractionType type, uint32_t timestamp, uint16_t latency_ms = 0) {
        XInteractionEvent ev;
        ev.source_id = source_id;
        ev.target_id = target_id;
        ev.timestamp_sec = timestamp;
        ev.type = static_cast<uint8_t>(type);
        ev.flags = 0;
        ev.latency_ms = latency_ms;
        ingest_batch(&ev, 1);
    }

    // FR-2.2: Publish atomic double-buffered CSR snapshot
    std::shared_ptr<const CSRGraph> publish_csr_snapshot() {
        auto new_csr = std::make_shared<CSRGraph>();

        {
            std::shared_lock lock(graph_mutex_);
            uint32_t num_nodes = static_cast<uint32_t>(adj_list_.size());
            new_csr->num_nodes = num_nodes;
            new_csr->row_ptr.resize(num_nodes + 1, 0);

            // Compute offsets
            uint32_t total_edges = 0;
            for (uint32_t i = 0; i < num_nodes; ++i) {
                total_edges += static_cast<uint32_t>(adj_list_[i].size());
                new_csr->row_ptr[i + 1] = total_edges;
            }

            new_csr->num_edges = total_edges;
            new_csr->col_ind.resize(total_edges);
            new_csr->weight_val.resize(total_edges);

            uint32_t edge_idx = 0;
            for (uint32_t u = 0; u < num_nodes; ++u) {
                for (const auto& edge : adj_list_[u]) {
                    new_csr->col_ind[edge_idx] = edge.target_id;
                    new_csr->weight_val[edge_idx] = get_weight(edge.type);
                    ++edge_idx;
                }
            }

            new_csr->snapshot_version = snapshot_counter_.fetch_add(1, std::memory_order_relaxed) + 1;
        }

        // Atomically replace active CSR projection pointer
        active_csr_.store(std::const_pointer_cast<const CSRGraph>(new_csr));
        return active_csr_.load();
    }

    // Get current active read-only CSR snapshot without holding write locks
    [[nodiscard]] std::shared_ptr<const CSRGraph> get_active_csr() const {
        return active_csr_.load();
    }

    // Read-only handle lookup (does not create a node)
    [[nodiscard]] std::optional<uint32_t> find_node_id(const std::string& handle) const {
        std::shared_lock lock(graph_mutex_);
        return handle_to_id_.find(handle);
    }

    // Read-only node metadata lookup by ID
    [[nodiscard]] std::optional<NodeMetadata> get_node_meta(uint32_t id) const {
        std::shared_lock lock(graph_mutex_);
        if (id < nodes_meta_.size()) return nodes_meta_[id];
        return std::nullopt;
    }

    [[nodiscard]] std::string get_handle(uint32_t id) const {
        std::shared_lock lock(graph_mutex_);
        if (id < id_to_handle_.size()) return id_to_handle_[id];
        return "user_" + std::to_string(id);
    }

    [[nodiscard]] std::vector<NodeMetadata> get_all_nodes_meta() const {
        std::shared_lock lock(graph_mutex_);
        return nodes_meta_;
    }

    void update_node_scores(const std::vector<double>& pr_scores, const std::vector<uint32_t>& communities) {
        std::unique_lock lock(graph_mutex_);
        size_t n = std::min(nodes_meta_.size(), pr_scores.size());
        for (size_t i = 0; i < n; ++i) {
            nodes_meta_[i].pagerank_score = pr_scores[i];
            if (i < communities.size()) {
                nodes_meta_[i].community_id = communities[i];
            }
        }
    }

    void mark_bot(uint32_t id, bool is_bot = true) {
        std::unique_lock lock(graph_mutex_);
        if (id < nodes_meta_.size()) {
            nodes_meta_[id].is_bot = is_bot;
        }
    }

    [[nodiscard]] const std::vector<XEdge>& get_edges(uint32_t node_id) const {
        std::shared_lock lock(graph_mutex_);
        static const std::vector<XEdge> empty_edges;
        if (node_id < adj_list_.size()) return adj_list_[node_id];
        return empty_edges;
    }

    [[nodiscard]] size_t node_count() const {
        std::shared_lock lock(graph_mutex_);
        return adj_list_.size();
    }

    [[nodiscard]] uint64_t total_edges() const {
        return total_edges_ingested_.load(std::memory_order_relaxed);
    }

    // ---------------- Persistence (binary snapshot) ----------------
    // Format: header {magic, version, saved_at, num_nodes, snapshot_counter}
    //         per-node metadata records, then per-node edge lists.
    // Intended for startup load / graceful shutdown save; not for concurrent hot reload.

    static constexpr uint32_t kSnapMagic = 0x494D5350u; // "IMSP"
    static constexpr uint32_t kSnapVersion = 1u;

    static void write_u16(std::ofstream& f, uint16_t v) { f.write(reinterpret_cast<const char*>(&v), 2); }
    static void write_u32(std::ofstream& f, uint32_t v) { f.write(reinterpret_cast<const char*>(&v), 4); }
    static void write_u64(std::ofstream& f, uint64_t v) { f.write(reinterpret_cast<const char*>(&v), 8); }
    static void write_f64(std::ofstream& f, double v) { f.write(reinterpret_cast<const char*>(&v), 8); }
    static void write_str(std::ofstream& f, const std::string& s) {
        write_u32(f, static_cast<uint32_t>(s.size()));
        if (!s.empty()) f.write(s.data(), static_cast<std::streamsize>(s.size()));
    }

    bool save_snapshot_file(const std::string& path) const {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f.is_open()) return false;

        std::shared_lock lock(graph_mutex_);
        uint32_t num_nodes = static_cast<uint32_t>(nodes_meta_.size());

        write_u32(f, kSnapMagic);
        write_u32(f, kSnapVersion);
        write_u64(f, static_cast<uint64_t>(std::time(nullptr)));
        write_u32(f, num_nodes);
        write_u64(f, snapshot_counter_.load(std::memory_order_relaxed));

        for (uint32_t i = 0; i < num_nodes; ++i) {
            const auto& m = nodes_meta_[i];
            write_str(f, m.handle);
            write_str(f, m.did);
            write_str(f, m.display_name);
            write_str(f, m.avatar_url);
            write_str(f, m.description);
            write_u32(f, m.followers_count);
            write_u32(f, m.following_count);
            write_f64(f, m.pagerank_score);
            write_u32(f, m.community_id);
            f.put(m.is_bot ? static_cast<char>(1) : static_cast<char>(0));
            write_u32(f, m.in_degree);
            write_u32(f, m.out_degree);
        }

        uint64_t edge_total = 0;
        for (uint32_t i = 0; i < num_nodes; ++i) edge_total += adj_list_[i].size();
        write_u64(f, edge_total);

        for (uint32_t i = 0; i < num_nodes; ++i) {
            const auto& edges = adj_list_[i];
            write_u32(f, static_cast<uint32_t>(edges.size()));
            for (const auto& e : edges) {
                write_u32(f, e.target_id);
                write_u32(f, e.timestamp_sec);
                f.put(static_cast<char>(e.type));
                write_u16(f, e.latency_ms);
            }
        }
        f.flush();
        return f.good();
    }

    // Loads a snapshot previously written by save_snapshot_file.
    // On any corruption the storage is reset to empty and false is returned.
    // out_saved_at (optional) receives the unix timestamp stored in the header.
    bool load_snapshot_file(const std::string& path, uint64_t* out_saved_at = nullptr) {
        std::ifstream f(path, std::ios::binary);
        if (!f.is_open()) return false;

        auto read_bytes = [&](void* dst, size_t n) -> bool {
            f.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(n));
            return f.gcount() == static_cast<std::streamsize>(n);
        };
        auto read_u8 = [&]() -> uint8_t { uint8_t v = 0; read_bytes(&v, 1); return v; };
        auto read_u16 = [&]() -> uint16_t { uint16_t v = 0; read_bytes(&v, 2); return v; };
        auto read_u32 = [&]() -> uint32_t { uint32_t v = 0; read_bytes(&v, 4); return v; };
        auto read_u64 = [&]() -> uint64_t { uint64_t v = 0; read_bytes(&v, 8); return v; };
        auto read_f64 = [&]() -> double { double v = 0.0; read_bytes(&v, 8); return v; };
        auto read_str = [&]() -> std::string {
            uint32_t len = read_u32();
            if (len > (1u << 28)) return {};
            std::string s(len, '\0');
            if (len && !read_bytes(s.data(), len)) return {};
            return s;
        };

        if (read_u32() != kSnapMagic) return false;
        if (read_u32() != kSnapVersion) return false;
        uint64_t saved_at = read_u64();
        uint32_t num_nodes = read_u32();
        uint64_t snap_counter = read_u64();
        if (!f.good() || num_nodes > 500000000u) return false;

        std::vector<NodeMetadata> meta(num_nodes);
        for (uint32_t i = 0; i < num_nodes; ++i) {
            auto& m = meta[i];
            m.handle = read_str();
            m.did = read_str();
            m.display_name = read_str();
            m.avatar_url = read_str();
            m.description = read_str();
            m.followers_count = read_u32();
            m.following_count = read_u32();
            m.pagerank_score = read_f64();
            m.community_id = read_u32();
            m.is_bot = read_u8() != 0;
            m.in_degree = read_u32();
            m.out_degree = read_u32();
            if (!f.good()) return false;
        }

        uint64_t edge_total = read_u64();
        if (!f.good()) return false;

        auto reset_storage = [&]() {
            std::unique_lock lock(graph_mutex_);
            handle_to_id_.clear();
            adj_list_.clear();
            nodes_meta_.clear();
            id_to_handle_.clear();
            total_edges_ingested_.store(0, std::memory_order_relaxed);
        };

        {
            std::unique_lock lock(graph_mutex_);
            handle_to_id_.clear();
            adj_list_.assign(num_nodes, {});
            nodes_meta_.assign(num_nodes, {});
            id_to_handle_.assign(num_nodes, {});

            for (uint32_t i = 0; i < num_nodes; ++i) {
                std::string h = meta[i].handle.empty()
                    ? ("user_" + std::to_string(i)) : meta[i].handle;
                uint32_t nid = handle_to_id_.get_or_create(h);
                if (nid != i) { reset_storage(); return false; }
                meta[i].id = i;
                meta[i].handle = h;
                nodes_meta_[i] = meta[i];
                id_to_handle_[i] = h;
            }

            for (uint32_t i = 0; i < num_nodes; ++i) {
                uint32_t cnt = read_u32();
                if (!f.good() || cnt > 100000000u) { reset_storage(); return false; }
                auto& out = adj_list_[i];
                out.reserve(cnt);
                for (uint32_t j = 0; j < cnt; ++j) {
                    XEdge e;
                    e.target_id = read_u32();
                    e.timestamp_sec = read_u32();
                    e.type = read_u8();
                    e.latency_ms = read_u16();
                    if (!f.good()) { reset_storage(); return false; }
                    out.push_back(e);
                }
            }

            total_edges_ingested_.store(edge_total, std::memory_order_relaxed);
            snapshot_counter_.store(snap_counter, std::memory_order_relaxed);
        }

        if (out_saved_at) *out_saved_at = saved_at;
        return true;
    }
};

} // namespace influmapper

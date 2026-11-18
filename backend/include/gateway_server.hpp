#pragma once

// GatewayServer — embedded HTTP + WebSocket server for the analytics engine.
// Zero external dependencies: raw sockets (Winsock/POSIX), hand-rolled
// RFC 6455 via websocket.hpp, JSON via json.hpp.
//
// Endpoints:
//   GET  /                     -> serves frontend/index.html
//   GET  /api/health           -> node/edge counts + load flags
//   GET  /api/graph            -> full graph topology (nodes + edges)
//   GET  /api/candidates       -> ambassador candidates ranked by PageRank
//   GET  /api/node?handle=X    -> single node lookup (404 if unknown)
//   GET  /api/refresh          -> re-ingest Bluesky, re-run analytics, persist
//   GET  /api/cascade?node_id&lambda&kappa&seed -> probabilistic decay cascade
//   GET  /api/cascade/compare?node_ids=1,2,3&lambda&kappa&seed -> multi-seed compare
//   GET  /ws                   -> WebSocket stream (health ticks + graph_updated)

#include "types.hpp"
#include "graph_storage.hpp"
#include "analytics.hpp"
#include "bluesky_ingestion.hpp"
#include "websocket.hpp"
#include "json.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <iomanip>
#include <cstring>
#include <cctype>
#include <unordered_set>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#ifndef SOCKET
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close
#endif
#endif

namespace influmapper {

// ============================================================================
// HTTP / request helpers (header-inline, stateless)
// ============================================================================

// Send a complete HTTP response; caller closes the socket.
// extra_headers: optional raw header lines (e.g. Content-Disposition) inserted
// before Content-Length.
inline void http_send_response(SOCKET client, int status, const char* reason,
                               const char* content_type, const std::string& body,
                               const std::string& extra_headers = "") {
    std::ostringstream resp;
    resp << "HTTP/1.1 " << status << " " << reason << "\r\n"
         << "Content-Type: " << content_type << "\r\n"
         << "Access-Control-Allow-Origin: *\r\n"
         << extra_headers
         << "Content-Length: " << body.size() << "\r\n"
         << "Connection: close\r\n\r\n"
         << body;
    std::string s = resp.str();
    send(client, s.data(), static_cast<int>(s.size()), 0);
}

inline void http_send_json(SOCKET client, int status, const char* reason,
                           const json::Value& body) {
    http_send_response(client, status, reason, "application/json", body.dump());
}

// Extract "key=value" from the query string of a request path.
inline std::string http_query_param(const std::string& path, const std::string& key) {
    size_t qpos = path.find('?');
    if (qpos == std::string::npos) return "";
    std::string query = path.substr(qpos + 1);

    const std::string prefix = key + "=";
    size_t pos = 0;
    while (pos <= query.size()) {
        size_t amp = query.find('&', pos);
        std::string pair = query.substr(pos, amp == std::string::npos
            ? std::string::npos : amp - pos);
        if (pair.rfind(prefix, 0) == 0) return pair.substr(prefix.size());
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    return "";
}

// Minimal percent-decoding (+ -> space, %XX -> byte).
inline std::string http_url_decode(const std::string& s) {
    auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int hi = hex(s[i + 1]), lo = hex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out += static_cast<char>((hi << 4) | lo);
                i += 2;
                continue;
            }
        }
        out += (s[i] == '+') ? ' ' : s[i];
    }
    return out;
}

// Format a double with fixed precision (e.g. fmt_fixed(0.0832, 4) -> "0.0832").
inline std::string http_fmt_fixed(double v, int prec) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.*f", prec, v);
    return buf;
}

// True when the raw request targets /ws AND carries a WebSocket Upgrade header.
inline bool http_is_websocket_upgrade(const std::string& path, const std::string& request) {
    if (!(path == "/ws" || path.rfind("/ws?", 0) == 0)) return false;
    std::string upgrade = ws::get_header(request, "Upgrade");
    for (auto& c : upgrade) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return upgrade.find("websocket") != std::string::npos;
}

// ============================================================================
// GatewayServer
// ============================================================================

class GatewayServer {
private:
    int port_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    GraphStorage& storage_;
    BlueskyIngestionEngine& bsky_engine_;
    bool bluesky_loaded_{false};
    bool twitter_loaded_{false};
    bool instagram_loaded_{false};
    bool threads_loaded_{false};
    bool snapshot_loaded_{false};
    std::string seed_handle_ = "cry-ptos.bsky.social";
    std::string snapshot_path_ = "graph_snapshot.bin";
    SOCKET listen_sock_{INVALID_SOCKET};

    // WebSocket state: connected clients + broadcast mutex
    std::mutex ws_mutex_;
    std::unordered_set<SOCKET> ws_clients_;
    std::chrono::steady_clock::time_point start_time_ = std::chrono::steady_clock::now();

    // ---------------- JSON builders ----------------

    json::Value build_health_json() const {
        auto csr = storage_.get_active_csr();
        size_t node_count = csr ? csr->num_nodes : storage_.node_count();
        uint64_t total_edges = csr ? csr->num_edges : 0;
        uint64_t snapshot_ver = csr ? csr->snapshot_version : 0;

        return json::obj({
            {"status", "ok"},
            {"nodes", node_count},
            {"edges", total_edges},
            {"snapshot_version", snapshot_ver},
            {"bluesky_loaded", bluesky_loaded_},
            {"twitter_loaded", twitter_loaded_},
            {"instagram_loaded", instagram_loaded_},
            {"threads_loaded", threads_loaded_},
            {"snapshot_loaded", snapshot_loaded_}
        });
    }

    // Shared node-type heuristic used by the graph endpoint.
    static const char* node_type_of(const NodeMetadata& n) {
        if (n.is_bot) return "bot";
        if (n.handle == "cry-ptos.bsky.social" || n.handle == "bsky.app" || n.followers_count > 30000)
            return "influencer";
        return "follower";
    }

    json::Value node_to_graph_json(const NodeMetadata& n) const {
        return json::obj({
            {"id", n.handle},
            {"node_id", n.id},
            {"label", "@" + n.handle},
            {"handle", n.handle},
            {"did", n.did},
            {"displayName", n.display_name.empty() ? n.handle : n.display_name},
            {"avatar", n.avatar_url},
            {"followersCount", n.followers_count},
            {"followingCount", n.following_count},
            {"pageRank", n.pagerank_score},
            {"community", n.community_id},
            {"in_degree", n.in_degree},
            {"out_degree", n.out_degree},
            {"is_bot", n.is_bot},
            {"type", node_type_of(n)},
            {"niche", classify_niche(n.handle)}
        });
    }

    json::Value build_graph_json() {
        auto nodes = storage_.get_all_nodes_meta();
        auto csr = storage_.get_active_csr();

        json::Value nodes_arr = json::array();
        for (const auto& n : nodes) nodes_arr.arr.push_back(node_to_graph_json(n));

        // Edges (iterate the dynamic adjacency lists; CSR loses edge types)
        json::Value edges_arr = json::array();
        for (size_t u = 0; u < nodes.size(); ++u) {
            for (const auto& e : storage_.get_edges(static_cast<uint32_t>(u))) {
                if (e.target_id >= nodes.size()) continue;
                bool collab = (nodes[u].handle == seed_handle_ ||
                               nodes[e.target_id].handle == "bsky.app");
                edges_arr.arr.push_back(json::obj({
                    {"id", "e_" + std::to_string(u) + "_" + std::to_string(e.target_id)},
                    {"source", nodes[u].handle},
                    {"target", nodes[e.target_id].handle},
                    {"type", collab ? "collab_edge" : "default"},
                    {"weight", e.type == 2 ? 3.0 : 1.0}
                }));
            }
        }

        return json::obj({
            {"status", "success"},
            {"num_nodes", nodes.size()},
            {"num_edges", csr ? csr->num_edges : 0},
            {"nodes", std::move(nodes_arr)},
            {"edges", std::move(edges_arr)}
        });
    }

    // One row of the ambassador shortlist — shared by /api/candidates (JSON)
    // and /api/export/candidates (CSV/JSON download) so both stay in sync.
    struct CandidateRow {
        std::string handle;
        std::string display_name;
        std::string avatar;
        std::string platform;
        std::string community;
        uint32_t node_id = 0;
        double page_rank = 0.0;
        std::string page_rank_str;
        std::string cascade_reach;
        std::string cascade_depth;
        std::string bot_risk;
        std::string fit_score;
        bool is_bot_ring = false;
        bool recommended = false;
        uint32_t followers = 0;
        std::string niche = "Other";
    };

    static std::string candidate_platform_of(const std::string& handle) {
        if (handle.rfind("ig:", 0) == 0) return "Instagram";
        if (handle.rfind("th:", 0) == 0) return "Threads (Synthetic)";
        if (handle.rfind("tw:", 0) == 0) return "X / Twitter (CSV)";
        return "Bluesky (AT Protocol)";
    }

    // Keyword-based niche classifier — first match wins, so order matters
    // (specific communities before broad tech terms).
    static std::string classify_niche(const std::string& handle) {
        std::string s = handle;
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c){ return std::tolower(c); });
        auto has = [&](std::initializer_list<const char*> kws) {
            for (auto k : kws) if (s.find(k) != std::string::npos) return true;
            return false;
        };
        if (has({"f1","racing","redbull","williams","haas","mercedes","audi",
                 "bmw","formula","motorsport","motogp","nascar"})) return "Motorsport";
        if (has({"fashion","style","ootd","outfit","denim","velvet","capsule",
                 "thrift","closet","street","lipliner","lace","runway"})) return "Fashion & Style";
        if (has({"vinyl","music","band","sound","hiphop","dj","concert",
                 "openmic","playlist"})) return "Music";
        if (has({"food","cook","sourdough","recipe","kitchen","bakery","chef",
                 "vegan","foodie","ramen","dessert","plantbased"})) return "Food & Cooking";
        if (has({"fitness","gym","hiit","protein","lift","yoga","marathon",
                 "athlete","coach","pilates","crossfit","runclub","mobility",
                 "stretch","macro"})) return "Fitness & Health";
        if (has({"travel","passport","wander","nomad","journey","trip","roam",
                 "backpack","explore","citywalks","hostel","coastline"})) return "Travel";
        if (has({"ink","gallery","print","design","photo","sketch","art",
                 "illustration","maker","brushbox","clay"})) return "Art & Design";
        if (has({"glow","beauty","skin","makeup","hair","salon","nails"})) return "Beauty & Grooming";
        if (has({"tech","code","dev","pixel","gadget","cyber","web3","crypto",
                 "blockchain","nft","ai","cloud","kernel","software","app",
                 "buildinpublic","startup","data","opensource","forge"})) return "Tech & Digital";
        return "Other";
    }

    std::vector<CandidateRow> collect_candidates() {
        auto nodes = storage_.get_all_nodes_meta();
        std::vector<NodeMetadata> sorted = nodes;
        std::sort(sorted.begin(), sorted.end(), [](const NodeMetadata& a, const NodeMetadata& b) {
            return a.pagerank_score > b.pagerank_score;
        });

        std::vector<CandidateRow> rows;
        rows.reserve(sorted.size());
        for (const auto& c : sorted) {
            int fit = std::min(100, std::max(60, static_cast<int>(c.pagerank_score * 800 + 70)));
            int depth = std::min(6, std::max(2, static_cast<int>(c.in_degree + c.out_degree) / 2 + 2));
            CandidateRow r;
            r.handle = c.handle;
            r.display_name = c.display_name.empty() ? c.handle : c.display_name;
            r.avatar = c.avatar_url;
            r.platform = candidate_platform_of(c.handle);
            r.community = "Motorsport & Tech Cluster (ID " + std::to_string(c.community_id) + ")";
            r.node_id = c.id;
            r.page_rank = c.pagerank_score;
            r.page_rank_str = http_fmt_fixed(c.pagerank_score, 4);
            r.cascade_reach = std::to_string(c.followers_count > 0
                ? c.followers_count : (c.in_degree * 5000 + 1200)) + "+ Reach";
            r.cascade_depth = std::to_string(depth) + " Tiers";
            r.bot_risk = c.is_bot ? "94.2% (BOT RING DETECTED)" : "0.5% (Safe)";
            r.is_bot_ring = c.is_bot;
            r.recommended = !c.is_bot && fit >= 80;
            r.fit_score = std::to_string(fit) + " / 100";
            r.followers = c.followers_count;
            r.niche = classify_niche(c.handle);
            rows.push_back(std::move(r));
        }
        return rows;
    }

    json::Value build_candidates_json() {
        json::Value arr = json::array();
        for (const auto& r : collect_candidates()) {
            arr.arr.push_back(json::obj({
                {"id", r.handle},
                {"node_id", r.node_id},
                {"handle", r.handle},
                {"displayName", r.display_name},
                {"avatar", r.avatar},
                {"platform", r.platform},
                {"community", r.community},
                {"pageRank", r.page_rank_str},
                {"cascadeReach", r.cascade_reach},
                {"cascadeDepth", r.cascade_depth},
                {"botRisk", r.bot_risk},
                {"isBotRing", r.is_bot_ring},
                {"recommended", r.recommended},
                {"fitScore", r.fit_score},
                {"followersCount", r.followers},
                {"niche", r.niche}
            }));
        }
        return arr;
    }

    json::Value build_node_json(const NodeMetadata& m) const {
        return json::obj({
            {"id", m.handle},
            {"node_id", m.id},
            {"handle", m.handle},
            {"did", m.did},
            {"displayName", m.display_name.empty() ? m.handle : m.display_name},
            {"avatar", m.avatar_url},
            {"followersCount", m.followers_count},
            {"followingCount", m.following_count},
            {"pageRank", m.pagerank_score},
            {"community", m.community_id},
            {"in_degree", m.in_degree},
            {"out_degree", m.out_degree},
            {"is_bot", m.is_bot},
            {"niche", classify_niche(m.handle)}
        });
    }

    json::Value cascade_result_json(const CascadeResult& res) const {
        json::Value tiers = json::array();
        for (const auto& t : res.tiers) {
            tiers.arr.push_back(json::obj({
                {"tier", t.tier},
                {"attempted", t.attempted},
                {"reached", t.reached},
                {"reach_ratio", t.reach_ratio}
            }));
        }

        json::Value nodes = json::array();
        for (const auto& s : res.steps) {
            nodes.arr.push_back(json::obj({
                {"handle", storage_.get_handle(s.node_id)},
                {"node_id", s.node_id},
                {"tier", s.depth},
                {"isLeaf", s.is_leaf}
            }));
        }

        return json::obj({
            {"tiers", std::move(tiers)},
            {"total_attempted", res.total_attempted},
            {"total_reached", res.total_reached},
            {"actual_reach_ratio", res.actual_reach_ratio},
            {"nodes", std::move(nodes)}
        });
    }

    // ---------------- WebSocket ----------------

    // Broadcast a JSON event to all connected WebSocket clients.
    // Dead sockets are detected on send failure and removed.
    void broadcast_event(const std::string& json) {
        std::vector<SOCKET> snapshot;
        {
            std::lock_guard<std::mutex> lock(ws_mutex_);
            snapshot.assign(ws_clients_.begin(), ws_clients_.end());
        }
        std::vector<SOCKET> dead;
        for (SOCKET s : snapshot) {
            if (!ws::send_text(s, json)) dead.push_back(s);
        }
        if (!dead.empty()) {
            std::lock_guard<std::mutex> lock(ws_mutex_);
            for (SOCKET s : dead) ws_clients_.erase(s);
            for (SOCKET s : dead) closesocket(s);
        }
    }

    void broadcast_json(const json::Value& v) {
        broadcast_event(v.dump());
    }

    // {"type":"health","uptime_s":N, ...health fields...} — used for the
    // connect-time push and the periodic ticker.
    json::Value health_event_json() const {
        auto up = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start_time_).count();
        json::Value msg = json::obj({
            {"type", "health"},
            {"uptime_s", static_cast<int64_t>(up)}
        });
        for (auto& f : build_health_json().obj) msg.obj.push_back(std::move(f));
        return msg;
    }

    // RFC 6455 WebSocket session: handshake, register, then frame loop.
    void handle_websocket(SOCKET client, const std::string& request) {
        std::string key = ws::get_header(request, "Sec-WebSocket-Key");
        if (key.empty()) {
            const char* bad = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            send(client, bad, static_cast<int>(strlen(bad)), 0);
            closesocket(client);
            return;
        }

        // Short send timeout so broadcasts can't stall on dead peers
        int snd_timeout_ms = 3000;
        setsockopt(client, SOL_SOCKET, SO_SNDTIMEO,
                   reinterpret_cast<const char*>(&snd_timeout_ms), sizeof(snd_timeout_ms));

        std::string resp = ws::handshake_response(key);
        if (!ws::send_all(client, resp.data(), resp.size())) {
            closesocket(client);
            return;
        }

        {
            std::lock_guard<std::mutex> lock(ws_mutex_);
            ws_clients_.insert(client);
        }

        // Send initial health snapshot immediately
        broadcast_json(health_event_json());

        // Frame loop: respond to ping, exit on close/error
        while (running_) {
            uint8_t opcode = 0;
            std::string payload;
            if (!ws::recv_frame(client, opcode, payload)) break;
            if (opcode == 0x8) {           // close
                ws::send_close(client);
                break;
            } else if (opcode == 0x9) {    // ping -> pong
                if (!ws::send_pong(client, payload)) break;
            }
            // text/binary/pong: ignore (server pushes only)
        }

        {
            std::lock_guard<std::mutex> lock(ws_mutex_);
            ws_clients_.erase(client);
        }
        closesocket(client);
    }

    // ---------------- Endpoint handlers ----------------

    void handle_health(SOCKET client) {
        http_send_json(client, 200, "OK", build_health_json());
    }

    void handle_graph(SOCKET client) {
        http_send_json(client, 200, "OK", build_graph_json());
    }

    void handle_candidates(SOCKET client) {
        http_send_json(client, 200, "OK", build_candidates_json());
    }

    // CSV field escaping: quote when the value contains , " or newline.
    static std::string csv_escape(const std::string& s) {
        if (s.find_first_of(",\"\n\r") == std::string::npos) return s;
        std::string out = "\"";
        for (char c : s) {
            if (c == '"') out += "\"\"";
            else out += c;
        }
        return out + "\"";
    }

    // GET /api/export/candidates?format=csv|json — downloadable shortlist.
    void handle_export_candidates(SOCKET client, const std::string& path) {
        std::string fmt = http_query_param(path, "format");
        if (fmt.empty()) fmt = "csv";
        auto rows = collect_candidates();

        if (fmt == "json") {
            json::Value arr = json::array();
            for (size_t i = 0; i < rows.size(); ++i) {
                const auto& r = rows[i];
                arr.arr.push_back(json::obj({
                    {"rank", static_cast<int64_t>(i + 1)},
                    {"node_id", r.node_id},
                    {"handle", r.handle},
                    {"displayName", r.display_name},
                    {"platform", r.platform},
                    {"community", r.community},
                    {"pageRank", r.page_rank},
                    {"cascadeReach", r.cascade_reach},
                    {"cascadeDepth", r.cascade_depth},
                    {"botRisk", r.bot_risk},
                    {"isBotRing", r.is_bot_ring},
                    {"recommended", r.recommended},
                    {"fitScore", r.fit_score},
                    {"followersCount", r.followers},
                    {"niche", r.niche}
                }));
            }
            http_send_response(client, 200, "OK", "application/json",
                json::obj({{"candidates", std::move(arr)},
                           {"count", rows.size()},
                           {"generated_at", static_cast<int64_t>(
                               std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count())}}).dump(),
                "Content-Disposition: attachment; filename=\"ambassador_shortlist.json\"\r\n");
            return;
        }

        if (fmt != "csv") {
            http_send_json(client, 400, "Bad Request",
                          json::obj({{"error", "format must be csv or json"}}));
            return;
        }

        std::ostringstream csv;
        csv << "rank,node_id,handle,display_name,platform,niche,community,page_rank,"
               "cascade_reach,cascade_depth,bot_risk,is_bot_ring,recommended,"
               "fit_score,followers_count\n";
        int rank = 0;
        for (const auto& r : rows) {
            csv << ++rank << ','
                << r.node_id << ','
                << csv_escape(r.handle) << ','
                << csv_escape(r.display_name) << ','
                << csv_escape(r.platform) << ','
                << csv_escape(r.niche) << ','
                << csv_escape(r.community) << ','
                << r.page_rank_str << ','
                << csv_escape(r.cascade_reach) << ','
                << csv_escape(r.cascade_depth) << ','
                << csv_escape(r.bot_risk) << ','
                << (r.is_bot_ring ? "true" : "false") << ','
                << (r.recommended ? "true" : "false") << ','
                << csv_escape(r.fit_score) << ','
                << r.followers << '\n';
        }
        http_send_response(client, 200, "OK", "text/csv; charset=utf-8", csv.str(),
            "Content-Disposition: attachment; filename=\"ambassador_shortlist.csv\"\r\n");
    }

    void handle_node(SOCKET client, const std::string& path) {
        std::string handle = http_url_decode(http_query_param(path, "handle"));

        auto nid = storage_.find_node_id(handle);
        if (!nid.has_value()) {
            http_send_json(client, 404, "Not Found",
                          json::obj({{"error", "handle not found in graph"}}));
            return;
        }
        auto meta = storage_.get_node_meta(*nid);
        if (!meta.has_value()) {
            http_send_json(client, 404, "Not Found",
                          json::obj({{"error", "node metadata unavailable"}}));
            return;
        }
        http_send_json(client, 200, "OK", build_node_json(*meta));
    }

    void handle_refresh(SOCKET client) {
        // Re-ingest real network live (dedup prevents edge duplication)
        bsky_engine_.ingest_real_network(seed_handle_);
        auto csr = storage_.publish_csr_snapshot();
        auto pr = AnalyticsEngine::compute_pagerank(*csr);
        auto lpa = AnalyticsEngine::compute_lpa(*csr);
        storage_.update_node_scores(pr, lpa);

        // Persist refreshed graph and notify WebSocket clients
        storage_.save_snapshot_file(snapshot_path_);
        broadcast_json(json::obj({
            {"type", "graph_updated"},
            {"health", build_health_json()}
        }));

        http_send_json(client, 200, "OK", json::obj({
            {"status", "refreshed"},
            {"nodes", storage_.node_count()}
        }));
    }

    // Parse lambda/kappa/seed decay params (clamped to [0,1] / uint64).
    void parse_decay_params(const std::string& path, CascadeConfig& cfg) {
        auto parse_double_param = [&](const std::string& s, double& out) {
            if (s.empty()) return;
            try {
                size_t used = 0;
                double v = std::stod(s, &used);
                if (used == s.size()) out = v;
            } catch (...) {}
        };
        parse_double_param(http_query_param(path, "lambda"), cfg.decay_rate_lambda);
        parse_double_param(http_query_param(path, "kappa"), cfg.fatigue_rate_kappa);
        cfg.decay_rate_lambda = std::min(1.0, std::max(0.0, cfg.decay_rate_lambda));
        cfg.fatigue_rate_kappa = std::min(1.0, std::max(0.0, cfg.fatigue_rate_kappa));

        std::string seed_str = http_query_param(path, "seed");
        if (!seed_str.empty()) {
            try {
                size_t used = 0;
                unsigned long long v = std::stoull(seed_str, &used);
                if (used == seed_str.size()) cfg.rng_seed = v;
            } catch (...) {}
        }
    }

    // Shared node_id validation for single-seed endpoints. Sends 400 on failure.
    bool parse_node_id_param(SOCKET client, const std::string& path, uint32_t& node_id) {
        std::string nid_str = http_query_param(path, "node_id");
        bool nid_valid = !nid_str.empty() &&
            std::all_of(nid_str.begin(), nid_str.end(),
                        [](unsigned char c) { return std::isdigit(c) != 0; });
        if (nid_valid) {
            try {
                unsigned long long v = std::stoull(nid_str);
                nid_valid = (v <= 0xFFFFFFFFULL);
                node_id = static_cast<uint32_t>(v);
            } catch (...) { nid_valid = false; }
        }
        if (!nid_valid) {
            http_send_json(client, 400, "Bad Request",
                          json::obj({{"error", "Missing or invalid node_id"}}));
            return false;
        }
        return true;
    }

    void handle_cascade(SOCKET client, const std::string& path) {
        // /api/cascade?node_id=<id>&lambda=<d>&kappa=<d>&seed=<int>
        CascadeConfig cfg;
        uint32_t node_id = 0;
        if (!parse_node_id_param(client, path, node_id)) return;
        parse_decay_params(path, cfg);

        auto csr = storage_.get_active_csr();
        uint32_t nn = csr ? csr->num_nodes : 0;
        if (node_id >= nn) {
            http_send_json(client, 404, "Not Found",
                          json::obj({{"error", "node_id out of range"}}));
            return;
        }

        CascadeResult res = AnalyticsEngine::trace_virality_cascade_with_decay(
            *csr, storage_, node_id, cfg);
        http_send_json(client, 200, "OK", cascade_result_json(res));
    }

    void handle_cascade_compare(SOCKET client, const std::string& path) {
        // /api/cascade/compare?node_ids=1,2,3&lambda=<d>&kappa=<d>&seed=<int>
        // Runs the same decay cascade for each seed and returns all results.
        CascadeConfig cfg;
        parse_decay_params(path, cfg);

        std::string ids_str = http_query_param(path, "node_ids");
        std::vector<uint32_t> node_ids;
        size_t pos = 0;
        while (pos <= ids_str.size() && node_ids.size() < 8) { // cap at 8 seeds
            size_t comma = ids_str.find(',', pos);
            std::string tok = ids_str.substr(pos, comma == std::string::npos
                ? std::string::npos : comma - pos);
            bool valid = !tok.empty() &&
                std::all_of(tok.begin(), tok.end(),
                            [](unsigned char c) { return std::isdigit(c) != 0; });
            if (valid) {
                try {
                    unsigned long long v = std::stoull(tok);
                    if (v <= 0xFFFFFFFFULL) node_ids.push_back(static_cast<uint32_t>(v));
                } catch (...) {}
            }
            if (comma == std::string::npos) break;
            pos = comma + 1;
        }

        if (node_ids.empty()) {
            http_send_json(client, 400, "Bad Request",
                          json::obj({{"error", "Missing or invalid node_ids (comma-separated)"}}));
            return;
        }

        auto csr = storage_.get_active_csr();
        uint32_t nn = csr ? csr->num_nodes : 0;

        json::Value results;
        results.type = json::Value::Type::Array;
        for (uint32_t nid : node_ids) {
            if (nid >= nn) {
                results.arr.push_back(json::obj({
                    {"node_id", nid},
                    {"error", "node_id out of range"}
                }));
                continue;
            }
            CascadeResult res = AnalyticsEngine::trace_virality_cascade_with_decay(
                *csr, storage_, nid, cfg);
            json::Value entry = cascade_result_json(res);
            // prepend seed identity for the comparison table
            entry.obj.insert(entry.obj.begin(), {"seed_handle", storage_.get_handle(nid)});
            entry.obj.insert(entry.obj.begin(), {"seed_node_id", json::integer(nid)});
            results.arr.push_back(std::move(entry));
        }

        http_send_json(client, 200, "OK", json::obj({
            {"results", std::move(results)}
        }));
    }

    void serve_frontend(SOCKET client) {
        // Try backend-relative path first, then repo-root-relative
        std::string file_path = "frontend/index.html";
        std::ifstream file(file_path);
        if (!file.is_open()) {
            file_path = "../frontend/index.html";
            file.open(file_path);
        }

        if (file.is_open()) {
            std::stringstream buffer;
            buffer << file.rdbuf();
            http_send_response(client, 200, "OK", "text/html; charset=utf-8", buffer.str(),
                "Cache-Control: no-store\r\n");
        } else {
            http_send_response(client, 500, "Internal Server Error", "text/plain; charset=utf-8",
                               "500 Internal Server Error: frontend/index.html not found");
        }
    }

    // ---------------- Request router ----------------

    void handle_client(SOCKET client) {
        char buf[4096];
        int bytes = recv(client, buf, sizeof(buf) - 1, 0);
        if (bytes <= 0) {
            closesocket(client);
            return;
        }
        buf[bytes] = '\0';
        std::string request(buf);

        std::string method, path;
        std::istringstream req_stream(request);
        req_stream >> method >> path;

        // CORS preflight
        if (method == "OPTIONS") {
            const char* resp = "HTTP/1.1 204 No Content\r\n"
                               "Access-Control-Allow-Origin: *\r\n"
                               "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
                               "Access-Control-Allow-Headers: Content-Type\r\n"
                               "Connection: close\r\n\r\n";
            send(client, resp, static_cast<int>(strlen(resp)), 0);
            closesocket(client);
            return;
        }

        // WebSocket upgrade endpoint (RFC 6455)
        if (http_is_websocket_upgrade(path, request)) {
            handle_websocket(client, request);
            return;
        }
        if (path == "/ws" || path.rfind("/ws?", 0) == 0) {
            const char* resp = "HTTP/1.1 426 Upgrade Required\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 26\r\n"
                               "Connection: close\r\n\r\n"
                               "Expected WebSocket upgrade";
            send(client, resp, static_cast<int>(strlen(resp)), 0);
            closesocket(client);
            return;
        }

        // Route on the path portion before any query string
        std::string base = path.substr(0, path.find('?'));
        if      (base == "/api/health")          handle_health(client);
        else if (base == "/api/graph")           handle_graph(client);
        else if (base == "/api/candidates")      handle_candidates(client);
        else if (base == "/api/export/candidates") handle_export_candidates(client, path);
        else if (base == "/api/node")            handle_node(client, path);
        else if (base == "/api/refresh")         handle_refresh(client);
        else if (base == "/api/cascade")         handle_cascade(client, path);
        else if (base == "/api/cascade/compare") handle_cascade_compare(client, path);
        else if (base == "/api" || base.rfind("/api/", 0) == 0) {
            http_send_json(client, 404, "Not Found",
                          json::obj({
                              {"error", "Not Found"},
                              {"path", path}
                          }));
        } else {
            serve_frontend(client);
        }
        closesocket(client);
    }

public:
    GatewayServer(int port, GraphStorage& storage, BlueskyIngestionEngine& bsky_engine,
                  bool bluesky_loaded = false, bool twitter_loaded = false,
                  const std::string& seed_handle = "cry-ptos.bsky.social",
                  bool snapshot_loaded = false,
                  const std::string& snapshot_path = "graph_snapshot.bin",
                  bool instagram_loaded = false,
                  bool threads_loaded = false)
        : port_(port), storage_(storage), bsky_engine_(bsky_engine),
          bluesky_loaded_(bluesky_loaded), twitter_loaded_(twitter_loaded),
          instagram_loaded_(instagram_loaded), threads_loaded_(threads_loaded),
          snapshot_loaded_(snapshot_loaded),
          seed_handle_(seed_handle.empty() ? "cry-ptos.bsky.social" : seed_handle),
          snapshot_path_(snapshot_path) {}

    ~GatewayServer() {
        stop();
    }

    void start() {
        running_ = true;
        server_thread_ = std::thread([this]() {
#ifdef _WIN32
            WSADATA wsa;
            WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
            listen_sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (listen_sock_ == INVALID_SOCKET) {
                std::cerr << "[Server Error] Failed to create socket." << std::endl;
                return;
            }

            int opt = 1;
            setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

            sockaddr_in server_addr{};
            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = INADDR_ANY;
            server_addr.sin_port = htons(static_cast<u_short>(port_));

            if (bind(listen_sock_, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == SOCKET_ERROR) {
                std::cerr << "[Server Error] Failed to bind to port " << port_ << std::endl;
                closesocket(listen_sock_);
                return;
            }

            if (listen(listen_sock_, SOMAXCONN) == SOCKET_ERROR) {
                std::cerr << "[Server Error] Failed to listen on socket." << std::endl;
                closesocket(listen_sock_);
                return;
            }

            std::cout << "[Server] C++ Gateway Server listening on http://localhost:" << port_ << std::endl;
            std::cout << "[Server] Health API: http://localhost:" << port_ << "/api/health" << std::endl;
            std::cout << "[Server] Graph API: http://localhost:" << port_ << "/api/graph" << std::endl;
            std::cout << "[Server] Candidates API: http://localhost:" << port_ << "/api/candidates" << std::endl;
            std::cout << "[Server] WebSocket Stream: ws://localhost:" << port_ << "/ws" << std::endl;

            // Health ticker: broadcast stats to WebSocket clients every 15s
            std::thread([this]() {
                while (running_) {
                    std::this_thread::sleep_for(std::chrono::seconds(15));
                    if (!running_) break;
                    broadcast_json(health_event_json());
                }
            }).detach();

            // Accept loop: one detached thread per client so long-lived
            // WebSocket sessions never block HTTP request handling.
            while (running_) {
                fd_set readfds;
                FD_ZERO(&readfds);
                FD_SET(listen_sock_, &readfds);

                timeval tv{0, 200000}; // 200ms timeout
                int sel = select(0, &readfds, nullptr, nullptr, &tv);
                if (sel > 0 && FD_ISSET(listen_sock_, &readfds)) {
                    sockaddr_in client_addr{};
                    int client_len = sizeof(client_addr);
                    SOCKET client = accept(listen_sock_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
                    if (client != INVALID_SOCKET) {
                        std::thread([this, client]() { handle_client(client); }).detach();
                    }
                }
            }

            if (listen_sock_ != INVALID_SOCKET) {
                closesocket(listen_sock_);
                listen_sock_ = INVALID_SOCKET;
            }
#ifdef _WIN32
            WSACleanup();
#endif
        });
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (listen_sock_ != INVALID_SOCKET) {
            closesocket(listen_sock_);
            listen_sock_ = INVALID_SOCKET;
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }
};

} // namespace influmapper

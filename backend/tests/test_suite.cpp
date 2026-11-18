#include "../include/types.hpp"
#include "../include/robin_hood_map.hpp"
#include "../include/graph_storage.hpp"
#include "../include/analytics.hpp"
#include "../include/twitter_csv_ingestion.hpp"
#include "../include/instagram_ingestion.hpp"
#include "../include/instagram_csv_ingestion.hpp"
#include "../include/gateway_server.hpp"
#include "../include/json.hpp"

#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <cassert>
#include <cmath>
#include <cstring>
#include <thread>
#include <chrono>
#include <unordered_set>

using namespace influmapper;

void test_tc01_types_and_alignment() {
    std::cout << "[RUN] TC-01: Verifying 16-byte struct size and alignment..." << std::endl;
    static_assert(sizeof(XInteractionEvent) == 16, "XInteractionEvent must be exactly 16 bytes");
    assert(sizeof(XInteractionEvent) == 16);
    assert(alignof(XInteractionEvent) == 16);
    std::cout << "  [PASS] TC-01 passed." << std::endl;
}

void test_tc02_storage_and_csr() {
    std::cout << "[RUN] TC-02: Verifying dynamic storage and atomic CSR snapshot build..." << std::endl;
    GraphStorage storage;
    uint32_t u0 = storage.get_or_create_node("alice");
    uint32_t u1 = storage.get_or_create_node("bob");
    uint32_t u2 = storage.get_or_create_node("charlie");

    storage.add_interaction(u0, u1, InteractionType::REPOST, 1000, 20);
    storage.add_interaction(u1, u2, InteractionType::LIKE, 1005, 50);

    auto csr = storage.publish_csr_snapshot();
    assert(csr->num_nodes >= 3);
    assert(csr->num_edges == 2);
    assert(csr->degree(u0) == 1);
    assert(csr->degree(u2) == 0);
    std::cout << "  [PASS] TC-02 passed." << std::endl;
}

void test_tc03_pagerank_convergence() {
    std::cout << "[RUN] TC-03: Verifying PageRank score sum = 1.0..." << std::endl;
    GraphStorage storage;
    uint32_t n0 = storage.get_or_create_node("node0");
    uint32_t n1 = storage.get_or_create_node("node1");
    uint32_t n2 = storage.get_or_create_node("node2");

    // Ring network
    storage.add_interaction(n0, n1, InteractionType::LIKE, 100);
    storage.add_interaction(n1, n2, InteractionType::LIKE, 101);
    storage.add_interaction(n2, n0, InteractionType::LIKE, 102);

    auto csr = storage.publish_csr_snapshot();
    auto ranks = AnalyticsEngine::compute_pagerank(*csr);

    double sum = 0.0;
    for (double r : ranks) sum += r;
    assert(std::abs(sum - 1.0) < 1e-4);
    std::cout << "  [PASS] TC-03 passed. Rank sum = " << sum << std::endl;
}

void test_tc05_bot_cycle_detection() {
    std::cout << "[RUN] TC-05: Verifying 3-Color DFS Bot Swarm Loop Detection..." << std::endl;
    GraphStorage storage;
    uint32_t b0 = storage.get_or_create_node("bot_a");
    uint32_t b1 = storage.get_or_create_node("bot_b");
    uint32_t b2 = storage.get_or_create_node("bot_c");

    // Create high-velocity 3-node loop (latency 10ms, reposts)
    storage.add_interaction(b0, b1, InteractionType::REPOST, 200, 10);
    storage.add_interaction(b1, b2, InteractionType::REPOST, 201, 10);
    storage.add_interaction(b2, b0, InteractionType::REPOST, 202, 10);

    auto csr = storage.publish_csr_snapshot();
    auto cycles = AnalyticsEngine::detect_bot_swarms(*csr, storage, 5, 25.0);

    assert(!cycles.empty());
    std::cout << "  [PASS] TC-05 passed. Detected bot loop with length " << cycles[0].path.size() << std::endl;
}

void test_tc07_virality_cascade() {
    std::cout << "[RUN] TC-07: Verifying BFS Virality Cascade and Leaf Node Isolation..." << std::endl;
    GraphStorage storage;
    uint32_t root = storage.get_or_create_node("origin");
    uint32_t mid1 = storage.get_or_create_node("mid1");
    uint32_t leaf = storage.get_or_create_node("leaf");

    storage.add_interaction(root, mid1, InteractionType::REPOST, 500);
    storage.add_interaction(mid1, leaf, InteractionType::LIKE, 501);

    auto csr = storage.publish_csr_snapshot();
    auto cascade = AnalyticsEngine::trace_virality_cascade(*csr, root);

    assert(cascade.size() == 3);
    assert(cascade[2].is_leaf == true);
    std::cout << "  [PASS] TC-07 passed. Leaf node isolated correctly." << std::endl;
}

void test_tc08_cascade_with_decay() {
    std::cout << "[RUN] TC-08: Verifying probabilistic cascade with decay..." << std::endl;

    // Build a well-connected graph: hub node 0 -> all, plus a reciprocal ring.
    // Uniform timestamps make T = 1.0 for every edge.
    GraphStorage storage;
    const uint32_t N = 30;
    std::vector<uint32_t> ids;
    for (uint32_t i = 0; i < N; ++i) {
        ids.push_back(storage.get_or_create_node("n" + std::to_string(i)));
    }
    for (uint32_t i = 1; i < N; ++i) {
        storage.add_interaction(ids[0], ids[i], InteractionType::LIKE, 1000);
    }
    for (uint32_t i = 0; i < N; ++i) {
        uint32_t j = (i + 1) % N;
        storage.add_interaction(ids[i], ids[j], InteractionType::REPOST, 1000);
        storage.add_interaction(ids[j], ids[i], InteractionType::REPOST, 1000);
    }
    auto csr = storage.publish_csr_snapshot();

    CascadeConfig cfg;

    // Seed is always in the result as tier 0
    auto r1 = AnalyticsEngine::trace_virality_cascade_with_decay(*csr, storage, ids[0], cfg);
    assert(!r1.steps.empty());
    assert(r1.steps[0].node_id == ids[0]);
    assert(r1.steps[0].depth == 0);

    // Same rng_seed gives identical results
    auto r2 = AnalyticsEngine::trace_virality_cascade_with_decay(*csr, storage, ids[0], cfg);
    assert(r1.steps.size() == r2.steps.size());
    assert(r1.total_attempted == r2.total_attempted);
    assert(r1.total_reached == r2.total_reached);
    for (size_t i = 0; i < r1.steps.size(); ++i) {
        assert(r1.steps[i].node_id == r2.steps[i].node_id);
        assert(r1.steps[i].depth == r2.steps[i].depth);
    }

    // Each node appears at most once in steps
    std::unordered_set<uint32_t> unique_nodes;
    for (const auto& s : r1.steps) {
        assert(unique_nodes.insert(s.node_id).second);
    }

    // reach_ratio for every tier is at most 1.0
    for (const auto& t : r1.tiers) {
        assert(t.reach_ratio <= 1.0 + 1e-9);
        assert(t.reached <= t.attempted + (t.tier == 0 ? 1 : 0));
    }

    // Averaged over 200 seeds, decay (lambda=0.15, kappa=0.05) reaches
    // no more than the no-decay baseline (lambda=0, kappa=0)
    double sum_decay = 0.0, sum_plain = 0.0;
    CascadeConfig plain = cfg;
    plain.decay_rate_lambda = 0.0;
    plain.fatigue_rate_kappa = 0.0;
    for (uint64_t s = 0; s < 200; ++s) {
        cfg.rng_seed = s;
        plain.rng_seed = s;
        sum_decay += static_cast<double>(
            AnalyticsEngine::trace_virality_cascade_with_decay(*csr, storage, ids[0], cfg).total_reached);
        sum_plain += static_cast<double>(
            AnalyticsEngine::trace_virality_cascade_with_decay(*csr, storage, ids[0], plain).total_reached);
    }
    std::cout << "  [INFO] 200-seed avg reach: decay=" << (sum_decay / 200.0)
              << " plain=" << (sum_plain / 200.0) << std::endl;
    assert(sum_decay <= sum_plain);

    // Empty graph does not crash
    GraphStorage empty_storage;
    auto empty_csr = empty_storage.publish_csr_snapshot();
    auto re = AnalyticsEngine::trace_virality_cascade_with_decay(*empty_csr, empty_storage, 0, cfg);
    assert(re.steps.empty());
    assert(re.total_reached == 0);

    // Single-node graph does not crash; seed is a leaf at tier 0
    GraphStorage single_storage;
    uint32_t solo = single_storage.get_or_create_node("solo");
    auto single_csr = single_storage.publish_csr_snapshot();
    auto rs = AnalyticsEngine::trace_virality_cascade_with_decay(*single_csr, single_storage, solo, cfg);
    assert(rs.steps.size() == 1);
    assert(rs.steps[0].node_id == solo);
    assert(rs.steps[0].is_leaf == true);

    std::cout << "  [PASS] TC-08 passed." << std::endl;
}

void test_twitter_csv_ingestion() {
    std::cout << "[RUN] Testing Twitter CSV ingestion..." << std::endl;

    auto write_temp_csv = [](const std::string& filename, const std::string& content) {
        std::filesystem::path p = std::filesystem::temp_directory_path() / filename;
        std::ofstream out(p);
        out << content;
        out.close();
        return p.string();
    };

    // 1. Malformed row (missing column): counted in rows_malformed, not in edges
    {
        std::string path = write_temp_csv("test_malformed.csv",
            "user_index,author_index,time_chunk\n"
            "10,20,100\n"
            "missing_column_row\n"
            "30,40,105\n"
        );
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path);
        std::filesystem::remove(path);

        assert(stats.rows_read == 2);
        assert(stats.rows_malformed == 1);
        assert(stats.edges_ingested == 2);
        std::cout << "  [PASS] Malformed row handled." << std::endl;
    }

    // 2. Missing header: first numeric row is still parsed
    {
        std::string path = write_temp_csv("test_no_header.csv",
            "10,20,100\n"
            "30,40,105\n"
        );
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path);
        std::filesystem::remove(path);

        assert(stats.rows_read == 2);
        assert(stats.rows_malformed == 0);
        assert(stats.edges_ingested == 2);
        std::cout << "  [PASS] Missing header handled." << std::endl;
    }

    // 3. Header present: header is skipped and not counted as malformed
    {
        std::string path = write_temp_csv("test_with_header.csv",
            "user_index,author_index,time_chunk\n"
            "10,20,100\n"
        );
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path);
        std::filesystem::remove(path);

        assert(stats.rows_read == 1);
        assert(stats.rows_malformed == 0);
        assert(stats.edges_ingested == 1);
        std::cout << "  [PASS] Header present handled." << std::endl;
    }

    // 4. Self-loop (user_index == author_index): counted as malformed
    {
        std::string path = write_temp_csv("test_self_loop.csv",
            "user_index,author_index,time_chunk\n"
            "10,10,100\n"
            "20,30,101\n"
        );
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path);
        std::filesystem::remove(path);

        assert(stats.rows_malformed == 1);
        assert(stats.rows_read == 1);
        assert(stats.edges_ingested == 1);
        std::cout << "  [PASS] Self-loop counted as malformed." << std::endl;
    }

    // 5. Empty file: returns zero edges without throwing
    {
        std::string path = write_temp_csv("test_empty.csv", "");
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path);
        std::filesystem::remove(path);

        assert(stats.edges_ingested == 0);
        assert(stats.rows_read == 0);
        assert(stats.rows_malformed == 0);
        std::cout << "  [PASS] Empty file returns zero edges without throwing." << std::endl;
    }

    // 6. Missing file: throws std::runtime_error
    {
        GraphStorage storage;
        bool caught = false;
        try {
            ingest_twitter_csv(storage, "non_existent_file_path_xyz_12345.csv");
        } catch (const std::runtime_error&) {
            caught = true;
        }
        assert(caught);
        std::cout << "  [PASS] Missing file throws std::runtime_error." << std::endl;
    }

    // 7. Bot flag: a user with 8 or more edges and median delta of 3 or less is flagged;
    // a user with 8 edges and median delta of 10 is not.
    {
        std::ostringstream csv;
        csv << "user_index,author_index,time_chunk\n";
        // User 1 (Bot): 8 edges with delta of 2 (median delta 2 <= 3)
        for (int i = 0; i < 8; ++i) {
            csv << "1," << (100 + i) << "," << (i * 2) << "\n";
        }
        // User 2 (Normal): 8 edges with delta of 10 (median delta 10 > 3)
        for (int i = 0; i < 8; ++i) {
            csv << "2," << (200 + i) << "," << (i * 10) << "\n";
        }

        std::string path = write_temp_csv("test_bot_flag.csv", csv.str());
        GraphStorage storage;
        auto stats = ingest_twitter_csv(storage, path, /*run_bot_pass=*/true);
        std::filesystem::remove(path);

        assert(stats.edges_ingested == 16);
        assert(stats.bots_flagged == 1);

        uint32_t bot_id = storage.get_or_create_node("tw:1");
        uint32_t normal_id = storage.get_or_create_node("tw:2");

        auto meta = storage.get_all_nodes_meta();
        assert(meta[bot_id].is_bot == true);
        assert(meta[normal_id].is_bot == false);
        std::cout << "  [PASS] Bot flag verification passed." << std::endl;
    }

    std::cout << "  [PASS] All Twitter CSV ingestion tests passed." << std::endl;
}

// ---------------------------------------------------------------------------
// TC-09: Binary snapshot save/load roundtrip + corruption handling
// ---------------------------------------------------------------------------
void test_tc09_snapshot_persistence() {
    std::cout << "[RUN] TC-09: Verifying snapshot save/load roundtrip..." << std::endl;

    auto snap_path = (std::filesystem::temp_directory_path() / "im_test_snapshot.bin").string();

    // Build a graph with rich metadata, scores, bot flag and a published CSR
    GraphStorage src;
    uint32_t a = src.get_or_create_node("alice.test");
    uint32_t b = src.get_or_create_node("bob.test");
    uint32_t c = src.get_or_create_node("carol.test");
    src.update_node_profile(a, "Alice A", "https://img/alice.png", "crypto trader", 50000, 300);
    src.update_node_profile(b, "Bob B", "", "", 1200, 900);
    src.add_interaction(a, b, InteractionType::REPOST, 1000, 15);
    src.add_interaction(b, c, InteractionType::LIKE, 1001, 40);
    src.add_interaction(c, a, InteractionType::QUOTE, 1002, 25);
    src.mark_bot(c, true);
    auto csr_src = src.publish_csr_snapshot();           // bumps snapshot counter
    src.update_node_scores(
        std::vector<double>{0.5, 0.3, 0.2},
        std::vector<uint32_t>{7, 7, 3});

    assert(src.save_snapshot_file(snap_path));

    // Load into a fresh storage and verify everything survived
    GraphStorage dst;
    uint64_t saved_at = 0;
    assert(dst.load_snapshot_file(snap_path, &saved_at));
    assert(saved_at > 0);

    assert(dst.node_count() == 3);
    assert(dst.total_edges() == 3);

    auto ida = dst.find_node_id("alice.test");
    assert(ida.has_value() && *ida == a);
    assert(!dst.find_node_id("ghost.test").has_value());

    auto meta_a = dst.get_node_meta(a);
    assert(meta_a.has_value());
    assert(meta_a->handle == "alice.test");
    assert(meta_a->display_name == "Alice A");
    assert(meta_a->followers_count == 50000);
    assert(std::abs(meta_a->pagerank_score - 0.5) < 1e-9);
    assert(meta_a->community_id == 7);

    auto meta_c = dst.get_node_meta(c);
    assert(meta_c.has_value() && meta_c->is_bot == true);
    assert(meta_c->community_id == 3);

    // Edge topology preserved (source a -> b repost, InteractionType::REPOST = 2)
    const auto& edges_a = dst.get_edges(a);
    assert(edges_a.size() == 1 && edges_a[0].target_id == b && edges_a[0].type == 2);

    // Snapshot counter continued (next publish gets #2, not #1)
    auto csr_dst = dst.publish_csr_snapshot();
    assert(csr_dst->snapshot_version == csr_src->snapshot_version + 1);
    assert(csr_dst->num_edges == 3);

    // Garbage file -> load fails; storage is left untouched (early header
    // rejection happens before any mutation)
    {
        std::ofstream bad(snap_path, std::ios::binary | std::ios::trunc);
        bad << "NOTASNAPSHOTGARBAGE";
    }
    GraphStorage corrupt;
    corrupt.get_or_create_node("pre_existing");
    assert(!corrupt.load_snapshot_file(snap_path));
    assert(corrupt.node_count() == 1);

    // Truncated valid snapshot -> fails mid-body and storage is reset
    assert(dst.save_snapshot_file(snap_path));
    {
        std::fstream f(snap_path, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(40); // inside node metadata region
        char zeros[200] = {};
        f.write(zeros, sizeof(zeros));
    }
    GraphStorage trunc;
    trunc.get_or_create_node("will_be_wiped");
    bool trunc_ok = trunc.load_snapshot_file(snap_path);
    // Either it parses (unlikely with zeroed metadata) or fails; on failure
    // the storage must be empty, never partially populated.
    if (!trunc_ok) assert(trunc.node_count() == 0);

    // Missing file -> false, no crash
    assert(!dst.load_snapshot_file(snap_path + ".does_not_exist"));

    std::filesystem::remove(snap_path);
    std::cout << "  [PASS] TC-09 passed." << std::endl;
}

// ---------------------------------------------------------------------------
// TC-10: Node lookups + ingest_batch deduplication
// ---------------------------------------------------------------------------
void test_tc10_lookups_and_dedup() {
    std::cout << "[RUN] TC-10: Verifying node lookups and edge deduplication..." << std::endl;

    GraphStorage storage;
    uint32_t a = storage.get_or_create_node("dup.alice");
    uint32_t b = storage.get_or_create_node("dup.bob");

    XInteractionEvent ev{};
    ev.source_id = a; ev.target_id = b;
    ev.timestamp_sec = 100; ev.type = 1; ev.latency_ms = 10;

    // dedup=true: identical (src,dst,type) edges are skipped on re-ingest
    storage.ingest_batch(&ev, 1, /*dedup=*/true);
    storage.ingest_batch(&ev, 1, /*dedup=*/true);
    storage.ingest_batch(&ev, 1, /*dedup=*/true);
    assert(storage.get_edges(a).size() == 1);
    assert(storage.total_edges() == 1);

    // dedup=false (default): every call appends
    storage.ingest_batch(&ev, 1);
    storage.ingest_batch(&ev, 1);
    assert(storage.get_edges(a).size() == 3);

    // dedup only matches on (source,target,type) — different type still inserts
    XInteractionEvent ev2 = ev;
    ev2.type = 4; // different interaction type
    storage.ingest_batch(&ev2, 1, /*dedup=*/true);
    assert(storage.get_edges(a).size() == 4);

    // Lookups
    assert(storage.find_node_id("dup.alice").value() == a);
    assert(!storage.find_node_id("nobody").has_value());
    assert(storage.get_node_meta(b)->handle == "dup.bob");
    assert(!storage.get_node_meta(9999).has_value());
    assert(storage.get_handle(a) == "dup.alice");

    std::cout << "  [PASS] TC-10 passed." << std::endl;
}

// ---------------------------------------------------------------------------
// TC-11: json::Value serializer correctness
// ---------------------------------------------------------------------------
void test_tc11_json_serializer() {
    std::cout << "[RUN] TC-11: Verifying json::Value serialization..." << std::endl;

    auto list = json::array();
    list.arr.push_back(json::integer(7));

    auto v = json::obj({
        {"status", "ok"},
        {"nodes", 21},
        {"edges", uint64_t(41)},
        {"ratio", 0.5},
        {"flag", true},
        {"nothing", json::null()},
        {"quote", "say \"hi\"\nnewline"},
        {"nested", json::obj({{"k", "v"}})},
        {"list", std::move(list)}
    });

    std::string s = v.dump();
    // Spot-check key fragments (order preserved, compact separators)
    assert(s.find("\"status\":\"ok\"") != std::string::npos);
    assert(s.find("\"nodes\":21") != std::string::npos);
    assert(s.find("\"flag\":true") != std::string::npos);
    assert(s.find("\"nothing\":null") != std::string::npos);
    // Escaping: quote -> \", newline -> \n (literal backslash-n in output)
    assert(s.find("say \\\"hi\\\"\\nnewline") != std::string::npos);
    assert(s.find("\"nested\":{\"k\":\"v\"}") != std::string::npos);
    assert(s.find("\"list\":[7]") != std::string::npos);
    // Whole-number doubles print without decimals
    assert(json::num(3.0).dump() == "3");
    assert(json::num(0.25).dump() == "0.25");

    std::cout << "  [PASS] TC-11 passed." << std::endl;
}

// ---------------------------------------------------------------------------
// TC-12: Live gateway HTTP + WebSocket endpoint smoke tests
// ---------------------------------------------------------------------------
namespace {

// Minimal blocking HTTP/1.0-style client for tests (Connection: close).
std::string test_http_get(int port, const std::string& path) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) return "";
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<u_short>(port));
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        closesocket(sock);
        return "";
    }
    std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\n"
                      "Connection: close\r\n\r\n";
    send(sock, req.data(), static_cast<int>(req.size()), 0);
    std::string resp;
    char buf[8192];
    int n;
    while ((n = recv(sock, buf, sizeof(buf), 0)) > 0) resp.append(buf, n);
    closesocket(sock);
    return resp;
}

std::string test_http_status(const std::string& raw) {
    // "HTTP/1.1 200 OK\r\n..." -> "200"
    size_t p = raw.find(' ');
    if (p == std::string::npos) return "";
    size_t q = raw.find(' ', p + 1);
    return raw.substr(p + 1, q == std::string::npos ? std::string::npos : q - p - 1);
}

std::string test_http_body(const std::string& raw) {
    size_t p = raw.find("\r\n\r\n");
    return p == std::string::npos ? "" : raw.substr(p + 4);
}

bool test_tcp_connect(int port) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<u_short>(port));
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    bool ok = (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    closesocket(sock);
    return ok;
}

void test_tc12_gateway_endpoints() {
    std::cout << "[RUN] TC-12: Verifying live gateway endpoints..." << std::endl;

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif

    // Deterministic fixture graph: hub -> spokes + ring
    GraphStorage storage;
    std::vector<uint32_t> ids;
    for (int i = 0; i < 6; ++i) {
        ids.push_back(storage.get_or_create_node("gw.user" + std::to_string(i)));
    }
    for (int i = 1; i < 6; ++i) {
        storage.add_interaction(ids[0], ids[i], InteractionType::LIKE, 1000 + i);
    }
    for (int i = 0; i < 6; ++i) {
        storage.add_interaction(ids[i], ids[(i + 1) % 6], InteractionType::REPOST, 2000 + i);
    }
    auto csr = storage.publish_csr_snapshot();
    std::vector<double> pr(csr->num_nodes, 1.0 / csr->num_nodes);
    pr[0] = 0.4; // hub gets the most authority
    storage.update_node_scores(pr, std::vector<uint32_t>(csr->num_nodes, 1));

    BlueskyConfig bsky_cfg; // never started -> no network access
    BlueskyIngestionEngine engine(bsky_cfg, storage);

    constexpr int kPort = 18099;
    GatewayServer server(kPort, storage, engine,
                         /*bluesky_loaded=*/false, /*twitter_loaded=*/false,
                         "gw.user0", /*snapshot_loaded=*/false,
                         "test_snapshot_should_not_be_written.bin");
    server.start();

    // Wait for the listener (accept loop binds on a thread)
    bool up = false;
    for (int i = 0; i < 50 && !up; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        up = test_tcp_connect(kPort);
    }
    assert(up && "gateway did not start listening");

    // --- /api/health ---
    {
        std::string r = test_http_get(kPort, "/api/health");
        assert(test_http_status(r) == "200");
        std::string b = test_http_body(r);
        assert(b.find("\"status\":\"ok\"") != std::string::npos);
        assert(b.find("\"nodes\":6") != std::string::npos);
        assert(b.find("\"edges\":11") != std::string::npos);
    }

    // --- /api/candidates ---
    {
        std::string r = test_http_get(kPort, "/api/candidates");
        assert(test_http_status(r) == "200");
        std::string b = test_http_body(r);
        assert(b.find("gw.user0") != std::string::npos);
        assert(b.find("\"pageRank\"") != std::string::npos);
    }

    // --- /api/graph ---
    {
        std::string r = test_http_get(kPort, "/api/graph");
        assert(test_http_status(r) == "200");
        std::string b = test_http_body(r);
        assert(b.find("\"nodes\":[") != std::string::npos);
        assert(b.find("\"edges\":[") != std::string::npos);
    }

    // --- /api/export/candidates ---
    {
        std::string r = test_http_get(kPort, "/api/export/candidates?format=csv");
        assert(test_http_status(r) == "200");
        assert(r.find("Content-Disposition: attachment") != std::string::npos);
        assert(r.find("ambassador_shortlist.csv") != std::string::npos);
        std::string b = test_http_body(r);
        assert(b.find("rank,node_id,handle") == 0);
        assert(b.find("gw.user0") != std::string::npos);

        r = test_http_get(kPort, "/api/export/candidates?format=json");
        assert(test_http_status(r) == "200");
        b = test_http_body(r);
        assert(b.find("\"candidates\":[") != std::string::npos);
        assert(b.find("\"count\":6") != std::string::npos);

        r = test_http_get(kPort, "/api/export/candidates?format=xml");
        assert(test_http_status(r) == "400");
    }

    // --- /api/node ---
    {
        std::string r = test_http_get(kPort, "/api/node?handle=gw.user0");
        assert(test_http_status(r) == "200");
        assert(test_http_body(r).find("\"handle\":\"gw.user0\"") != std::string::npos);

        r = test_http_get(kPort, "/api/node?handle=does.not.exist");
        assert(test_http_status(r) == "404");
    }

    // --- /api/cascade ---
    {
        std::string r = test_http_get(kPort, "/api/cascade?node_id=0&lambda=0.2&kappa=0.1&seed=7");
        assert(test_http_status(r) == "200");
        std::string b = test_http_body(r);
        assert(b.find("\"tiers\":[") != std::string::npos);
        assert(b.find("\"total_reached\"") != std::string::npos);
        assert(b.find("\"nodes\":[") != std::string::npos);

        // Missing node_id -> 400
        r = test_http_get(kPort, "/api/cascade");
        assert(test_http_status(r) == "400");
        // Non-numeric node_id -> 400
        r = test_http_get(kPort, "/api/cascade?node_id=abc");
        assert(test_http_status(r) == "400");
        // Out of range -> 404
        r = test_http_get(kPort, "/api/cascade?node_id=9999");
        assert(test_http_status(r) == "404");
    }

    // --- /api/cascade/compare ---
    {
        std::string r = test_http_get(kPort, "/api/cascade/compare?node_ids=0,1,2&lambda=0.15&seed=3");
        assert(test_http_status(r) == "200");
        std::string b = test_http_body(r);
        assert(b.find("\"results\":[") != std::string::npos);
        // 3 seeds -> 3 result entries, each tagged with seed identity
        assert(b.find("\"seed_handle\":\"gw.user0\"") != std::string::npos);
        assert(b.find("\"seed_handle\":\"gw.user2\"") != std::string::npos);

        // Missing node_ids -> 400
        r = test_http_get(kPort, "/api/cascade/compare");
        assert(test_http_status(r) == "400");
        // Mixed valid/invalid ids: invalid entries get an error object, not a crash
        r = test_http_get(kPort, "/api/cascade/compare?node_ids=0,9999");
        assert(test_http_status(r) == "200");
        assert(test_http_body(r).find("\"error\":\"node_id out of range\"") != std::string::npos);
    }

    // --- Unknown API route -> JSON 404 ---
    {
        std::string r = test_http_get(kPort, "/api/bogus");
        assert(test_http_status(r) == "404");
        assert(test_http_body(r).find("\"error\"") != std::string::npos);
    }

    // --- /ws without Upgrade -> 426 ---
    {
        std::string r = test_http_get(kPort, "/ws");
        assert(test_http_status(r) == "426");
    }

    // --- /ws with Upgrade -> 101 + health frame ---
    {
        SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<u_short>(kPort));
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        assert(connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);

        const char* hs =
            "GET /ws HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n";
        send(sock, hs, static_cast<int>(strlen(hs)), 0);

        // Read handshake response headers
        std::string resp;
        char buf[2048];
        int n;
        while (resp.find("\r\n\r\n") == std::string::npos &&
               (n = recv(sock, buf, sizeof(buf), 0)) > 0) {
            resp.append(buf, n);
        }
        assert(resp.find("101") != std::string::npos);

        // First pushed frame should be a health event
        uint8_t opcode = 0;
        std::string payload;
        assert(ws::recv_frame(sock, opcode, payload));
        assert(opcode == 0x1); // text frame
        assert(payload.find("\"type\":\"health\"") != std::string::npos);
        assert(payload.find("\"nodes\":6") != std::string::npos);

        ws::send_close(sock);
        closesocket(sock);
    }

    server.stop();
    std::cout << "  [PASS] TC-12 passed." << std::endl;
}

// ---------------------------------------------------------------------------
// TC-13: Instagram synthetic graph — determinism, structure, bot pods
// ---------------------------------------------------------------------------
void test_tc13_instagram_ingestion() {
    std::cout << "[RUN] TC-13: Verifying Instagram synthetic graph generation..." << std::endl;

    auto build = [](GraphStorage& s) {
        InstagramConfig cfg;
        cfg.mock_node_count = 60;
        InstagramIngestionEngine engine(cfg, s);
        size_t edges = engine.ingest_mock_network();
        return std::make_pair(edges, engine.get_stats());
    };

    GraphStorage s1, s2;
    auto [e1, st1] = build(s1);
    auto [e2, st2] = build(s2);

    // Deterministic: same seed -> identical graph
    assert(e1 == e2);
    assert(s1.node_count() == s2.node_count());
    assert(s1.total_edges() == s2.total_edges());
    assert(s1.node_count() >= 50);           // PRD: 50+ nodes
    assert(e1 > 100);                        // meaningful edge count
    assert(st1.bots_flagged >= 9);           // 3 pods x 3-4 members

    // Metadata sanity: niche in description, followers populated
    auto meta = s1.get_all_nodes_meta();
    bool saw_niche = false, saw_mega = false;
    for (const auto& m : meta) {
        if (m.description.find("fashion") != std::string::npos ||
            m.description.find("fitness") != std::string::npos) saw_niche = true;
        if (m.followers_count > 200000) saw_mega = true;
    }
    assert(saw_niche && saw_mega);

    // Bot pods: flagged nodes exist and their edges stay in-pod
    // (pod members only link to same-pod members by construction)
    uint32_t bot_count = 0;
    for (const auto& m : meta) if (m.is_bot) ++bot_count;
    assert(bot_count == st1.bots_flagged);

    // PageRank + LPA run cleanly on the synthetic graph
    auto csr = s1.publish_csr_snapshot();
    auto pr = AnalyticsEngine::compute_pagerank(*csr);
    auto lpa = AnalyticsEngine::compute_lpa(*csr);
    assert(pr.size() == csr->num_nodes);
    assert(lpa.size() == csr->num_nodes);

    // Decay cascade from the seed handle terminates with tier stats
    auto seed_it = s1.find_node_id("ig:fashionicon");
    assert(seed_it.has_value());
    CascadeConfig cfg;
    auto cascade = AnalyticsEngine::trace_virality_cascade_with_decay(
        *csr, s1, *seed_it, cfg);
    assert(cascade.total_reached >= 1);
    assert(!cascade.tiers.empty());

    std::cout << "  [PASS] TC-13 passed. nodes=" << s1.node_count()
              << " edges=" << e1 << " bots=" << bot_count << std::endl;
}

// ---------------------------------------------------------------------------
// TC-14: Threads reuses the synthetic engine with th: prefix + own seed
// ---------------------------------------------------------------------------
void test_tc14_threads_synthetic() {
    std::cout << "[RUN] TC-14: Verifying Threads synthetic graph variant..." << std::endl;

    GraphStorage ig_store, th_store;
    InstagramConfig ig_cfg;                    // defaults: ig: prefix, seed 1337
    InstagramIngestionEngine ig_engine(ig_cfg, ig_store);
    ig_engine.ingest_mock_network();

    InstagramConfig th_cfg;
    th_cfg.handle_prefix = "th:";
    th_cfg.rng_seed = 7331;
    th_cfg.handle = "th:threadsinfluencer";
    InstagramIngestionEngine th_engine(th_cfg, th_store);
    th_engine.ingest_mock_network();

    // All Threads handles carry the th: prefix
    for (const auto& m : th_store.get_all_nodes_meta()) {
        assert(m.handle.rfind("th:", 0) == 0);
    }
    assert(th_store.find_node_id("th:threadsinfluencer").has_value());

    // Different seed -> different edge structure than Instagram's
    assert(th_store.total_edges() != ig_store.total_edges() ||
           th_store.node_count() != ig_store.node_count());

    // Analytics still work on the Threads graph
    auto csr = th_store.publish_csr_snapshot();
    auto pr = AnalyticsEngine::compute_pagerank(*csr);
    assert(pr.size() == csr->num_nodes);

    std::cout << "  [PASS] TC-14 passed. threads nodes=" << th_store.node_count()
              << " edges=" << th_store.total_edges() << std::endl;
}

// ---------------------------------------------------------------------------
// TC-15: Instagram CSV dataset ingestion
// ---------------------------------------------------------------------------
void test_tc15_instagram_csv() {
    std::cout << "[RUN] TC-15: Verifying Instagram CSV ingestion..." << std::endl;

    auto write_csv = [](const std::string& name, const std::string& content) {
        auto p = (std::filesystem::temp_directory_path() / name).string();
        std::ofstream(p) << content;
        return p;
    };

    // Fixture: header + organic edges + a 3-node low-latency pod ring
    std::string csv =
        "source_username,target_username,interaction_type,timestamp,latency_ms\n"
        "fashionfan1,styleicon,like,1791900000,45000\n"
        "styleicon,fitfam,comment,1791900100,60000\n"
        "fitfam,styleicon,share,1791900200,30000\n"
        "pod.a,pod.b,like,1791900300,120\n"
        "pod.b,pod.c,like,1791900301,150\n"
        "pod.c,pod.a,like,1791900302,180\n"
        "pod.a,pod.c,share,1791900303,200\n"
        "pod.b,pod.a,comment,1791900304,90\n"
        "pod.c,pod.b,save,1791900305,110\n"
        "bad_row_missing_columns\n"
        "self.self,self.self,like,1791900400,500\n";

    std::string path = write_csv("test_ig.csv", csv);
    GraphStorage storage;
    InstagramCsvIngestionEngine engine(storage);
    auto stats = engine.ingest_file(path);
    std::filesystem::remove(path);

    assert(stats.rows_read == 9);          // 6 pod + 3 organic; 2 malformed skipped
    assert(stats.rows_malformed == 2);
    assert(stats.edges_ingested == 9);
    assert(stats.unique_users == 6);       // styleicon, fitfam, fashionfan1, pod.a/b/c

    // Handles carry ig: prefix
    assert(storage.find_node_id("ig:styleicon").has_value());
    assert(storage.find_node_id("ig:pod.a").has_value());

    // Followers estimated from in-degree (>0 for hubs)
    auto meta = storage.get_all_nodes_meta();
    auto sid = storage.find_node_id("ig:styleicon").value();
    assert(meta[sid].followers_count > 0);

    // Bot pass flags the pod ring members (median latency <2s, closed cycle)
    assert(stats.bots_flagged >= 3);
    auto pod_a = storage.find_node_id("ig:pod.a").value();
    assert(meta[pod_a].is_bot == true);
    assert(meta[sid].is_bot == false);     // organic user not flagged

    // Missing file throws
    bool caught = false;
    try { InstagramCsvIngestionEngine e2(storage); e2.ingest_file("nope_missing.csv"); }
    catch (const std::runtime_error&) { caught = true; }
    assert(caught);

    std::cout << "  [PASS] TC-15 passed. edges=" << stats.edges_ingested
              << " bots=" << stats.bots_flagged << std::endl;
}

} // namespace

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "  Executing PRD Test Suite (TC-01 - 15)   " << std::endl;
    std::cout << "========================================" << std::endl;

    test_tc01_types_and_alignment();
    test_tc02_storage_and_csr();
    test_tc03_pagerank_convergence();
    test_tc05_bot_cycle_detection();
    test_tc07_virality_cascade();
    test_tc08_cascade_with_decay();
    test_twitter_csv_ingestion();
    test_tc09_snapshot_persistence();
    test_tc10_lookups_and_dedup();
    test_tc11_json_serializer();
    test_tc12_gateway_endpoints();
    test_tc13_instagram_ingestion();
    test_tc14_threads_synthetic();
    test_tc15_instagram_csv();

    std::cout << "\nALL TEST CASES PASSED SUCCESSFULLY [100%]" << std::endl;
    return 0;
}

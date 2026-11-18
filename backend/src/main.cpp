// InfluenceMapper — Main Entrypoint
//
// Pipeline: load .env config -> prepare graph (snapshot or live ingest)
//           -> PageRank + LPA analytics -> launch HTTP/WebSocket gateway.

#include "types.hpp"
#include "env_loader.hpp"
#include "bluesky_ingestion.hpp"
#include "twitter_csv_ingestion.hpp"
#include "instagram_ingestion.hpp"
#include "instagram_csv_ingestion.hpp"
#include "graph_storage.hpp"
#include "analytics.hpp"
#include "gateway_server.hpp"

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iomanip>
#include <thread>
#include <chrono>
#include <ctime>
#include <csignal>
#include <atomic>

namespace {

// Snapshot file stores the full graph (nodes + edges + metadata) so restarts
// skip bulk ingestion entirely.
constexpr const char* kSnapshotPath = "graph_snapshot.bin";
constexpr int kGatewayPort = 18080;

std::atomic<bool> g_shutdown{false};
void handle_signal(int) { g_shutdown = true; }

// Which data sources to load at startup (CLI flags override defaults).
struct SourceFlags {
    bool load_bsky    = true;
    bool load_twitter = true;
    bool load_instagram = true;
    bool load_threads  = true;
    bool force_rebuild = false;   // --rebuild / --fresh: ignore snapshot
    bool list_sources = false;    // --help
};

void print_usage() {
    std::cout << "InfluenceMapper Backend Server\n"
              << "Usage: influence_mapper [options]\n"
              << "  (no flags)     load snapshot if present, else ingest all sources\n"
              << "  --rebuild      force full re-ingestion, ignore snapshot\n"
              << "  --fresh        alias for --rebuild\n"
              << "  --no-twitter   skip Twitter/X CSV ingestion\n"
              << "  --no-bluesky   skip Bluesky ingestion\n"
              << "  --no-instagram skip Instagram synthetic graph\n"
              << "  --no-threads   skip Threads synthetic graph\n"
              << "  --help         show this message\n";
}

SourceFlags parse_args(int argc, char* argv[]) {
    SourceFlags f;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--help")              f.list_sources = true;
        else if (a == "--rebuild" || a == "--fresh") f.force_rebuild = true;
        else if (a == "--no-twitter")        f.load_twitter = false;
        else if (a == "--no-bluesky")        f.load_bsky = false;
        else if (a == "--no-instagram")      f.load_instagram = false;
        else if (a == "--no-threads")        f.load_threads = false;
    }
    return f;
}

} // namespace

int main(int argc, char* argv[]) {
    SourceFlags flags = parse_args(argc, argv);
    if (flags.list_sources) {
        print_usage();
        return 0;
    }

    std::cout << "==================================================\n"
              << "       InfluenceMapper - Graph Analytics Engine    \n"
              << "==================================================\n\n";

    // --- Load configuration (.env -> ApiConfig) ---
    // The repo keeps .env at the root; the exe may run from backend/ or root.
    influmapper::ApiConfig cfg_env;
    for (const char* p : {"backend/.env", ".env", "../.env"}) {
        std::ifstream probe(p);
        if (probe.good()) {
            cfg_env = influmapper::ApiConfig::load_from_env_file(p);
            std::cout << "[Config] Loaded " << p << "\n";
            break;
        }
    }

    // --- Graph storage (shared by all ingestion sources) ---
    influmapper::GraphStorage storage;

    // --- Bluesky engine (also used by the /api/refresh endpoint) ---
    influmapper::BlueskyConfig bsky_cfg;
    if (!cfg_env.bluesky_handle.empty())          bsky_cfg.handle = cfg_env.bluesky_handle;
    if (!cfg_env.bluesky_app_password.empty())    bsky_cfg.app_password = cfg_env.bluesky_app_password;
    if (!cfg_env.bluesky_pds_host.empty())        bsky_cfg.pds_host = cfg_env.bluesky_pds_host;
    if (!cfg_env.bluesky_jetstream_url.empty())   bsky_cfg.jetstream_url = cfg_env.bluesky_jetstream_url;

    influmapper::BlueskyIngestionEngine bsky_engine(bsky_cfg, storage);
    bsky_engine.set_on_batch_ingested([](size_t n) {
        std::cout << "[Bluesky] Batch ingested: " << n << " events." << std::endl;
    });

    // --- Prepare graph data: snapshot first, ingest if missing ---
    bool snapshot_loaded = false;
    bool bluesky_loaded = false;
    bool twitter_loaded = false;
    bool instagram_loaded = false;
    bool threads_loaded = false;

    if (!flags.force_rebuild) {
        uint64_t saved_at = 0;
        snapshot_loaded = storage.load_snapshot_file(kSnapshotPath, &saved_at);
        if (snapshot_loaded) {
            auto age_h = (static_cast<uint64_t>(std::time(nullptr)) - saved_at) / 3600;
            std::cout << "[Persistence] Loaded snapshot (" << storage.node_count()
                      << " nodes, " << storage.total_edges() << " edges, saved "
                      << age_h << "h ago).\n";
        }
    }

    if (!snapshot_loaded) {
        // --- Twitter/X CSV ingestion ---
        if (flags.load_twitter) {
            const std::string& csv_path = cfg_env.twitter_csv_path;
            if (csv_path.empty()) {
                std::cout << "[Ingestion] TWITTER_CSV_PATH not set - skipping Twitter ingestion.\n";
            } else {
                std::cout << "[Ingestion] Ingesting local Twitter/X CSV dataset: "
                          << csv_path << std::endl;
                try {
                    auto tw_stats = influmapper::ingest_twitter_csv(
                        storage, csv_path, /*run_bot_pass=*/true);
                    std::cout << "[Ingestion] Twitter CSV: " << tw_stats.edges_ingested
                              << " edges, " << tw_stats.unique_users << " unique nodes, "
                              << tw_stats.bots_flagged << " bots flagged."
                              << " (parse: " << tw_stats.parse_time_ms << " ms)"
                              << std::endl;
                    twitter_loaded = true;
                } catch (const std::exception& ex) {
                    std::cerr << "[Warning] Twitter CSV ingestion skipped: " << ex.what() << std::endl;
                }
            }
        } else {
            std::cout << "[Config] Twitter/X ingestion disabled via flag.\n";
        }

        // --- Bluesky live ingestion ---
        if (flags.load_bsky) {
            std::cout << "[Ingestion] Pulling live Bluesky graph from @"
                      << bsky_cfg.handle << "..." << std::endl;
            size_t bsky_edges = bsky_engine.ingest_real_network(bsky_cfg.handle);
            std::cout << "[Ingestion] Bluesky: " << bsky_edges << " interaction events." << std::endl;
            bluesky_loaded = true;
        } else {
            std::cout << "[Config] Bluesky ingestion disabled via flag.\n";
        }

        // --- Instagram graph: real CSV dataset if configured, else synthetic ---
        if (flags.load_instagram) {
            if (!cfg_env.instagram_csv_path.empty()) {
                std::cout << "[Ingestion] Loading Instagram CSV dataset: "
                          << cfg_env.instagram_csv_path << std::endl;
                try {
                    influmapper::InstagramCsvIngestionEngine ig_csv_engine(storage);
                    auto ig_stats = ig_csv_engine.ingest_file(cfg_env.instagram_csv_path);
                    std::cout << "[Ingestion] Instagram CSV: " << ig_stats.edges_ingested
                              << " edges, " << ig_stats.unique_users << " users, "
                              << ig_stats.bots_flagged << " pods flagged.\n";
                    instagram_loaded = true;
                } catch (const std::exception& ex) {
                    std::cerr << "[Warning] Instagram CSV ingestion skipped: "
                              << ex.what() << std::endl;
                }
            }
            if (!instagram_loaded) {
                influmapper::InstagramConfig insta_cfg;
                if (!cfg_env.instagram_handle.empty()) insta_cfg.handle = cfg_env.instagram_handle;
                insta_cfg.mock_node_count = cfg_env.instagram_mock_nodes;
                influmapper::InstagramIngestionEngine insta_engine(insta_cfg, storage);
                size_t ig_edges = insta_engine.ingest_mock_network();
                auto ig_stats = insta_engine.get_stats();
                std::cout << "[Ingestion] Instagram (synthetic): "
                          << ig_stats.unique_users << " creators, " << ig_edges
                          << " interactions, " << ig_stats.bots_flagged
                          << " pod accounts flagged.\n";
                instagram_loaded = true;
            }
        } else {
            std::cout << "[Config] Instagram ingestion disabled via flag.\n";
        }

        // --- Threads synthetic creator graph (real Meta API pending approval) ---
        if (flags.load_threads) {
            influmapper::InstagramConfig th_cfg;
            th_cfg.handle_prefix = "th:";
            th_cfg.rng_seed = 7331; // different topology from Instagram's 1337
            th_cfg.handle = "th:threadsinfluencer";
            if (!cfg_env.threads_handle.empty()) th_cfg.handle = cfg_env.threads_handle;
            th_cfg.mock_node_count = cfg_env.threads_mock_nodes;
            influmapper::InstagramIngestionEngine th_engine(th_cfg, storage);
            size_t th_edges = th_engine.ingest_mock_network();
            auto th_stats = th_engine.get_stats();
            std::cout << "[Ingestion] Threads: " << th_stats.unique_users
                      << " creators, " << th_edges << " interactions, "
                      << th_stats.bots_flagged << " pod accounts flagged.\n";
            threads_loaded = true;
        } else {
            std::cout << "[Config] Threads ingestion disabled via flag.\n";
        }
    } else {
        std::cout << "[Config] Snapshot loaded - skipping bulk ingestion. "
                  << "Use --rebuild to force re-ingestion.\n";
    }

    // --- Summary block ---
    std::cout << "\n=================================================================\n"
              << "  Data Sources Ingestion Summary                                \n"
              << "=================================================================\n"
              << "  Bluesky AT Protocol : " << (bluesky_loaded ? "[LOADED]" : "[NOT LOADED]") << "\n"
              << "  Twitter/X CSV Graph : " << (twitter_loaded ? "[LOADED]" : "[NOT LOADED]") << "\n"
              << "  Instagram (Synthetic): " << (instagram_loaded ? "[LOADED]" : "[NOT LOADED]") << "\n"
              << "  Threads (Synthetic)  : " << (threads_loaded ? "[LOADED]" : "[NOT LOADED]") << "\n"
              << "  Disk Snapshot        : " << (snapshot_loaded ? "[LOADED]" : "[NOT USED]") << "\n"
              << "=================================================================\n\n";

    // --- Publish merged CSR snapshot (Bluesky + Twitter) ---
    std::cout << "[Storage] Publishing atomic read-only CSR projection...\n";
    auto csr = storage.publish_csr_snapshot();
    std::cout << "[Storage] CSR Snapshot #" << csr->snapshot_version
              << " generated with " << csr->num_nodes << " nodes and "
              << csr->num_edges << " directed edges.\n";

    // --- Analytics ---
    std::cout << "[Analytics] Running parallel PageRank (d=0.85, 25 iterations)...\n";
    auto pr_start = std::chrono::high_resolution_clock::now();
    auto pr_scores = influmapper::AnalyticsEngine::compute_pagerank(*csr);
    auto pr_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Analytics] PageRank computed in "
              << std::chrono::duration<double, std::milli>(pr_end - pr_start).count()
              << " ms.\n";

    std::cout << "[Analytics] Running Label Propagation Algorithm (LPA)...\n";
    auto communities = influmapper::AnalyticsEngine::compute_lpa(*csr);
    storage.update_node_scores(pr_scores, communities);

    // --- Persist snapshot ---
    if (storage.save_snapshot_file(kSnapshotPath)) {
        std::cout << "[Persistence] Graph snapshot saved to '" << kSnapshotPath << "'\n";
    } else {
        std::cerr << "[Persistence] Warning: failed to save snapshot to '"
                  << kSnapshotPath << "'\n";
    }

    // --- Top influencers ---
    auto all_meta = storage.get_all_nodes_meta();
    std::vector<influmapper::NodeMetadata> ranked = all_meta;
    std::sort(ranked.begin(), ranked.end(), [](const influmapper::NodeMetadata& a,
                                                const influmapper::NodeMetadata& b) {
        return a.pagerank_score > b.pagerank_score;
    });

    std::cout << "\n[Results] Top Ranked Real Accounts by PageRank:\n";
    for (size_t i = 0; i < std::min(ranked.size(), size_t(8)); ++i) {
        std::cout << "  #" << (i + 1) << " @" << ranked[i].handle
                  << " (" << (ranked[i].display_name.empty() ? ranked[i].handle
                                                        : ranked[i].display_name) << ")"
                  << " | PR: " << std::fixed << std::setprecision(4) << ranked[i].pagerank_score
                  << " | Cluster: " << ranked[i].community_id
                  << " | In-Degree: " << ranked[i].in_degree << "\n";
    }

    // --- Demo cascade simulation (probabilistic decay model) ---
    std::cout << "\n[Simulation] Tracing viral cascade diffusion from seed node 0...\n";
    influmapper::CascadeConfig demo_cfg;
    demo_cfg.rng_seed = 42;
    auto cascade = influmapper::AnalyticsEngine::trace_virality_cascade_with_decay(
        *csr, storage, 0, demo_cfg);
    std::cout << "[Simulation] Cascade reached " << cascade.total_reached
              << " real users out of " << cascade.total_attempted
              << " attempted transmissions.\n";

    // --- Launch C++ Gateway Server ---
    influmapper::GatewayServer server(
        kGatewayPort, storage, bsky_engine,
        bluesky_loaded, twitter_loaded, bsky_cfg.handle, snapshot_loaded,
        kSnapshotPath, instagram_loaded, threads_loaded);
    server.start();

    // Background live stream pulse (Jetstream listener, if configured)
    bsky_engine.start();

    std::cout << "\n=================================================================\n"
              << " [SERVER ONLINE] Open your browser at: http://localhost:" << kGatewayPort << "    \n"
              << " Press Enter to stop server...                                   \n"
              << "=================================================================\n";

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::string line;
    std::thread input_thread([&]() {
        std::getline(std::cin, line);
        g_shutdown = true;
    });
    input_thread.detach();

    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "\n[System] Shutting down server and ingestion...\n";
    bsky_engine.stop();
    storage.save_snapshot_file(kSnapshotPath);
    server.stop();
    std::cout << "[System] Engine shutdown cleanly.\n";
    return 0;
}

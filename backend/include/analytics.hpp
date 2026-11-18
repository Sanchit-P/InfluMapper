#pragma once

#include "types.hpp"
#include "graph_storage.hpp"

#include <vector>
#include <queue>
#include <random>
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cmath>

#if defined(_OPENMP)
#include <omp.h>
#endif

namespace influmapper {

// Configuration for the probabilistic cascade model with decay (FR-3.5)
struct CascadeConfig {
    double alpha = 0.5;                 // Interaction frequency weight in S(u,v)
    double beta = 0.3;                  // Reciprocity weight in S(u,v)
    double gamma = 0.2;                 // Recency weight in S(u,v)
    double decay_rate_lambda = 0.15;    // Distance decay rate: D(d) = exp(-lambda * d)
    double fatigue_rate_kappa = 0.05;   // Fatigue rate: M(t) = 1 / (1 + kappa * t)
    uint32_t max_tiers = 6;             // Maximum cascade depth (seed = tier 0)
    uint64_t rng_seed = 42;             // Deterministic RNG seed
};

// Per-tier transmission statistics for the decay cascade
struct CascadeTierStats {
    uint32_t tier = 0;
    size_t attempted = 0;   // Transmission attempts targeting this tier
    size_t reached = 0;     // Nodes infected at this tier
    double reach_ratio = 0.0; // reached / attempted (1.0 for tier 0 seed)
};

struct CascadeResult {
    std::vector<CascadeStep> steps;      // Reached nodes (depth field = tier), with is_leaf
    std::vector<CascadeTierStats> tiers; // One entry per tier 0..max_tiers
    size_t total_attempted = 0;
    size_t total_reached = 0;
    double actual_reach_ratio = 0.0;     // total_reached / (total_attempted + 1 seed)
};

// FR-3: Analytics & Security Engines
class AnalyticsEngine {
public:
    // FR-3.1: Network Authority - Parallel OpenMP PageRank
    // Calculates double-precision PageRank scores on CSR snapshot
    static std::vector<double> compute_pagerank(
        const CSRGraph& graph, 
        double damping = 0.85, 
        uint32_t max_iterations = 25, 
        double tolerance = 1e-6
    ) {
        uint32_t n = graph.num_nodes;
        if (n == 0) return {};

        std::vector<double> rank(n, 1.0 / n);
        std::vector<double> next_rank(n, 0.0);

        // Precompute out-degrees and inverse degrees
        std::vector<double> inv_out_degree(n, 0.0);
        for (uint32_t i = 0; i < n; ++i) {
            uint32_t deg = graph.degree(i);
            inv_out_degree[i] = (deg > 0) ? (1.0 / deg) : 0.0;
        }

        for (uint32_t iter = 0; iter < max_iterations; ++iter) {
            double dangling_sum = 0.0;

            #pragma omp parallel for reduction(+:dangling_sum) schedule(static)
            for (int64_t i = 0; i < static_cast<int64_t>(n); ++i) {
                if (graph.degree(static_cast<uint32_t>(i)) == 0) {
                    dangling_sum += rank[i];
                }
            }

            double base_val = ((1.0 - damping) + damping * dangling_sum) / n;
            std::fill(next_rank.begin(), next_rank.end(), base_val);

            // Accumulate contributions from each node u to its outgoing targets v
            #pragma omp parallel for schedule(dynamic, 64)
            for (int64_t u = 0; u < static_cast<int64_t>(n); ++u) {
                uint32_t deg = graph.degree(static_cast<uint32_t>(u));
                if (deg == 0) continue;

                double contribution = damping * rank[u] * inv_out_degree[u];
                uint32_t start_edge = graph.row_ptr[u];
                uint32_t end_edge = graph.row_ptr[u + 1];

                for (uint32_t e = start_edge; e < end_edge; ++e) {
                    uint32_t v = graph.col_ind[e];
                    #pragma omp atomic
                    next_rank[v] += contribution;
                }
            }

            // Check convergence
            double diff = 0.0;
            #pragma omp parallel for reduction(+:diff) schedule(static)
            for (int64_t i = 0; i < static_cast<int64_t>(n); ++i) {
                diff += std::abs(next_rank[i] - rank[i]);
            }

            rank = next_rank;
            if (diff < tolerance) break;
        }

        // Normalize sum to 1.0 (Pass criteria TC-03)
        double total_sum = 0.0;
        for (double r : rank) total_sum += r;
        if (total_sum > 0.0) {
            for (double& r : rank) r /= total_sum;
        }

        return rank;
    }

    // FR-3.2: Community Clustering - Label Propagation Algorithm (LPA)
    // Uses randomized permutation array to prevent label oscillation
    static std::vector<uint32_t> compute_lpa(
        const CSRGraph& graph, 
        uint32_t max_iterations = 15
    ) {
        uint32_t n = graph.num_nodes;
        if (n == 0) return {};

        std::vector<uint32_t> labels(n);
        std::iota(labels.begin(), labels.end(), 0);

        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);

        std::mt19937 rng(42); // Deterministic seed for testing reproducibility

        for (uint32_t iter = 0; iter < max_iterations; ++iter) {
            std::shuffle(order.begin(), order.end(), rng);
            bool changed = false;

            for (uint32_t u : order) {
                uint32_t start_edge = graph.row_ptr[u];
                uint32_t end_edge = graph.row_ptr[u + 1];
                if (start_edge == end_edge) continue;

                const bool has_weights = !graph.weight_val.empty();

                // Count label frequency weighted by edge weights
                std::unordered_map<uint32_t, float> label_weights;
                for (uint32_t e = start_edge; e < end_edge; ++e) {
                    uint32_t neighbor = graph.col_ind[e];
                    float w = has_weights ? graph.weight_val[e] : 1.0f;
                    label_weights[labels[neighbor]] += w;
                }

                if (label_weights.empty()) continue;

                // Find majority label
                uint32_t best_label = labels[u];
                float max_weight = -1.0f;
                for (const auto& [lbl, wt] : label_weights) {
                    if (wt > max_weight) {
                        max_weight = wt;
                        best_label = lbl;
                    }
                }

                if (best_label != labels[u]) {
                    labels[u] = best_label;
                    changed = true;
                }
            }

            if (!changed) break;
        }

        return labels;
    }

    // FR-3.3: Virality Simulation - Queue-based BFS Cascade
    // Isolates leaf nodes where out-degree = 0 and tracks cascade reach depth
    static std::vector<CascadeStep> trace_virality_cascade(
        const CSRGraph& graph, 
        uint32_t source_node
    ) {
        uint32_t n = graph.num_nodes;
        if (source_node >= n) return {};

        std::vector<CascadeStep> trace;
        std::vector<bool> visited(n, false);
        std::queue<std::pair<uint32_t, uint32_t>> q; // {node_id, depth}

        visited[source_node] = true;
        q.push({source_node, 0});

        while (!q.empty()) {
            auto [curr, depth] = q.front();
            q.pop();

            uint32_t deg = graph.degree(curr);
            bool is_leaf = (deg == 0);

            trace.push_back(CascadeStep{curr, depth, is_leaf});

            uint32_t start_edge = graph.row_ptr[curr];
            uint32_t end_edge = graph.row_ptr[curr + 1];

            for (uint32_t e = start_edge; e < end_edge; ++e) {
                uint32_t neighbor = graph.col_ind[e];
                if (!visited[neighbor]) {
                    visited[neighbor] = true;
                    q.push({neighbor, depth + 1});
                }
            }
        }

        return trace;
    }

    // FR-3.5: Probabilistic Virality Cascade with Distance Decay & Fatigue
    // Unlike trace_virality_cascade (deterministic BFS that infects every reachable
    // neighbor), this model transmits along each edge with probability
    //   P(u -> v) = clamp(S(u,v) * D(d) * M(t), 0, 1)
    // where S is interaction strength, D is distance decay and M is message fatigue.
    //
    // Interaction strength S(u,v) = alpha*F + beta*R + gamma*T, clamped to [0,1]:
    //   F: parallel-edge count u->v divided by u's max parallel-edge count to any neighbor
    //   R: 1.0 if a reciprocal edge v->u exists, else 0.0
    //   T: (t_last(u,v) - t_min) / (t_max - t_min) over graph-wide timestamp_sec;
    //      1.0 when t_max == t_min (or the graph has no timestamps)
    //
    // Distance decay D(d) = exp(-lambda * d), where d is the tier of the node being
    // attempted (seed neighbors are tier 1).
    //
    // Fatigue M(t) = 1 / (1 + kappa * t), where t is the tier index of the PARENT node.
    // We deliberately use tier index instead of wall-clock time so that results are
    // fully deterministic for a given rng_seed and graph snapshot.
    //
    // Complexity: O(V + E) per cascade. Global t_min/t_max are computed once up front,
    // and per-node edge profiles (parallel counts, last timestamps, neighbor sets for
    // reciprocity) are built lazily so each node's adjacency is scanned at most once.
    static CascadeResult trace_virality_cascade_with_decay(
        const CSRGraph& graph,
        const GraphStorage& storage,
        uint32_t seed_node,
        const CascadeConfig& config
    ) {
        CascadeResult result;
        const uint32_t n = graph.num_nodes;
        if (n == 0 || seed_node >= n) return result;

        const auto clamp01 = [](double x) { return std::min(1.0, std::max(0.0, x)); };

        // Global timestamp bounds across the whole graph (single O(V+E) pass).
        uint32_t t_min = UINT32_MAX;
        uint32_t t_max = 0;
        for (uint32_t u = 0; u < n; ++u) {
            for (const auto& e : storage.get_edges(u)) {
                t_min = std::min(t_min, e.timestamp_sec);
                t_max = std::max(t_max, e.timestamp_sec);
            }
        }
        const bool uniform_time = (t_max <= t_min); // also covers the no-edge case

        // Lazily-built per-node edge profile. Each node is scanned at most once,
        // keeping total work O(V + E) even on the multi-million-edge CSV graph.
        struct NodeEdgeProfile {
            // target -> {parallel edge count, most recent timestamp_sec}
            std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> pair_stats;
            std::unordered_set<uint32_t> neighbors; // unique out-neighbors (for R)
            uint32_t max_parallel = 0;
        };
        std::unordered_map<uint32_t, NodeEdgeProfile> profiles;

        auto build_profile = [&](uint32_t u) {
            NodeEdgeProfile prof;
            for (const auto& e : storage.get_edges(u)) {
                auto& st = prof.pair_stats[e.target_id];
                st.first += 1;
                st.second = std::max(st.second, e.timestamp_sec);
                prof.neighbors.insert(e.target_id);
                prof.max_parallel = std::max(prof.max_parallel, st.first);
            }
            profiles.emplace(u, std::move(prof));
        };
        auto ensure_profile = [&](uint32_t u) {
            if (profiles.find(u) == profiles.end()) build_profile(u);
        };

        // S(u,v): interaction strength in [0,1]
        auto strength = [&](uint32_t u, uint32_t v) -> double {
            ensure_profile(u);
            ensure_profile(v); // both inserts done before taking references (rehash safety)
            const NodeEdgeProfile& pu = profiles.find(u)->second;
            const NodeEdgeProfile& pv = profiles.find(v)->second;

            auto pit = pu.pair_stats.find(v);
            if (pit == pu.pair_stats.end()) return 0.0;

            double freq = (pu.max_parallel > 0)
                ? static_cast<double>(pit->second.first) / pu.max_parallel
                : 0.0;
            double recip = pv.neighbors.count(u) ? 1.0 : 0.0;
            double recency = uniform_time
                ? 1.0
                : static_cast<double>(pit->second.second - t_min) /
                  static_cast<double>(t_max - t_min);

            return clamp01(config.alpha * freq + config.beta * recip + config.gamma * recency);
        };

        std::vector<CascadeTierStats> tier_stats(config.max_tiers + 1);
        for (uint32_t t = 0; t <= config.max_tiers; ++t) tier_stats[t].tier = t;
        tier_stats[0].reached = 1; // seed is always infected at tier 0

        std::vector<char> visited(n, 0);
        std::vector<uint32_t> seen(n, 0); // per-parent dedup stamp for parallel edges
        uint32_t stamp = 0;

        std::queue<std::pair<uint32_t, uint32_t>> q; // {node_id, tier}
        std::mt19937_64 rng(config.rng_seed);
        std::uniform_real_distribution<double> uni(0.0, 1.0);

        visited[seed_node] = 1;
        q.push({seed_node, 0});
        result.steps.push_back(CascadeStep{seed_node, 0, graph.degree(seed_node) == 0});

        while (!q.empty()) {
            auto [curr, tier] = q.front();
            q.pop();

            // Nodes at max_tiers are reached but do not spread further.
            if (tier >= config.max_tiers) continue;

            const uint32_t target_tier = tier + 1;
            ++stamp;

            uint32_t start_edge = graph.row_ptr[curr];
            uint32_t end_edge = graph.row_ptr[curr + 1];

            for (uint32_t e = start_edge; e < end_edge; ++e) {
                uint32_t v = graph.col_ind[e];
                if (v >= n || seen[v] == stamp) continue; // one attempt per unique neighbor
                seen[v] = stamp;
                if (visited[v]) continue;                 // already infected: not attempted

                tier_stats[target_tier].attempted++;
                result.total_attempted++;

                double dist_decay = std::exp(-config.decay_rate_lambda *
                                             static_cast<double>(target_tier));
                double fatigue = 1.0 / (1.0 + config.fatigue_rate_kappa *
                                        static_cast<double>(tier));
                double p = clamp01(strength(curr, v) * dist_decay * fatigue);

                if (uni(rng) < p) {
                    visited[v] = 1;
                    q.push({v, target_tier});
                    result.steps.push_back(CascadeStep{v, target_tier,
                                                       graph.degree(v) == 0});
                    tier_stats[target_tier].reached++;
                }
            }
        }

        result.total_reached = result.steps.size();
        for (const auto& ts : tier_stats) {
            CascadeTierStats out = ts;
            out.reach_ratio = (ts.tier == 0)
                ? (ts.reached > 0 ? 1.0 : 0.0)
                : (ts.attempted > 0
                       ? static_cast<double>(ts.reached) / ts.attempted
                       : 0.0);
            result.tiers.push_back(out);
        }

        // The seed counts as a guaranteed infection, so the denominator is
        // total_attempted + 1 (the seed). This keeps the ratio within [0, 1].
        result.actual_reach_ratio =
            (result.total_attempted > 0)
                ? static_cast<double>(result.total_reached) /
                      static_cast<double>(result.total_attempted + 1)
                : (result.total_reached > 0 ? 1.0 : 0.0);

        return result;
    }

    // FR-3.4: Bot Swarm Detection - 3-Color DFS Cycle Tracking
    // WHITE (0) = unvisited, GRAY (1) = visiting (in recursion stack), BLACK (2) = fully visited
    // Identifies directed loops (length <= 5) and filters with temporal velocity bounds
    enum Color : uint8_t { WHITE = 0, GRAY = 1, BLACK = 2 };

    static std::vector<BotCycle> detect_bot_swarms(
        const CSRGraph& graph, 
        const GraphStorage& storage,
        uint32_t max_cycle_length = 5,
        double max_anomaly_delta_ms = 50.0 // Velocity bound: cycles occurring in < 50ms
    ) {
        uint32_t n = graph.num_nodes;
        if (n == 0) return {};

        std::vector<uint8_t> color(n, WHITE);
        std::vector<uint32_t> path;
        std::vector<BotCycle> detected_cycles;

        auto dfs = [&](auto& self, uint32_t u, uint32_t start_node, uint32_t depth) -> void {
            if (depth > max_cycle_length) return;

            color[u] = GRAY;
            path.push_back(u);

            uint32_t start_edge = graph.row_ptr[u];
            uint32_t end_edge = graph.row_ptr[u + 1];

            for (uint32_t e = start_edge; e < end_edge; ++e) {
                uint32_t v = graph.col_ind[e];

                if (v == start_node && path.size() >= 2 && path.size() <= max_cycle_length) {
                    // Closed cycle found: path -> start_node
                    // Check temporal velocity bounds and interaction ratios
                    double total_latency = 0.0;
                    size_t edge_count = 0;
                    size_t repost_count = 0;

                    for (size_t i = 0; i < path.size(); ++i) {
                        uint32_t from = path[i];
                        uint32_t to = (i + 1 < path.size()) ? path[i + 1] : start_node;

                        const auto& edges = storage.get_edges(from);
                        for (const auto& edge : edges) {
                            if (edge.target_id == to) {
                                total_latency += edge.latency_ms;
                                edge_count++;
                                if (edge.type == static_cast<uint8_t>(InteractionType::REPOST)) {
                                    repost_count++;
                                }
                            }
                        }
                    }

                    double avg_delta = (edge_count > 0) ? (total_latency / edge_count) : 0.0;
                    double repost_ratio = (edge_count > 0) ? (static_cast<double>(repost_count) / edge_count) : 0.0;

                    // TC-05 & TC-06: Flag if rapid cycle anomaly or high repost loop ratio
                    if (avg_delta <= max_anomaly_delta_ms || repost_ratio >= 0.8) {
                        BotCycle cycle;
                        cycle.path = path;
                        cycle.min_delta_ms = avg_delta;
                        cycle.multi_edge_ratio = repost_ratio;
                        cycle.reason = "High-velocity coordinated loop (Δt=" + 
                                       std::to_string(static_cast<int>(avg_delta)) + "ms, Repost=" + 
                                       std::to_string(static_cast<int>(repost_ratio * 100)) + "%)";
                        detected_cycles.push_back(cycle);
                    }
                } else if (color[v] == WHITE) {
                    self(self, v, start_node, depth + 1);
                }
            }

            path.pop_back();
            color[u] = WHITE; // Reset for all simple cycles exploration up to length 5
        };

        for (uint32_t i = 0; i < n; ++i) {
            dfs(dfs, i, i, 1);
        }

        return detected_cycles;
    }
};

} // namespace influmapper

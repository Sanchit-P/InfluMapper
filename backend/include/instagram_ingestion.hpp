#pragma once

// Instagram/Threads ingestion engine — deterministic synthetic creator graph.
// (Reused for Threads via InstagramConfig::handle_prefix = "th:" until the
// real Meta Graph API credentials are approved.)
//
// Generates a realistic Instagram-style network (niche communities, hub-spoke
// influencers, engagement pods, inactive followers) and ingests it as real
// XInteractionEvents + NodeMetadata into GraphStorage, so PageRank / LPA /
// bot-cycle detection / decay cascades all work on it unchanged.
//
// Deterministic: same config seed -> identical graph every run, which keeps
// snapshot persistence and tests reproducible.

#include "types.hpp"
#include "graph_storage.hpp"

#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <unordered_map>
#include <cstdint>

namespace influmapper {

struct InstagramConfig {
    std::string handle = "ig:fashionicon";   // seed influencer handle
    std::string handle_prefix = "ig:";       // "ig:" for Instagram, "th:" for Threads
    int mock_node_count = 60;                // total creator accounts
    uint64_t rng_seed = 1337;                // deterministic generation
};

struct InstagramIngestionStats {
    size_t edges_ingested = 0;
    size_t unique_users = 0;
    size_t bots_flagged = 0;
};

class InstagramIngestionEngine {
private:
    InstagramConfig config_;
    GraphStorage& storage_;
    std::mt19937 rng_;

    struct Actor {
        std::string handle;
        std::string niche;
        uint32_t followers = 0;
        uint32_t following = 0;
        bool is_bot = false;
        int pod_id = -1;          // >=0 -> member of bot pod N
        bool mega = false;        // hub influencer
    };

    std::vector<Actor> actors_;
    std::unordered_map<std::string, uint32_t> handle_to_node_;

    static const inline std::vector<std::pair<std::string, int>> kNiches = {
        {"fashion", 10}, {"fitness", 10}, {"food", 8}, {"travel", 8},
        {"tech", 8}, {"art", 7}, {"music", 5}, {"beauty", 4}
    };

    std::string make_handle(const std::string& niche, int idx) const {
        static const char* kSuffix[] = {
            "lover", "guru", "daily", "vibes", "world", "addict", "hub", "diary"
        };
        std::string h = niche + kSuffix[idx % 8];
        int num = idx / 8;
        if (num > 0) h += std::to_string(num);
        return config_.handle_prefix + h;
    }

    void build_actors() {
        actors_.clear();
        handle_to_node_.clear();
        std::uniform_int_distribution<int> foll_small(800, 20000);
        std::uniform_int_distribution<int> foll_mid(20000, 200000);
        std::uniform_int_distribution<int> foll_mega(200000, 2000000);
        std::uniform_int_distribution<int> foll_bot(300, 5000);
        std::uniform_int_distribution<int> following_dist(150, 2500);

        int idx = 0;
        int mega_count = 0, mid_count = 0;
        for (const auto& [niche, quota] : kNiches) {
            for (int i = 0; i < quota && static_cast<int>(actors_.size()) < config_.mock_node_count; ++i, ++idx) {
                Actor a;
                a.handle = make_handle(niche, idx);
                a.niche = niche;

                // Tier assignment: first 3 overall -> mega, next ~10 -> mid
                if (mega_count < 3) { a.followers = foll_mega(rng_); a.mega = true; ++mega_count; }
                else if (mid_count < 10) { a.followers = foll_mid(rng_); ++mid_count; }
                else a.followers = foll_small(rng_);
                a.following = std::min<uint32_t>(a.followers / 4 + 50, following_dist(rng_));
                actors_.push_back(a);
            }
        }

        // Ensure the configured seed handle exists as a mega influencer
        bool seed_exists = false;
        for (auto& a : actors_) if (a.handle == config_.handle) seed_exists = true;
        if (!seed_exists && !actors_.empty()) {
            actors_[0].handle = config_.handle;
            actors_[0].niche = "fashion";
            actors_[0].mega = true;
            actors_[0].followers = foll_mega(rng_);
        }

        // --- Bot pods: 3 pods x 3-4 members, disjoint from niche structure ---
        // Pods reuse niche handles with a "pod" marker so they look camouflaged.
        std::uniform_int_distribution<int> pod_size_dist(3, 4);
        int pod_members = 0;
        for (int pod = 0; pod < 3 && pod_members + 4 <= static_cast<int>(actors_.size()); ++pod) {
            int size = pod_size_dist(rng_);
            for (int m = 0; m < size; ++m) {
                Actor& a = actors_[actors_.size() - 1 - pod_members - m]; // take from tail
                a.is_bot = true;
                a.pod_id = pod;
                a.followers = foll_bot(rng_);
                a.following = foll_bot(rng_);
                a.handle = config_.handle_prefix + "pod" + std::to_string(pod) + "acct" + std::to_string(m);
            }
            pod_members += size;
        }
    }

    uint32_t node_id_for(const Actor& a) {
        auto it = handle_to_node_.find(a.handle);
        if (it != handle_to_node_.end()) return it->second;
        uint32_t id = storage_.get_or_create_node(a.handle, "ig");
        storage_.update_node_profile(
            id, a.handle, "",
            a.niche + (a.is_bot ? " | high-velocity engagement" : ""),
            a.followers, a.following);
        if (a.is_bot) storage_.mark_bot(id, true);
        handle_to_node_[a.handle] = id;
        return id;
    }

    void add_edge(uint32_t src, uint32_t dst, InteractionType type,
                  uint32_t ts, uint16_t latency_ms) {
        XInteractionEvent ev{};
        ev.source_id = src;
        ev.target_id = dst;
        ev.timestamp_sec = ts;
        ev.type = static_cast<uint8_t>(type);
        ev.latency_ms = latency_ms;
        storage_.ingest_batch(&ev, 1);
        ++stats_.edges_ingested;
    }

    InstagramIngestionStats stats_;

public:
    InstagramIngestionEngine(InstagramConfig config, GraphStorage& storage)
        : config_(std::move(config)), storage_(storage),
          rng_(config_.rng_seed) {}

    // Build the synthetic network and ingest it. Deterministic per rng_seed.
    size_t ingest_mock_network() {
        build_actors();
        stats_ = {};

        // Register all actors first so IDs are stable
        std::vector<uint32_t> ids;
        ids.reserve(actors_.size());
        for (const auto& a : actors_) ids.push_back(node_id_for(a));
        stats_.unique_users = actors_.size();
        for (const auto& a : actors_) if (a.is_bot) ++stats_.bots_flagged;

        if (actors_.empty()) return 0;

        std::uniform_int_distribution<size_t> pick(0, actors_.size() - 1);
        std::uniform_int_distribution<int> lat_normal(5000, 300000);   // 5s - 5min
        std::uniform_int_distribution<int> lat_bot(20, 400);           // pod-speed reactions
        std::uniform_real_distribution<double> cross_prob(0.0, 1.0);
        const double kCrossNicheProb = 0.30;

        // Timestamps: engagements clustered in the last ~30 days (epoch ~1.76e9)
        const uint32_t base_ts = 1760000000u;
        uint32_t ts = base_ts;

        // --- Pod rings: members ONLY interact inside their pod, ultra-low latency ---
        for (size_t i = 0; i < actors_.size(); ++i) {
            const Actor& a = actors_[i];
            if (a.pod_id < 0) continue;
            // closed directed cycle within the pod + a few reciprocal likes
            for (size_t j = 0; j < actors_.size(); ++j) {
                const Actor& b = actors_[j];
                if (b.pod_id != a.pod_id || i == j) continue;
                add_edge(ids[i], ids[j],
                         (j % 2 == 0) ? InteractionType::REPOST : InteractionType::LIKE,
                         ts, static_cast<uint16_t>(lat_bot(rng_)));
                ts += 1 + static_cast<uint32_t>(rng_() % 3);
            }
        }

        // --- Organic engagement: 70% intra-niche, 30% cross-niche ---
        for (size_t i = 0; i < actors_.size(); ++i) {
            const Actor& a = actors_[i];
            if (a.is_bot) continue; // pods already handled

            int interactions = a.mega ? 14 : (a.followers > 20000 ? 9 : 5);
            for (int k = 0; k < interactions; ++k) {
                // Pick target: same niche 70% of the time, else any actor
                size_t t = pick(rng_);
                if (cross_prob(rng_) >= kCrossNicheProb) {
                    for (int tries = 0; tries < 8 && actors_[t].niche != a.niche; ++tries)
                        t = pick(rng_);
                }
                if (t == i || actors_[t].is_bot) continue;

                // Hub-spoke: mega influencers mostly RECEIVE, small accounts mostly GIVE
                bool reverse = a.mega ? (rng_() % 4 != 0) : false;
                uint32_t src = reverse ? ids[t] : ids[i];
                uint32_t dst = reverse ? ids[i] : ids[t];

                // Weighted interaction type: likes dominate, then comments/shares/saves
                int roll = static_cast<int>(rng_() % 100);
                InteractionType type =
                    roll < 55 ? InteractionType::LIKE :
                    roll < 75 ? InteractionType::REPLY :
                    roll < 90 ? InteractionType::REPOST :
                                InteractionType::BOOKMARK;

                add_edge(src, dst, type, ts,
                         static_cast<uint16_t>(lat_normal(rng_)));
                ts += 60 + static_cast<uint32_t>(rng_() % 600); // organic pacing
            }
        }

        // --- Reciprocal follows (~40%): mutual REPLY edges between niche peers ---
        for (size_t i = 0; i + 1 < actors_.size(); i += 2) {
            if (actors_[i].is_bot || actors_[i + 1].is_bot) continue;
            if (actors_[i].niche != actors_[i + 1].niche) continue;
            if (rng_() % 100 >= 40) continue;
            add_edge(ids[i], ids[i + 1], InteractionType::REPLY, ts,
                     static_cast<uint16_t>(lat_normal(rng_)));
            add_edge(ids[i + 1], ids[i], InteractionType::REPLY, ts + 30,
                     static_cast<uint16_t>(lat_normal(rng_)));
            ts += 120;
        }

        return stats_.edges_ingested;
    }

    InstagramIngestionStats get_stats() const { return stats_; }
};

} // namespace influmapper

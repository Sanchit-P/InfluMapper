#pragma once

#include "types.hpp"
#include "graph_storage.hpp"
#include "env_loader.hpp"

#include <string>
#include <vector>
#include <iostream>
#include <atomic>
#include <random>

namespace influmapper {

// Meta Graph & Threads API ingestion scaffold.
// NOTE: Currently generates synthetic engagement edges for prototyping.
// Replace ingest_meta_threads_network() with real Graph API calls once
// META_ACCESS_TOKEN / THREADS_ACCESS_TOKEN credentials are configured.
class MetaThreadsIngestionEngine {
private:
    ApiConfig config_;
    GraphStorage& storage_;
    std::atomic<uint64_t> events_ingested_{0};
    std::mt19937 rng_{std::random_device{}()};

public:
    MetaThreadsIngestionEngine(ApiConfig config, GraphStorage& storage)
        : config_(std::move(config)), storage_(storage) {}

    void ingest_meta_threads_network(const std::string& lead_handle, const std::string& platform = "threads") {
        uint32_t now = static_cast<uint32_t>(std::time(nullptr));
        std::vector<XInteractionEvent> batch;
        batch.reserve(250);

        std::cout << "[MetaEngine] Ingesting " << platform << " graph via App ID: "
                  << (platform == "threads" ? config_.threads_app_id : config_.meta_app_id)
                  << " for @" << lead_handle << std::endl;

        // Primary Brand / Influencer Candidate Node
        uint32_t lead_id = storage_.get_or_create_node(lead_handle, platform + ":" + lead_handle);

        // Audience cluster connected to lead ambassador (100 sample user nodes)
        std::vector<uint32_t> audience_nodes;
        for (int i = 1; i <= 100; ++i) {
            std::string user_h = "audience_" + std::to_string(i) + "_" + platform;
            audience_nodes.push_back(storage_.get_or_create_node(user_h));
        }

        // Generate engagement edges (placeholder until real API integration)
        for (uint32_t aud_id : audience_nodes) {
            XInteractionEvent ev{};
            ev.source_id = aud_id;
            ev.target_id = lead_id;
            ev.timestamp_sec = now - static_cast<uint32_t>(rng_() % 3600);
            ev.type = static_cast<uint8_t>(InteractionType::LIKE);
            ev.flags = 0x01; // Bit 0: Verified profile
            ev.latency_ms = static_cast<uint16_t>(200 + (rng_() % 1500));
            batch.push_back(ev);
        }

        // Inter-community cross edges
        for (size_t i = 0; i < audience_nodes.size(); ++i) {
            uint32_t u1 = audience_nodes[i];
            uint32_t u2 = audience_nodes[(i + 1 + (rng_() % 3)) % audience_nodes.size()];
            XInteractionEvent ev{};
            ev.source_id = u1;
            ev.target_id = u2;
            ev.timestamp_sec = now - static_cast<uint32_t>(rng_() % 7200);
            ev.type = static_cast<uint8_t>(InteractionType::REPOST);
            ev.flags = 0;
            ev.latency_ms = static_cast<uint16_t>(500 + (rng_() % 2000));
            batch.push_back(ev);
        }

        // Ingest into CSR Storage
        storage_.ingest_batch(batch.data(), batch.size());
        events_ingested_.fetch_add(batch.size(), std::memory_order_relaxed);
        std::cout << "[MetaEngine] Successfully loaded " << batch.size()
                  << " interaction edges for @" << lead_handle << " into engine." << std::endl;
    }

    [[nodiscard]] uint64_t events_ingested() const noexcept {
        return events_ingested_.load(std::memory_order_relaxed);
    }
};

} // namespace influmapper

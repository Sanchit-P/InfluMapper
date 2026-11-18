#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>

namespace influmapper {

// Enum representing the type of social interaction
enum class InteractionType : uint8_t {
    LIKE = 0,
    REPLY = 1,
    REPOST = 2,
    QUOTE = 3,
    BOOKMARK = 4
};

// FR-1.2: 16-byte packed struct aligned to 16 bytes.
// Fits exactly 4 events per 64-byte L1 CPU cache line.
struct alignas(16) XInteractionEvent {
    uint32_t source_id;      // 4 bytes: Contiguous 32-bit user ID of initiator
    uint32_t target_id;      // 4 bytes: Contiguous 32-bit user ID of recipient
    uint32_t timestamp_sec;  // 4 bytes: Unix epoch timestamp in seconds
    uint8_t  type;           // 1 byte: InteractionType
    uint8_t  flags;          // 1 byte: e.g. bit 0: verified, bit 1: bot suspect
    uint16_t latency_ms;     // 2 bytes: Response latency / delta (0-65535 ms)
};
static_assert(sizeof(XInteractionEvent) == 16, "XInteractionEvent must be exactly 16 bytes");
static_assert(alignof(XInteractionEvent) == 16, "XInteractionEvent must be 16-byte aligned");

// Dynamic multigraph edge
struct XEdge {
    uint32_t target_id;
    uint32_t timestamp_sec;
    uint8_t  type;
    uint16_t latency_ms;
};

// Compressed Sparse Row (CSR) read-only snapshot projection
struct CSRGraph {
    std::vector<uint32_t> row_ptr;    // Size: num_nodes + 1
    std::vector<uint32_t> col_ind;    // Size: total_edges
    std::vector<float>    weight_val;   // Size: total_edges (interaction weights)
    uint32_t num_nodes = 0;
    uint32_t num_edges = 0;
    uint64_t snapshot_version = 0;

    [[nodiscard]] inline uint32_t degree(uint32_t u) const noexcept {
        if (u + 1 >= row_ptr.size()) return 0;
        return row_ptr[u + 1] - row_ptr[u];
    }
};

// Node metadata for presentation and filtering
struct NodeMetadata {
    uint32_t id = 0;
    std::string handle;
    std::string did;
    std::string display_name;
    std::string avatar_url;
    std::string description;
    uint32_t followers_count = 0;
    uint32_t following_count = 0;
    double pagerank_score = 0.0;
    uint32_t community_id = 0;
    bool is_bot = false;
    uint32_t in_degree = 0;
    uint32_t out_degree = 0;
};

// Bot cycle loop structure for detection alerts
struct BotCycle {
    std::vector<uint32_t> path; // Sequence of user IDs forming closed loop
    double min_delta_ms;
    double multi_edge_ratio;
    std::string reason;
};

// Virality cascade trace step
struct CascadeStep {
    uint32_t node_id;
    uint32_t depth;
    bool is_leaf;
};

} // namespace influmapper

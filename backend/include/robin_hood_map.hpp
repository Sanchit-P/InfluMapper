#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <cstring>
#include <utility>

namespace influmapper {

// FR-1.3: Robin Hood Hash Map mapping string handles/DIDs to contiguous uint32_t IDs.
// Uses open addressing with backward-shift deletion and Robin Hood displacement to maintain
// minimal probe sequence lengths (PSL) and optimal L1 cache locality.
class RobinHoodHandleMap {
private:
    struct Entry {
        std::string key;
        uint32_t val = 0;
        uint16_t psl = 0;
        bool occupied = false;
    };

    std::vector<Entry> table_;
    size_t size_ = 0;
    size_t mask_ = 0;
    uint32_t next_id_ = 0;

    // FNV-1a 64-bit hash function
    [[nodiscard]] static inline uint64_t hash_str(const std::string& str) noexcept {
        uint64_t hash = 14695981039346656037ULL;
        for (char c : str) {
            hash ^= static_cast<uint8_t>(c);
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    void rehash(size_t new_capacity) {
        std::vector<Entry> old_table = std::move(table_);
        table_.assign(new_capacity, Entry{});
        mask_ = new_capacity - 1;
        size_ = 0;

        for (auto& entry : old_table) {
            if (entry.occupied) {
                insert_internal(std::move(entry.key), entry.val);
            }
        }
    }

    uint32_t insert_internal(std::string key, uint32_t val) {
        if (size_ * 10 >= table_.size() * 8) { // Load factor 0.8
            rehash(table_.empty() ? 64 : table_.size() * 2);
        }

        uint64_t h = hash_str(key);
        size_t idx = h & mask_;
        uint16_t psl = 0;

        Entry curr{std::move(key), val, psl, true};

        while (true) {
            if (!table_[idx].occupied) {
                curr.psl = psl;
                table_[idx] = std::move(curr);
                ++size_;
                return val;
            }

            if (table_[idx].key == curr.key) {
                return table_[idx].val; // Already present
            }

            // Robin Hood rule: if current entry has traveled further than incumbent, swap them
            if (psl > table_[idx].psl) {
                curr.psl = psl;
                std::swap(curr, table_[idx]);
                psl = curr.psl;
            }

            idx = (idx + 1) & mask_;
            ++psl;
        }
    }

public:
    explicit RobinHoodHandleMap(size_t initial_capacity = 256) {
        size_t cap = 16;
        while (cap < initial_capacity) cap <<= 1;
        table_.assign(cap, Entry{});
        mask_ = cap - 1;
    }

    // Get existing ID or assign a new contiguous uint32_t ID
    uint32_t get_or_create(const std::string& key) {
        if (key.empty()) return 0;
        
        auto existing = find(key);
        if (existing.has_value()) {
            return *existing;
        }

        uint32_t assigned_id = next_id_++;
        return insert_internal(key, assigned_id);
    }

    [[nodiscard]] std::optional<uint32_t> find(const std::string& key) const noexcept {
        if (table_.empty()) return std::nullopt;

        uint64_t h = hash_str(key);
        size_t idx = h & mask_;
        uint16_t psl = 0;

        while (true) {
            if (!table_[idx].occupied || psl > table_[idx].psl) {
                return std::nullopt;
            }
            if (table_[idx].key == key) {
                return table_[idx].val;
            }
            idx = (idx + 1) & mask_;
            ++psl;
        }
    }

    [[nodiscard]] size_t size() const noexcept { return size_; }
    [[nodiscard]] uint32_t next_id() const noexcept { return next_id_; }

    // Reset the table to an empty state (used when loading a persisted snapshot)
    void clear() noexcept {
        table_.assign(64, Entry{});
        mask_ = table_.size() - 1;
        size_ = 0;
        next_id_ = 0;
    }
};

} // namespace influmapper

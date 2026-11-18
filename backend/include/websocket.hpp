#pragma once

// Minimal RFC 6455 WebSocket server helpers (zero external dependencies).
// Supports: HTTP Upgrade handshake, unmasked server->client text frames,
// masked client->server frame parsing, ping/pong, close.

#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <cctype>
#include <array>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close
#endif

namespace influmapper {
namespace ws {

// ---------------- SHA-1 (needed for Sec-WebSocket-Accept) ----------------
class Sha1 {
public:
    Sha1() { reset(); }

    void update(const uint8_t* data, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            buffer_[buffer_len_++] = data[i];
            if (buffer_len_ == 64) {
                process_block(buffer_.data());
                buffer_len_ = 0;
            }
        }
        total_len_ += len;
    }

    void update(const std::string& s) {
        update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
    }

    std::array<uint8_t, 20> final() {
        uint64_t bit_len = total_len_ * 8;
        uint8_t pad = 0x80;
        update(&pad, 1);
        uint8_t zero = 0x00;
        while (buffer_len_ != 56) update(&zero, 1);
        uint8_t len_bytes[8];
        for (int i = 0; i < 8; ++i) len_bytes[i] = static_cast<uint8_t>(bit_len >> (56 - i * 8));
        update(len_bytes, 8);

        std::array<uint8_t, 20> out;
        for (int i = 0; i < 5; ++i) {
            out[i * 4 + 0] = static_cast<uint8_t>(h_[i] >> 24);
            out[i * 4 + 1] = static_cast<uint8_t>(h_[i] >> 16);
            out[i * 4 + 2] = static_cast<uint8_t>(h_[i] >> 8);
            out[i * 4 + 3] = static_cast<uint8_t>(h_[i]);
        }
        return out;
    }

private:
    void reset() {
        h_[0] = 0x67452301; h_[1] = 0xEFCDAB89; h_[2] = 0x98BADCFE;
        h_[3] = 0x10325476; h_[4] = 0xC3D2E1F0;
        buffer_len_ = 0;
        total_len_ = 0;
    }

    static uint32_t rol(uint32_t v, int n) { return (v << n) | (v >> (32 - n)); }

    void process_block(const uint8_t* p) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(p[i * 4]) << 24) |
                   (static_cast<uint32_t>(p[i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(p[i * 4 + 2]) << 8) |
                   static_cast<uint32_t>(p[i * 4 + 3]);
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20)      { f = (b & c) | ((~b) & d);          k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ d;                    k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d);   k = 0x8F1BBCDC; }
            else             { f = b ^ c ^ d;                    k = 0xCA62C1D6; }
            uint32_t temp = rol(a, 5) + f + e + k + w[i];
            e = d; d = c; c = rol(b, 30); b = a; a = temp;
        }
        h_[0] += a; h_[1] += b; h_[2] += c; h_[3] += d; h_[4] += e;
    }

    uint32_t h_[5]{};
    std::array<uint8_t, 64> buffer_{};
    size_t buffer_len_ = 0;
    uint64_t total_len_ = 0;
};

// ---------------- Base64 ----------------
inline std::string base64_encode(const uint8_t* data, size_t len) {
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (size_t i = 0; i < len; i += 3) {
        uint32_t v = static_cast<uint32_t>(data[i]) << 16;
        if (i + 1 < len) v |= static_cast<uint32_t>(data[i + 1]) << 8;
        if (i + 2 < len) v |= static_cast<uint32_t>(data[i + 2]);
        out += tbl[(v >> 18) & 0x3F];
        out += tbl[(v >> 12) & 0x3F];
        out += (i + 1 < len) ? tbl[(v >> 6) & 0x3F] : '=';
        out += (i + 2 < len) ? tbl[v & 0x3F] : '=';
    }
    return out;
}

// ---------------- Handshake ----------------
// Extract a header value (case-insensitive) from the raw HTTP request.
inline std::string get_header(const std::string& request, const std::string& name) {
    // Case-insensitive search for "name:" at a line start
    std::string lower_req = request;
    for (auto& c : lower_req) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    std::string lower_name = name;
    for (auto& c : lower_name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    std::string needle = lower_name + ":";

    size_t pos = lower_req.find(needle);
    if (pos == std::string::npos) return "";
    pos += needle.size();
    while (pos < request.size() && (request[pos] == ' ' || request[pos] == '\t')) ++pos;
    size_t end = request.find("\r\n", pos);
    if (end == std::string::npos) end = request.size();
    return request.substr(pos, end - pos);
}

// Compute Sec-WebSocket-Accept for a client key.
inline std::string accept_key(const std::string& client_key) {
    static const std::string guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    Sha1 sha;
    sha.update(client_key);
    sha.update(guid);
    auto digest = sha.final();
    return base64_encode(digest.data(), digest.size());
}

// Build the 101 Switching Protocols response.
inline std::string handshake_response(const std::string& client_key) {
    std::string accept = accept_key(client_key);
    return "HTTP/1.1 101 Switching Protocols\r\n"
           "Upgrade: websocket\r\n"
           "Connection: Upgrade\r\n"
           "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
}

// ---------------- Socket helpers ----------------
inline bool send_all(SOCKET sock, const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < len) {
        int n = send(sock, p + sent, static_cast<int>(len - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

inline bool recv_exact(SOCKET sock, void* dst, size_t len) {
    char* p = static_cast<char*>(dst);
    size_t got = 0;
    while (got < len) {
        int n = recv(sock, p + got, static_cast<int>(len - got), 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

// Send an unmasked server->client frame.
// opcode: 0x1 text, 0x2 binary, 0x8 close, 0x9 ping, 0xA pong
inline bool send_frame(SOCKET sock, uint8_t opcode, const std::string& payload) {
    std::vector<uint8_t> frame;
    frame.reserve(payload.size() + 14);
    frame.push_back(static_cast<uint8_t>(0x80 | opcode)); // FIN + opcode

    size_t len = payload.size();
    if (len < 126) {
        frame.push_back(static_cast<uint8_t>(len)); // unmasked
    } else if (len <= 0xFFFF) {
        frame.push_back(126);
        frame.push_back(static_cast<uint8_t>(len >> 8));
        frame.push_back(static_cast<uint8_t>(len));
    } else {
        frame.push_back(127);
        for (int i = 7; i >= 0; --i)
            frame.push_back(static_cast<uint8_t>(len >> (i * 8)));
    }
    if (!payload.empty())
        frame.insert(frame.end(), payload.begin(), payload.end());
    return send_all(sock, frame.data(), frame.size());
}

inline bool send_text(SOCKET sock, const std::string& payload) {
    return send_frame(sock, 0x1, payload);
}

inline bool send_close(SOCKET sock) {
    return send_frame(sock, 0x8, "");
}

inline bool send_pong(SOCKET sock, const std::string& payload) {
    return send_frame(sock, 0xA, payload);
}

// Receive one client->server frame. Client frames are masked per RFC 6455.
// Returns false on socket error/close. out_opcode/out_payload set on success.
// Handles fragmented messages by concatenating continuation frames.
inline bool recv_frame(SOCKET sock, uint8_t& out_opcode, std::string& out_payload) {
    out_opcode = 0;
    out_payload.clear();
    bool fragmented = false;

    while (true) {
        uint8_t hdr[2];
        if (!recv_exact(sock, hdr, 2)) return false;

        uint8_t fin = hdr[0] & 0x80;
        uint8_t opcode = hdr[0] & 0x0F;
        bool masked = (hdr[1] & 0x80) != 0;
        uint64_t len = hdr[1] & 0x7F;

        if (len == 126) {
            uint8_t ext[2];
            if (!recv_exact(sock, ext, 2)) return false;
            len = (static_cast<uint64_t>(ext[0]) << 8) | ext[1];
        } else if (len == 127) {
            uint8_t ext[8];
            if (!recv_exact(sock, ext, 8)) return false;
            len = 0;
            for (int i = 0; i < 8; ++i) len = (len << 8) | ext[i];
        }
        if (len > (64u * 1024u * 1024u)) return false; // sanity cap

        uint8_t mask_key[4] = {0, 0, 0, 0};
        if (masked && !recv_exact(sock, mask_key, 4)) return false;

        std::string payload(len, '\0');
        if (len && !recv_exact(sock, payload.data(), len)) return false;
        if (masked) {
            for (size_t i = 0; i < len; ++i)
                payload[i] = static_cast<char>(payload[i] ^ mask_key[i % 4]);
        }

        // Control frames (close/ping/pong) have FIN set and short payloads
        if (opcode >= 0x8) {
            out_opcode = opcode;
            out_payload = std::move(payload);
            return true;
        }

        if (opcode == 0x0) { // continuation
            out_payload += payload;
            if (fin) { fragmented = false; return true; }
            continue;
        }

        if (opcode == 0x1 || opcode == 0x2) { // text / binary start
            if (fragmented) return false; // protocol violation
            out_opcode = opcode;
            out_payload = std::move(payload);
            if (fin) return true;
            fragmented = true;
            continue;
        }

        // Unknown opcode -> protocol error
        return false;
    }
}

} // namespace ws
} // namespace influmapper

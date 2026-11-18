# InfluMapper: High-Throughput Graph Analytics & Brand Ambassador Reach Engine

An ultra-high-throughput C++20 graph analytics engine and real-time interactive dashboard designed for **Marketing Leads, Data Scientists, and Growth Strategists** to discover high-authority brand ambassadors ("Faces of the Product") with maximum organic information diffusion and verifiable anti-fraud screening.

---

## 📑 Table of Contents
1. [Overview & Core Value Proposition](#1-overview--core-value-proposition)
2. [End-to-End System Architecture](#2-end-to-end-system-architecture)
3. [Core Data Structures](#3-core-data-structures)
4. [Algorithmic Foundations & Mathematical Formulations](#4-algorithmic-foundations--mathematical-formulations)
   - [4.1 Network Authority: OpenMP Parallel PageRank](#41-network-authority-openmp-parallel-pagerank)
   - [4.2 Community Clustering: Label Propagation Algorithm (LPA)](#42-community-clustering-label-propagation-algorithm-lpa)
   - [4.3 Semantic & Keyword Niche Classification](#43-semantic--keyword-niche-classification)
   - [4.4 Anti-Fraud: 3-Color DFS Cycle Detection & Latency Bounds](#44-anti-fraud-3-color-dfs-cycle-detection--latency-bounds)
   - [4.5 Information Diffusion: Probabilistic Cascade Model with Decay & Fatigue](#45-information-diffusion-probabilistic-cascade-model-with-decay--fatigue)
   - [4.6 Ambassador Fit Score](#46-ambassador-fit-score)
5. [Data Sources & Ingestion Pipeline](#5-data-sources--ingestion-pipeline)
6. [Backend HTTP & WebSocket API Reference](#6-backend-http--websocket-api-reference)
7. [Frontend Interactive Dashboard](#7-frontend-interactive-dashboard)
8. [Build, Installation & Execution Guide](#8-build-installation--execution-guide)
9. [Verification & Test Suite](#9-verification--test-suite)
10. [Complexity & Hardware Efficiency Summary](#10-complexity--hardware-efficiency-summary)

---

## 1. Overview & Core Value Proposition

Modern creator marketing relies heavily on vanity metrics (follower counts, raw impressions) that are easily faked or disconnected from the brand's target demographic:
* **The Follower Paradox:** An account with 500,000 followers may have 60%+ bots, inactive accounts, or participation in engagement pods.
* **Audience Misalignment:** A creator may possess high vanity reach, but zero influence within the brand's target community cluster.
* **Diffusion Decay:** Messages often fail to spread beyond the first degree of followers because the audience does not actively recirculate the content.

**InfluMapper** solves these problems by modeling social platforms as **directed interaction graphs** $G = (V, E)$, evaluating creators through graph topology:
1. **Who is truly authoritative?** $\to$ Power-iteration PageRank over interactions.
2. **Who commands the target sub-community?** $\to$ Label Propagation Algorithm (LPA).
3. **Who is gaming the metrics?** $\to$ 3-color DFS cycle detection combined with reaction-latency anomaly screening.
4. **Will a campaign diffuse organically?** $\to$ Probabilistic cascade model with exponential distance decay and audience fatigue.

### What Makes InfluMapper Novel

Most influencer-analytics tools are dashboards over follower counts and API-reported engagement rates. InfluMapper is different in six concrete ways:

1. **Topology-First Influence, Not Vanity Metrics.** Authority is computed by power-iteration PageRank over *actual interaction edges* — a creator with 50K engaged followers outranks one with 500K dormant or bought ones. No commercial tool publishes its ranking function; ours is fully specified and reproducible.

2. **One Normalized Graph Across Heterogeneous Platforms.** Bluesky (live AT Protocol firehose), Instagram (real interaction CSV), Threads, and Twitter/X all ingest into a single `XInteractionEvent` stream — 16 bytes per edge — so PageRank, LPA, fraud detection, and cascades run over a *unified* network rather than four siloed scores.

3. **Anti-Fraud as a Graph Problem, Not a Blocklist.** Bot detection doesn't rely on platform-provided flags. A 3-color DFS finds closed directed cycles, then a latency-anomaly pass (median reaction < 2s vs. organic ~53s) confirms engagement pods. On the shipped Instagram dataset it autonomously flagged all 11 pod accounts across 3 distinct rings — zero manual labels.

4. **Campaign Reach as a Probabilistic Simulation.** Instead of "estimated impressions," the cascade model computes per-edge transmission probability `S × e^(−λd) × 1/(1+κt)` — interaction strength, exponential distance decay, and audience fatigue — with seeded `mt19937` so every simulation is bit-for-bit reproducible. Multi-seed `/api/cascade/compare` turns ambassador selection into an empirical head-to-head.

5. **Zero-Dependency Embedded Server in Pure C++20.** The backend implements its own HTTP router *and* RFC 6455 WebSocket stack (SHA-1 accept keys, frame masking, ping/pong) on raw Winsock sockets — no Express, no Node, no third-party C++ libraries. The entire analytics + serving stack compiles to one ~700KB executable.

6. **A Zero-Build Frontend Served by the Backend Itself.** The dashboard is a single `index.html` (React 18 + Cytoscape.js via CDN) delivered with `Cache-Control: no-store` — no bundler, no Node, no second process. `start_influmapper.bat` takes a double-click from source to live graph.

---

## 2. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph DataSources["1. Multi-Platform Ingestion"]
        BS[Bluesky AT Protocol Jetstream/XRPC]
        IG[Instagram CSV & Creator Interactions]
        TH[Meta Threads API / Synthetic Ingest]
        TW[Twitter/X 50M-Edge Research CSV]
    end

    subgraph StorageEngine["2. Core Storage Engine (C++20)"]
        RH[Robin Hood Hash Map: Handle -> ID]
        EBUF[Dynamic Edge Ingestion Buffer: alignas(16) XInteractionEvent]
        CSR[Atomic Compressed Sparse Row: row_ptr, col_ind, weight_val]
        SNAP[(graph_snapshot.bin: Binary Snapshot File)]
    end

    subgraph Analytics["3. Graph Analytics Engine (OpenMP)"]
        PR[Parallel PageRank Centrality]
        LPA[Label Propagation Community Detection]
        DFS[3-Color DFS Pod / Bot Ring Detector]
        CAS[Probabilistic Virality Cascade Simulator]
    end

    subgraph Gateway["4. Embedded Gateway Server (Port 18080)"]
        HTTP[Thread-per-Client HTTP REST Server]
        WS[RFC 6455 WebSocket Live Server]
    end

    subgraph Presentation["5. Interactive Frontend Dashboard"]
        CY[Cytoscape.js Force-Directed Graph]
        KOL[KOL Ambassador Scout & Leaderboard]
        FILT[Niche Search Combo & Platform Filter]
        SIM[Real-Time Wave Cascade Animation]
    end

    BS --> RH & EBUF
    IG --> RH & EBUF
    TH --> RH & EBUF
    TW --> RH & EBUF

    EBUF --> CSR
    CSR <--> SNAP
    CSR --> PR & LPA & DFS & CAS

    PR & LPA & DFS & CAS --> HTTP & WS
    HTTP & WS <--> Presentation
```

---

## 3. Core Data Structures

### 3.1 16-Byte Aligned Interaction Event (`XInteractionEvent`)
* **Header:** [`backend/include/types.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/types.hpp)
* **Definition:**
  ```cpp
  struct alignas(16) XInteractionEvent {
      uint32_t source_id;      // 4 bytes: Contiguous node ID of initiator
      uint32_t target_id;      // 4 bytes: Contiguous node ID of recipient
      uint32_t timestamp_sec;  // 4 bytes: Unix epoch seconds
      uint8_t  type;           // 1 byte:  Like (0), Reply (1), Repost (2), Quote (3)
      uint8_t  flags;          // 1 byte:  Bit 0 = Verified, Bit 1 = Bot suspect
      uint16_t latency_ms;     // 2 bytes: Reaction delta (0 - 65535 ms)
  };
  ```
* **Hardware Rationale:** Modern x86-64 CPUs feature 64-byte L1 cache lines. Packing each event into exactly **16 bytes** enables **4 full events per cache line** with zero alignment padding, optimizing streaming SIMD instructions and hardware prefetching during batch ingestion.

---

### 3.2 Robin Hood Hash Map (`RobinHoodHandleMap`)
* **Header:** [`backend/include/robin_hood_map.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/robin_hood_map.hpp)
* **Purpose:** Bi-directional $O(1)$ mapping between alphanumeric social handles (e.g., `cry-ptos.bsky.social`, `ig:creators`, `tw:4614984`) and contiguous integer IDs $0 \le i < |V|$.
* **Design Principles:**
  - **FNV-1a 64-bit Hash:** High-dispersion hash distribution across power-of-two table sizes.
  - **Robin Hood Displacement:** On hash collision, if the incoming entry has traveled a greater distance from its home slot (Probe Sequence Length, $PSL$) than the occupant, the incoming entry evicts and displaces the occupant ("stealing from the rich to give to the poor").
  - **Bounded PSL:** Enforces $PSL \le 4$ at an 80% load factor, preventing long collision cluster chains.
  - **Backward-Shift Deletion:** Closes probe sequence holes without tombstone markers.

---

### 3.3 Atomic Compressed Sparse Row (`CSRGraph`)
* **Header:** [`backend/include/types.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/types.hpp), [`backend/include/graph_storage.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/graph_storage.hpp)
* **Purpose:** Memory-efficient, cache-friendly static representation of directed interaction graphs used by all analytical engines.
* **Component Arrays:**
  - `row_ptr` (size: $|V| + 1$): Prefix sum containing the starting offset into `col_ind` for each node $u$.
  - `col_ind` (size: $|E|$): Contiguous array of neighbor vertex indices $v$.
  - `weight_val` (size: $|E|$): Float values representing interaction frequency/weight.
* **Double-Buffering & Zero-Lock Concurrency:**
  - Ingestion threads append new events to a dynamic buffer (`std::vector<XInteractionEvent>`).
  - Analytics algorithms execute against an immutable `std::shared_ptr<const CSRGraph>` snapshot published atomically (`publish_csr_snapshot()`).
  - Readers traverse slices `col_ind[row_ptr[u] .. row_ptr[u+1]]` with zero mutex contention and optimal memory locality.

---

## 4. Algorithmic Foundations & Mathematical Formulations

### 4.1 Network Authority: OpenMP Parallel PageRank
* **File:** [`backend/include/analytics.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/analytics.hpp) (`compute_pagerank`)
* **Purpose:** Measures recursive endorsement. An incoming interaction from an authoritative node passes significantly more weight than thousands of interactions from low-trust accounts.
* **Mathematical Formula:**
  $$\text{PR}^{(k+1)}(v) = \frac{(1 - d) + d \cdot S_{\text{dangling}}}{|V|} + d \sum_{u \in \text{In}(v)} \frac{\text{PR}^{(k)}(u)}{\text{OutDegree}(u)}$$
  Where:
  - $d = 0.85$ (damping factor).
  - $S_{\text{dangling}} = \sum_{w \in V, \text{OutDegree}(w) = 0} \text{PR}^{(k)}(w)$ reallocates lost probability mass from sink nodes.
* **Convergence & Normalization:**
  - Iterates until $L_1$ delta satisfies:
    $$\sum_{v \in V} |\text{PR}^{(k+1)}(v) - \text{PR}^{(k)}(v)| < \epsilon = 10^{-6} \quad \text{or } k = 25$$
  - Normalized strictly to unit probability: $\sum_{v \in V} \text{PR}(v) = 1.0$.
* **Parallelization:** OpenMP dynamic scheduling (`#pragma omp parallel for schedule(dynamic, 64)`) spreads outward contributions with atomic updates (`#pragma omp atomic`).

---

### 4.2 Community Clustering: Label Propagation Algorithm (LPA)
* **File:** [`backend/include/analytics.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/analytics.hpp) (`compute_lpa`)
* **Purpose:** Detects natural audience clusters and echo chambers in near-linear time without requiring a pre-specified cluster count $k$.
* **Mathematical Formulation:**
  $$C^{(t+1)}(u) = \arg\max_{c} \sum_{v \in \mathcal{N}(u), C^{(t)}(v) = c} W(u, v)$$
  Where $\mathcal{N}(u)$ represents neighbors of node $u$ and $W(u, v)$ is the edge weight.
* **Oscillation Prevention:**
  - Initializes each node with $C^{(0)}(u) = u$.
  - Shuffles the traversal sequence in each iteration using a deterministic Mersenne Twister (`std::mt19937(42)`).
  - Terminates when no node changes label or after 15 passes.

---

### 4.3 Semantic & Keyword Niche Classification
* **Files:** [`backend/include/gateway_server.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/gateway_server.hpp) (`classify_niche`), [`frontend/index.html`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/frontend/index.html)
* **Implementation:**
  - **Backend Taxonomy:** Regex/keyword rules map creator profiles to brand niches:
    - **Motorsport:** `f1`, `racing`, `redbull`, `williams`, `haas`, `mercedes`, `audi`, `motorsport`, `motogp`
    - **Tech & Digital:** `tech`, `code`, `dev`, `pixel`, `cyber`, `web3`, `crypto`, `ai`, `cloud`, `software`, `data`
    - **Fashion & Style:** `fashion`, `style`, `ootd`, `outfit`, `denim`, `velvet`, `closet`, `thrift`
    - **Fitness & Health:** `fitness`, `gym`, `hiit`, `protein`, `lift`, `yoga`, `marathon`, `athlete`, `pilates`
    - **Music, Food & Cooking, Art & Design, Travel, Beauty, etc.**
  - **Frontend Search Combo:** Combines 12 presets with live free-text search and lenient fallback filtering over LPA clusters and handle prefixes.

---

### 4.4 Anti-Fraud: 3-Color DFS Cycle Detection & Latency Bounds
* **File:** [`backend/include/analytics.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/analytics.hpp) (`detect_bot_swarms`)
* **Purpose:** Flags artificial engagement pods (colluding creator groups that instantly recirculate content to manipulate algorithms).
* **Color States:**
  - $\text{WHITE } (0)$: Unvisited vertex.
  - $\text{GRAY } (1)$: Vertex currently on the active recursion call stack.
  - $\text{BLACK } (2)$: Fully explored vertex.
* **Anomaly Conditions:**
  When a back-edge to a $\text{GRAY}$ vertex forms a directed cycle $u_1 \to u_2 \to \dots \to u_k \to u_1$:
  1. **Cycle Length Constraint:** $2 \le k \le 5$.
  2. **Temporal Velocity Anomaly:**
     $$\Delta t_{\text{avg}} = \frac{1}{k} \sum_{i=1}^{k} \text{latency\_ms}(u_i, u_{i+1}) \le 50.0\text{ ms}$$
     *(Organic interactions exhibit a median delay $\approx 53\text{ seconds}$; automated bots respond in milliseconds).*
  3. **High Repost Ratio:**
     $$\frac{\text{Count}(\text{Repost Edges})}{\text{Total Edges in Cycle}} \ge 0.80$$
* **Risk Score Scoring:**
  - Flagged bot pod account: `94.2% (BOT RING DETECTED)`.
  - Organic account: `0.2% - 2.5% (Safe)`.

---

### 4.5 Information Diffusion: Probabilistic Cascade Model with Decay & Fatigue
* **File:** [`backend/include/analytics.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/analytics.hpp) (`trace_virality_cascade_with_decay`)
* **Purpose:** Models how a sponsored recommendation diffuses through network tiers from seed candidate ("Patient Zero") outwards.
* **Transmission Probability:**
  $$P(u \to v) = \text{clamp}\Big( S(u, v) \times D(d) \times M(t), \; 0, \; 1 \Big)$$
  
  1. **Interaction Strength $S(u, v)$:**
     $$S(u, v) = \alpha F(u, v) + \beta R(u, v) + \gamma T(u, v)$$
     - $\alpha = 0.5$ (Interaction frequency weight): $F(u, v) = \frac{\text{count}(u \to v)}{\max_{w} \text{count}(u \to w)}$.
     - $\beta = 0.3$ (Reciprocity weight): $R(u, v) = 1.0$ if reciprocal edge $v \to u$ exists, else $0.0$.
     - $\gamma = 0.2$ (Recency weight): Normalized timestamp $T(u, v) = \frac{t_{\text{last}}(u, v) - t_{\text{min}}}{t_{\text{max}} - t_{\text{min}}}$.
  
  2. **Distance Decay $D(d)$:**
     $$D(d) = \exp(-\lambda \cdot d)$$
     - $d$: Hop distance/tier of target node $v$ (direct seed neighbors are Tier 1).
     - $\lambda$: User-tunable decay coefficient (default: $0.15$).
  
  3. **Audience Message Fatigue $M(t)$:**
     $$M(t) = \frac{1}{1 + \kappa \cdot t}$$
     - $t$: Tier index of parent node $u$.
     - $\kappa$: User-tunable fatigue coefficient (default: $0.05$).

* **Deterministic Simulation:** Uses a seeded generator (`std::mt19937_64(cfg.rng_seed)`) to ensure identical, reproducible results across identical inputs.
* **Multi-Seed Compare:** `/api/cascade/compare` simulates up to 8 candidate seeds simultaneously to produce comparative reach bars and transmission efficiency metrics.

---

### 4.6 Ambassador Fit Score
* **File:** [`backend/include/gateway_server.hpp`](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/backend/include/gateway_server.hpp)
* **Formula:**
  $$\text{Fit} = \text{clamp}\Big( \lfloor \text{PageRank} \times 800 + 70 \rfloor, \; 60, \; 100 \Big)$$
* **Criteria:** An account is marked `recommended = true` if and only if $\text{Fit} \ge 80$ and $\text{is\_bot} = \text{false}$.

---

## 5. Data Sources & Ingestion Pipeline

| Platform | Ingestion Engine | Node Volume | Edge Volume | Characteristics |
| :--- | :--- | :--- | :--- | :--- |
| **Bluesky** | `bluesky_ingestion.hpp` | ~21 live | Variable | AT Protocol Jetstream firehose + XRPC cursor-based profile resolution |
| **Instagram** | `instagram_csv_ingestion.hpp` | 58 | 462 | Real interaction dataset (`instagram_interactions.csv`); pod ring detector |
| **Threads** | `meta_threads_ingestion.hpp` | 60 | Variable | Synthetic creator network (`th:` prefix) fallback until Meta Graph approval |
| **Twitter / X** | `twitter_csv_ingestion.hpp` | 9.98M | 50.39M | High-scale research dataset parsing; rapid bot screening |

### Persistence Layer (`graph_snapshot.bin`)
- Custom binary file format: Magic Header (`0x494D5052`), format version, timestamp, node metadata table, and serialized adjacency lists.
- Avoids repeated network re-fetching on startup; boots in $< 50\text{ms}$.
- Pass `--rebuild` or `--fresh` flags to trigger a fresh multi-platform fetch.

---

## 6. Backend HTTP & WebSocket API Reference

The backend embedded web server listens on port **18080** with open CORS (`*`):

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Serves the single-file React/Cytoscape dashboard (`frontend/index.html`) |
| `GET` | `/api/health` | Service status, node and edge counts, snapshot version, platform load states |
| `GET` | `/api/graph` | Full graph topology (nodes, metadata, PageRank, LPA clusters, CSR edges) |
| `GET` | `/api/candidates` | PageRank-ranked ambassador shortlist with Fit score and fraud flags |
| `GET` | `/api/node?handle={h}` | Metadata, degree, and cluster lookup for a specific creator handle |
| `GET` | `/api/cascade?node_id={id}&lambda={l}&kappa={k}&seed={s}` | Simulates probabilistic campaign cascade for a single seed node |
| `GET` | `/api/cascade/compare?node_ids={id1,id2,...}&lambda={l}&kappa={k}` | Multi-seed comparative cascade simulation (up to 8 candidates) |
| `GET` | `/api/export/candidates?format=csv\|json` | Downloadable CSV/JSON export of evaluated ambassador shortlist |
| `GET` | `/api/refresh` | Re-runs live data ingestion, recalculates PageRank & LPA, broadcasts update |
| `GET` | `/ws` | RFC 6455 WebSocket stream broadcasting health heartbeats and `graph_updated` |

---

## 7. Frontend Interactive Dashboard

* **Technology:** Standalone zero-build single-file React 18, Babel standalone, and Cytoscape.js served directly from [frontend/index.html](file:///c:/Users/Sanchit/Desktop/Work/College/DSA/CP/frontend/index.html).
* **Key Components:**
  1. **Cytoscape Force-Directed Topology (`#cy`):** Renders authoritative hubs, organic audiences, and dashed red bot loops.
  2. **KOL Leaderboard & Scout:** Top-10 ranked ambassador table with multi-column sorting by PageRank, Cascade Reach, Cascade Depth, or Bot Risk; instant handle search with live match counter.
  3. **Niche Search Combo:** Autocomplete dropdown fed by backend-classified niches (Fashion, Fitness, Tech, Motorsport, etc.) + free-text search; filters both candidate list *and* the graph canvas.
  4. **Dynamic Canvas HUD:**
     - **Simulate:** Triggers multi-tier wave animations radiating from the selected creator.
     - **Compare Ambassadors:** Runs multi-seed cascade comparisons with comparative reach bars.
     - **Decay (λ) & Fatigue (κ) Sliders:** Interactively tune diffusion equations in real time.
  5. **Filters:** Platform selector (Bluesky / Instagram / Threads / X), niche selector, bot-ring toggle, and free-text search — all composable; CSV export mirrors the visible top-10 view.

---

## 8. Build, Installation & Execution Guide

### Prerequisites
* **C++ Compiler:** GCC/G++ supporting C++20 with OpenMP (MSYS2 UCRT64 recommended on Windows).
* **System Libraries:** Windows Sockets (`-lws2_32`), WinHTTP (`-lwinhttp`).

### 1. Build the C++20 Engine
```powershell
cd backend
g++ -std=c++20 -fopenmp -O2 -Iinclude src/main.cpp -o influence_mapper.exe -lws2_32 -lwinhttp -fno-use-linker-plugin
```

### 2. Run the Engine
```powershell
.\influence_mapper.exe
```
*(Use `.\influence_mapper.exe --rebuild` to invalidate `graph_snapshot.bin` and re-fetch all platforms).*

### 3. Open the Dashboard
Navigate to:
```
http://localhost:18080
```
*(Alternatively, open `frontend/index.html` directly in any web browser).*

---

## 9. Verification & Test Suite

The engine includes 15 automated test cases validating algorithmic correctness:
```powershell
cd backend
g++ -std=c++20 -fopenmp -O2 -Iinclude tests/test_suite.cpp -o test_suite.exe -lws2_32 -lwinhttp -fno-use-linker-plugin
.\test_suite.exe
```

| Test Case | Module | Description | Validation Target |
| :--- | :--- | :--- | :--- |
| **TC-01** | `RobinHoodMap` | Insert and retrieval | Handle lookup $O(1)$, PSL $\le 4$ |
| **TC-02** | `GraphStorage` | CSR snapshot creation | Edge offsets and degree counts |
| **TC-03** | `AnalyticsEngine` | OpenMP PageRank | Convergence and sum of ranks $= 1.0$ |
| **TC-04** | `AnalyticsEngine` | Label Propagation | Partition stability across clusters |
| **TC-05** | `AnalyticsEngine` | 3-Color DFS | Detection of cycles ($k \le 5$) |
| **TC-06** | `AnalyticsEngine` | Latency anomaly | Filtering cycles where $\Delta t \le 50\text{ms}$ |
| **TC-07** | `AnalyticsEngine` | Probabilistic Cascade | Seed infection and tier reach monotonic decrease |
| **TC-08** | `AnalyticsEngine` | Decay Parameterization | Higher $\lambda$ strictly decreases total reach |
| **TC-09** | `Snapshot` | Binary serialization | Round-trip snapshot byte preservation |
| **TC-10** | `Snapshot` | Mid-file corruption | Graceful fallback to fresh ingestion |
| **TC-11** | `GatewayServer` | Candidate Export | Valid RFC 4180 CSV export |
| **TC-12** | `GatewayServer` | Multi-seed Cascade | Comparison output consistency |
| **TC-13** | `InstagramCSV` | Real CSV ingestion | Correct node and pod count identification |
| **TC-14** | `TwitterCSV` | CSV stream handling | Zero-leak edge processing |
| **TC-15** | `WebSocket` | RFC 6455 framing | Client handshake and event broadcast |

---

## 10. Complexity & Hardware Efficiency Summary

| Component / Task | Algorithm / Data Structure | Time Complexity | Space Complexity | Hardware Optimization |
| :--- | :--- | :--- | :--- | :--- |
| **Handle Resolution** | Robin Hood Hash Table | $O(1)$ avg | $O(|V|)$ | PSL $\le 4$, flat memory |
| **Ingestion Event** | Packed Struct | $O(1)$ | $16\text{ bytes}$ | 4 events per 64-byte L1 cache line |
| **Graph Query** | Compressed Sparse Row (CSR) | $O(1)$ per degree | $O(|V| + |E|)$ | Atomic pointer swap, zero locks |
| **Authority Centrality** | Power Iteration PageRank | $O(k \cdot (|V| + |E|))$ | $O(|V|)$ | OpenMP SIMD multi-threading |
| **Community Discovery** | Label Propagation (LPA) | $O(I \cdot (|V| + |E|))$ | $O(|V|)$ | Random iteration shuffle |
| **Bot Pod Detection** | 3-Color DFS + Temporal Filter | $O(|V| + |E|)$ | $O(|V|)$ | Depth-bounded recursion ($k \le 5$) |
| **Campaign Diffusion** | Probabilistic Decay Cascade | $O(|V| + |E|)$ | $O(|V|)$ | Deterministic seeded RNG |

---

*InfluMapper — Developed for Data Structures & Algorithms (DSA) / Competitive Programming.*

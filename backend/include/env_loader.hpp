#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <iostream>

namespace influmapper {

struct ApiConfig {
    std::string meta_app_id;
    std::string meta_app_secret;
    std::string meta_access_token;

    std::string threads_app_id;
    std::string threads_app_secret;
    std::string threads_access_token;
    std::string threads_redirect_uri;

    std::string bluesky_handle;
    std::string bluesky_app_password;
    std::string bluesky_pds_host;
    std::string bluesky_jetstream_url;

    // Local Twitter/X CSV dataset path (optional, has built-in default)
    std::string twitter_csv_path;

    // Instagram/Threads synthetic graphs (optional)
    std::string instagram_handle;
    std::string instagram_csv_path;   // real CSV dataset (takes precedence over synthetic)
    std::string threads_handle;
    int instagram_mock_nodes = 60;
    int threads_mock_nodes = 60;

    static ApiConfig load_from_env_file(const std::string& path = ".env") {
        ApiConfig config;
        std::ifstream file(path);
        if (!file.is_open()) {
            std::cout << "[Config] Warning: Could not open " << path << " directly, trying parent/cwd..." << std::endl;
        }

        std::string line;
        while (std::getline(file, line)) {
            // Trim leading spaces
            size_t start = line.find_first_not_of(" \t\r\n");
            if (start == std::string::npos || line[start] == '#') continue;

            size_t eq = line.find('=', start);
            if (eq == std::string::npos) continue;

            std::string key = line.substr(start, eq - start);
            std::string val = line.substr(eq + 1);

            // Trim trailing spaces and quotes
            while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) key.pop_back();
            size_t val_start = val.find_first_not_of(" \t\r\n\"");
            size_t val_end = val.find_last_not_of(" \t\r\n\"");
            if (val_start != std::string::npos && val_end != std::string::npos) {
                val = val.substr(val_start, val_end - val_start + 1);
            } else {
                val = "";
            }

            if (key == "META_APP_ID") config.meta_app_id = val;
            else if (key == "META_APP_SECRET") config.meta_app_secret = val;
            else if (key == "META_ACCESS_TOKEN") config.meta_access_token = val;
            else if (key == "THREADS_APP_ID") config.threads_app_id = val;
            else if (key == "THREADS_APP_SECRET") config.threads_app_secret = val;
            else if (key == "THREADS_ACCESS_TOKEN") config.threads_access_token = val;
            else if (key == "THREADS_REDIRECT_URI") config.threads_redirect_uri = val;
            else if (key == "BLUESKY_HANDLE") config.bluesky_handle = val;
            else if (key == "BLUESKY_APP_PASSWORD") config.bluesky_app_password = val;
            else if (key == "BLUESKY_PDS_HOST") config.bluesky_pds_host = val;
            else if (key == "BLUESKY_JETSTREAM_URL") config.bluesky_jetstream_url = val;
            else if (key == "TWITTER_CSV_PATH") config.twitter_csv_path = val;
            else if (key == "INSTAGRAM_HANDLE") config.instagram_handle = val;
            else if (key == "INSTAGRAM_CSV_PATH") config.instagram_csv_path = val;
            else if (key == "THREADS_HANDLE") config.threads_handle = val;
            else if (key == "INSTAGRAM_MOCK_NODES") {
                try { config.instagram_mock_nodes = std::stoi(val); } catch (...) {}
            }
            else if (key == "THREADS_MOCK_NODES") {
                try { config.threads_mock_nodes = std::stoi(val); } catch (...) {}
            }
        }

        return config;
    }
};

} // namespace influmapper

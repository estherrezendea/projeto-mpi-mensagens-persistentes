#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
class Logger {
public:
    static void setPaused(bool paused) {
        std::lock_guard<std::mutex> lock(outputMutex_);
        paused_ = paused;
        if (!paused_) {
            flushUnlocked();
        }
    }

    static void info(
        int rank,
        const std::string& component,
        const std::string& message
    ) {
        std::lock_guard<std::mutex> lock(outputMutex_);

        std::ostringstream formattedMessage;
        formattedMessage
            << '[' << currentTime() << ']'
            << " [Processo " << rank << ']'
            << " [" << component << "] "
            << message;

        if (paused_) {
            pendingLogs_.push_back(formattedMessage.str());
        } else {
            std::cout << formattedMessage.str() << std::endl;
        }
    }

private:
    inline static std::mutex outputMutex_;
    inline static bool paused_ = false;
    inline static std::vector<std::string> pendingLogs_;

    static void flushUnlocked() {
        if (!pendingLogs_.empty()) {
            std::cout << "\n--- Logs acumulados ---\n";
            for (const auto& log : pendingLogs_) {
                std::cout << log << std::endl;
            }
            std::cout << "-----------------------\n" << std::endl;
            pendingLogs_.clear();
        }
    }

    static std::string currentTime() {
        const auto now = std::chrono::system_clock::now();
        const std::time_t time = std::chrono::system_clock::to_time_t(now);

        std::tm localTime{};

#if defined(_WIN32)
        localtime_s(&localTime, &time);
#else
        localtime_r(&time, &localTime);
#endif

        std::ostringstream output;
        output << std::put_time(&localTime, "%H:%M:%S");

        return output.str();
    }
};
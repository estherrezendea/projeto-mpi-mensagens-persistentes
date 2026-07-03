#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

class Logger {
public:
    static void info(
        int rank,
        const std::string& component,
        const std::string& message
    ) {
        std::lock_guard<std::mutex> lock(outputMutex_);

        std::cout
            << '[' << currentTime() << ']'
            << " [Processo " << rank << ']'
            << " [" << component << "] "
            << message
            << std::endl;
    }

private:
    inline static std::mutex outputMutex_;

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
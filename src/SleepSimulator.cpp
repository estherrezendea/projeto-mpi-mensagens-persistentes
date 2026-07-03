#include "SleepSimulator.hpp"

#include "Logger.hpp"

#include <chrono>
#include <thread>

void sleepRandomly(
    const int rank,
    const std::string& component,
    const std::string& reason,
    std::mt19937& randomGenerator
) {
    std::uniform_int_distribution<int> secondsDistribution(1, 3);

    const int seconds = secondsDistribution(randomGenerator);

    Logger::info(
        rank,
        component,
        "Entrando em espera aleatoria por " +
        std::to_string(seconds) +
        " segundo(s). " +
        reason
    );

    std::this_thread::sleep_for(
        std::chrono::seconds(seconds)
    );

    Logger::info(
        rank,
        component,
        "Acordou e retomou a execucao."
    );
}

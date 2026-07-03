#pragma once

#include <random>
#include <string>

void sleepRandomly(
    int rank,
    const std::string& component,
    const std::string& reason,
    std::mt19937& randomGenerator
);

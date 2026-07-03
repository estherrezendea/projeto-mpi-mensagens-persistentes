#pragma once

#include "PersistentQueue.hpp"

#include <random>

void runOrigin(
    int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
);

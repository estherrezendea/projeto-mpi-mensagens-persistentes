#pragma once

#include "PersistentQueue.hpp"

#include <random>

void runDestination(
    int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
);

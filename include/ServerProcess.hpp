#pragma once

#include "PersistentQueue.hpp"

#include <random>

void runServer(
    int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
);

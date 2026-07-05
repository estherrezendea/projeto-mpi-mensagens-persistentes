#pragma once

#include <atomic>

class SimulationConsole {
public:
    explicit SimulationConsole(std::atomic_bool& stopRequested);

    void run();

private:
    std::atomic_bool& stopRequested_;
    int nextMessageId_{1};

    void printMenu() const;

    void sendMessage();

    void sleepProcess(int rank);

    void showMessageTimeline();

    int readInt(const char* prompt) const;
};

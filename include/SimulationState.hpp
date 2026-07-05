#pragma once

#include "Message.hpp"

#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

enum class SimulationCommandType {
    SendMessage,
    Sleep,
    Stop
};

struct SimulationCommand {
    SimulationCommandType type{SimulationCommandType::Stop};
    int messageId{};
    int seconds{};
    std::string content;
};

class SimulationState {
public:
    static void prepareGlobalState();

    static void prepareNodeState(int rank);

    static void appendCommand(
        int rank,
        const SimulationCommand& command
    );

    [[nodiscard]] static std::vector<SimulationCommand> consumeCommands(
        int rank
    );

    static void markAwake(int rank);

    static void markSleeping(
        int rank,
        int remainingSeconds
    );

    static void markStopped(int rank);

    static void appendProcessedMessage(
        int rank,
        const Message& message
    );

    static void appendHistory(
        int messageId,
        const std::string& nodeName,
        const std::string& event,
        const std::string& content = ""
    );

    static void printQueuesAndStatus(std::ostream& output);

    static void printHistory(std::ostream& output);

    static void printMessageTimeline(
        std::ostream& output,
        int messageId
    );
};

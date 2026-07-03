#pragma once

#include <string>

enum class MessageType {
    Normal,
    Ack,
    Finish
};

struct Message {
    int id{};
    int senderRank{};
    int receiverRank{};
    MessageType type{MessageType::Normal};
    std::string content;
};
#pragma once

#include "Message.hpp"
#include "MpiMessageTransport.hpp"
#include "PersistentQueue.hpp"

#include <exception>
#include <list>
#include <string>
#include <vector>

Message createMessage(
    int id,
    int senderRank,
    int receiverRank,
    MessageType type,
    const std::string& content
);

Message createAck(
    int messageId,
    int senderRank,
    int receiverRank,
    const std::string& content
);

[[nodiscard]] bool isFinishMessage(const Message& message);

void validateAck(
    const Message& ack,
    int expectedMessageId,
    int expectedSender,
    const std::string& receiverName
);

Message removeMessageAfterAck(
    PersistentQueue& queue,
    const std::string& nodeName
);

[[nodiscard]] std::vector<Message> createDemoMessages();

void checkThreadException(const std::exception_ptr& exception);

void networkPause();

void registerPendingSend(
    std::list<MpiMessageTransport::PendingSend>& pendingSends,
    const Message& message,
    int destinationRank
);

void testPendingSends(
    std::list<MpiMessageTransport::PendingSend>& pendingSends
);

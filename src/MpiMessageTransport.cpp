#include "MpiMessageTransport.hpp"

#include <mpi.h>

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>

namespace {
    constexpr int MESSAGE_TAG = 100;
}

void MpiMessageTransport::startSend(
    PendingSend& pendingSend,
    const Message& message,
    const int destinationRank
) {
    if (
        message.content.size() >
        static_cast<std::size_t>(MAX_CONTENT_SIZE)
    ) {
        throw std::runtime_error(
            "Mensagem excede o tamanho maximo permitido de " +
            std::to_string(MAX_CONTENT_SIZE) +
            " caracteres."
        );
    }

    pendingSend.packet = WirePacket{};

    pendingSend.packet.id = message.id;
    pendingSend.packet.senderRank = message.senderRank;
    pendingSend.packet.receiverRank = message.receiverRank;
    pendingSend.packet.type = static_cast<int>(message.type);
    pendingSend.packet.contentSize =
        static_cast<int>(message.content.size());

    std::copy(
        message.content.begin(),
        message.content.end(),
        pendingSend.packet.content.begin()
    );

    MPI_Isend(
        &pendingSend.packet,
        static_cast<int>(sizeof(WirePacket)),
        MPI_BYTE,
        destinationRank,
        MESSAGE_TAG,
        MPI_COMM_WORLD,
        &pendingSend.request
    );
}

void MpiMessageTransport::startReceive(
    PendingReceive& pendingReceive,
    const int sourceRank
) {
    pendingReceive.packet = WirePacket{};

    MPI_Irecv(
        &pendingReceive.packet,
        static_cast<int>(sizeof(WirePacket)),
        MPI_BYTE,
        sourceRank,
        MESSAGE_TAG,
        MPI_COMM_WORLD,
        &pendingReceive.request
    );
}

void MpiMessageTransport::wait(PendingSend& pendingSend) {
    MPI_Wait(
        &pendingSend.request,
        MPI_STATUS_IGNORE
    );
}

Message MpiMessageTransport::wait(
    PendingReceive& pendingReceive
) {
    MPI_Wait(
        &pendingReceive.request,
        MPI_STATUS_IGNORE
    );

    return packetToMessage(pendingReceive.packet);
}

bool MpiMessageTransport::test(PendingSend& pendingSend) {
    int completed = 0;

    MPI_Test(
        &pendingSend.request,
        &completed,
        MPI_STATUS_IGNORE
    );

    return completed != 0;
}

std::optional<Message> MpiMessageTransport::test(
    PendingReceive& pendingReceive
) {
    int completed = 0;

    MPI_Test(
        &pendingReceive.request,
        &completed,
        MPI_STATUS_IGNORE
    );

    if (completed == 0) {
        return std::nullopt;
    }

    return packetToMessage(pendingReceive.packet);
}

bool MpiMessageTransport::hasMessage(const int sourceRank) {
    int hasPendingMessage = 0;

    MPI_Iprobe(
        sourceRank,
        MESSAGE_TAG,
        MPI_COMM_WORLD,
        &hasPendingMessage,
        MPI_STATUS_IGNORE
    );

    return hasPendingMessage != 0;
}

Message MpiMessageTransport::receiveAvailable(const int sourceRank) {
    PendingReceive pendingReceive;

    startReceive(pendingReceive, sourceRank);

    return wait(pendingReceive);
}

Message MpiMessageTransport::packetToMessage(const WirePacket& packet) {
    if (
        packet.contentSize < 0 ||
        packet.contentSize > MAX_CONTENT_SIZE
    ) {
        throw std::runtime_error(
            "Tamanho de conteudo invalido recebido via MPI."
        );
    }

    Message message;
    message.id = packet.id;
    message.senderRank = packet.senderRank;
    message.receiverRank = packet.receiverRank;
    message.type = static_cast<MessageType>(packet.type);
    message.content = std::string(
        packet.content.data(),
        static_cast<std::size_t>(packet.contentSize)
    );

    return message;
}
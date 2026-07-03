#pragma once

#include "Message.hpp"

#include <mpi.h>

#include <array>
#include <string>

class MpiMessageTransport {
public:
    static constexpr int MAX_CONTENT_SIZE = 512;

    struct WirePacket {
        int id{};
        int senderRank{};
        int receiverRank{};
        int type{};
        int contentSize{};
        std::array<char, MAX_CONTENT_SIZE> content{};
    };

    struct PendingSend {
        WirePacket packet{};
        MPI_Request request{MPI_REQUEST_NULL};
    };

    struct PendingReceive {
        WirePacket packet{};
        MPI_Request request{MPI_REQUEST_NULL};
    };

    static void startSend(
        PendingSend& pendingSend,
        const Message& message,
        int destinationRank
    );

    static void startReceive(
        PendingReceive& pendingReceive,
        int sourceRank
    );

    static void wait(PendingSend& pendingSend);

    [[nodiscard]] static Message wait(
        PendingReceive& pendingReceive
    );
};
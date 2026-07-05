#include "ServerProcess.hpp"

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "SimulationRoles.hpp"
#include "SimulationState.hpp"
#include "SimulationSupport.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <list>
#include <optional>
#include <string>
#include <thread>

namespace {
    void runServerApplication(
        const int rank,
        const std::atomic_bool& networkFinished,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!networkFinished.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type != SimulationCommandType::Sleep) {
                        continue;
                    }

                    for (int remaining = command.seconds;
                         remaining > 0 && !networkFinished.load();
                         --remaining) {
                        SimulationState::markSleeping(rank, remaining);
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Aplicacao dormindo."
                        );
                        std::this_thread::sleep_for(
                            std::chrono::seconds(1)
                        );
                    }

                    SimulationState::markAwake(rank);
                    Logger::info(rank, "SERVIDOR", "Aplicacao acordou.");
                }

                if (commands.empty()) {
                    SimulationState::markAwake(rank);
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(100)
                    );
                }
            }

            SimulationState::markStopped(rank);
        } catch (...) {
            exception = std::current_exception();
        }
    }

    void runServerNetwork(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& networkFinished,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::list<MpiMessageTransport::PendingSend> pendingAckSends;
            std::optional<Message> forwardingMessage;
            MpiMessageTransport::PendingSend pendingForwardSend;
            MpiMessageTransport::PendingReceive pendingAckReceive;
            bool forwardSendActive = false;
            bool destinationAckActive = false;

            Logger::info(
                rank,
                "SERVIDOR-REDE",
                "Loop MPI iniciado. Recebimento, ACK e encaminhamento rodam fora da thread de aplicacao."
            );

            while (!networkFinished.load()) {
                testPendingSends(pendingAckSends);

                if (MpiMessageTransport::hasMessage(ORIGEM)) {
                    const Message receivedMessage =
                        MpiMessageTransport::receiveAvailable(ORIGEM);

                    queue.enqueue(receivedMessage);

                    if (isFinishMessage(receivedMessage)) {
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Mensagem de finalizacao recebida e persistida."
                        );
                    } else {
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Servidor",
                            "RECEIVED"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Servidor",
                            "SERVER_PERSISTED"
                        );

                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Mensagem recebida."
                        );
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Mensagem gravada na fila persistente."
                        );
                    }

                    registerPendingSend(
                        pendingAckSends,
                        createAck(
                            receivedMessage.id,
                            SERVIDOR,
                            ORIGEM,
                            "Mensagem persistida pelo Servidor."
                        ),
                        ORIGEM
                    );

                    Logger::info(
                        rank,
                        "SERVIDOR-REDE",
                        "ACK assincrono registrado para a Origem."
                    );
                }

                if (!forwardingMessage.has_value()) {
                    const auto pendingMessage = queue.peek();

                    if (pendingMessage.has_value()) {
                        forwardingMessage = pendingMessage.value();

                        if (isFinishMessage(forwardingMessage.value())) {
                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Mensagem de finalizacao lida da fila persistente antes do encaminhamento."
                            );
                        } else {
                            SimulationState::appendHistory(
                                forwardingMessage->id,
                                "Servidor",
                                "READ_FROM_QUEUE"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Mensagem recuperada da fila persistente."
                            );
                        }

                        forwardingMessage->senderRank = SERVIDOR;
                        forwardingMessage->receiverRank = DESTINO;

                        MpiMessageTransport::startReceive(
                            pendingAckReceive,
                            DESTINO
                        );
                        destinationAckActive = true;

                        MpiMessageTransport::startSend(
                            pendingForwardSend,
                            forwardingMessage.value(),
                            DESTINO
                        );
                        forwardSendActive = true;

                        if (!isFinishMessage(forwardingMessage.value())) {
                            SimulationState::appendHistory(
                                forwardingMessage->id,
                                "Servidor",
                                "FORWARDED"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Mensagem encaminhada ao destino."
                            );
                        }
                    }
                }

                if (
                    forwardSendActive &&
                    MpiMessageTransport::test(pendingForwardSend)
                ) {
                    forwardSendActive = false;
                }

                if (destinationAckActive) {
                    const auto ack = MpiMessageTransport::test(
                        pendingAckReceive
                    );

                    if (ack.has_value()) {
                        destinationAckActive = false;

                        validateAck(
                            ack.value(),
                            forwardingMessage->id,
                            DESTINO,
                            "o Servidor"
                        );

                        const Message removedMessage =
                            removeMessageAfterAck(queue, "o Servidor");

                        if (isFinishMessage(removedMessage)) {
                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "ACK da finalizacao recebido. Servidor encerrou."
                            );

                            networkFinished.store(true);
                        } else {
                            SimulationState::appendHistory(
                                removedMessage.id,
                                "Servidor",
                                "ACK_FROM_DESTINATION"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "ACK recebido do destino. Mensagem removida da fila."
                            );
                        }

                        forwardingMessage.reset();
                    }
                }

                networkPause();
            }

            while (!pendingAckSends.empty()) {
                testPendingSends(pendingAckSends);
                networkPause();
            }
        } catch (...) {
            exception = std::current_exception();
            networkFinished.store(true);
        }
    }
}

void runServer(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool networkFinished{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;

    std::thread applicationThread(
        runServerApplication,
        rank,
        std::cref(networkFinished),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runServerNetwork,
        rank,
        std::ref(queue),
        std::ref(networkFinished),
        std::ref(networkException)
    );

    applicationThread.join();
    networkThread.join();

    checkThreadException(applicationException);
    checkThreadException(networkException);
}

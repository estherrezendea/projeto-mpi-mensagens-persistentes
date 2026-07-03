#include "ServerProcess.hpp"

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "SimulationRoles.hpp"
#include "SimulationSupport.hpp"
#include "SleepSimulator.hpp"

#include <atomic>
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
        try {
            while (!networkFinished.load()) {
                sleepRandomly(
                    rank,
                    "SERVIDOR-APP",
                    "A aplicacao do servidor esta indisponivel; a rede continua recebendo e persistindo.",
                    randomGenerator
                );
            }
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
                            "SERVIDOR-REDE",
                            "Mensagem de finalizacao recebida e persistida."
                        );
                    } else {
                        Logger::info(
                            rank,
                            "SERVIDOR-REDE",
                            "Mensagem #" + std::to_string(receivedMessage.id) +
                            " recebida e persistida: \"" +
                            receivedMessage.content + "\""
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
                                "SERVIDOR-REDE",
                                "Mensagem de finalizacao lida da fila persistente antes do encaminhamento."
                            );
                        } else {
                            Logger::info(
                                rank,
                                "SERVIDOR-REDE",
                                "Mensagem #" +
                                std::to_string(forwardingMessage->id) +
                                " lida da fila persistente antes do encaminhamento."
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

                        Logger::info(
                            rank,
                            "SERVIDOR-REDE",
                            "Encaminhamento assincrono da mensagem #" +
                            std::to_string(forwardingMessage->id) +
                            " para o Destino iniciado."
                        );
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
                                "SERVIDOR-REDE",
                                "ACK da finalizacao recebido. Servidor encerrou."
                            );

                            networkFinished.store(true);
                        } else {
                            Logger::info(
                                rank,
                                "SERVIDOR-REDE",
                                "ACK do Destino recebido para a mensagem #" +
                                std::to_string(removedMessage.id) +
                                ". Mensagem removida da fila."
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

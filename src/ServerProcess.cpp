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
        const std::atomic_bool& isNetworkShutdownRequested,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!isNetworkShutdownRequested.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type != SimulationCommandType::Sleep) {
                        continue;
                    }

                    for (int remaining = command.seconds;
                         remaining > 0 && !isNetworkShutdownRequested.load();
                         --remaining) {
                        SimulationState::markSleeping(rank, remaining);
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Processo de aplicacao suspenso (idle mode)."
                        );
                        std::this_thread::sleep_for(
                            std::chrono::seconds(1)
                        );
                    }

                    SimulationState::markAwake(rank);
                    Logger::info(rank, "SERVIDOR", "Processo de aplicacao retomado.");
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
        std::atomic_bool& isNetworkShutdownRequested,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::list<MpiMessageTransport::PendingSend> pendingAcksToDispatch;
            std::optional<Message> messagePendingForward;
            MpiMessageTransport::PendingSend pendingForwardSend;
            MpiMessageTransport::PendingReceive pendingAcknowledgment;
            bool isForwardTransmissionActive = false;
            bool isDestinationAcknowledgmentActive = false;

            Logger::info(
                rank,
                "SERVIDOR-REDE",
                "Ciclo de rede MPI assincrono (recv/ack/forward) inicializado (thread separada)."
            );

            while (!isNetworkShutdownRequested.load()) {
                testPendingSends(pendingAcksToDispatch);

                if (MpiMessageTransport::hasMessage(ORIGEM)) {
                    const Message receivedMessage =
                        MpiMessageTransport::receiveAvailable(ORIGEM);

                    queue.enqueue(receivedMessage);

                    if (isFinishMessage(receivedMessage)) {
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Sinal de finalizacao (Tombstone) recebido e gravado em disco."
                        );
                    } else {
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Servidor",
                            "RECEBIDA_PELA_REDE"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Servidor",
                            "GRAVADA_FILA_SERVIDOR"
                        );

                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Mensagem recebida via protocolo MPI."
                        );
                        Logger::info(
                            rank,
                            "SERVIDOR",
                            "Mensagem inserida na fila de processamento persistente em disco."
                        );
                    }

                    registerPendingSend(
                        pendingAcksToDispatch,
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
                        "Confirmacao (ACK) assincrona enfileirada para a Origem."
                    );
                }

                if (!messagePendingForward.has_value()) {
                    const auto pendingMessage = queue.peek();

                    if (pendingMessage.has_value()) {
                        messagePendingForward = pendingMessage.value();

                        if (isFinishMessage(messagePendingForward.value())) {
                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Sinal de finalizacao processado pela fila de encaminhamento."
                            );
                        } else {
                            SimulationState::appendHistory(
                                messagePendingForward->id,
                                "Servidor",
                                "LIDA_FILA_PERSISTENTE"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Mensagem recuperada da fila em disco para roteamento."
                            );
                        }

                        messagePendingForward->senderRank = SERVIDOR;
                        messagePendingForward->receiverRank = DESTINO;

                        MpiMessageTransport::startReceive(
                            pendingAcknowledgment,
                            DESTINO
                        );
                        isDestinationAcknowledgmentActive = true;

                        MpiMessageTransport::startSend(
                            pendingForwardSend,
                            messagePendingForward.value(),
                            DESTINO
                        );
                        isForwardTransmissionActive = true;

                        if (!isFinishMessage(messagePendingForward.value())) {
                            SimulationState::appendHistory(
                                messagePendingForward->id,
                                "Servidor",
                                "ENCAMINHADA_AO_DESTINO_MPI"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Mensagem despachada ao no Destino via comunicacao MPI."
                            );
                        }
                    }
                }

                if (
                    isForwardTransmissionActive &&
                    MpiMessageTransport::test(pendingForwardSend)
                ) {
                    isForwardTransmissionActive = false;
                }

                if (isDestinationAcknowledgmentActive) {
                    const auto ack = MpiMessageTransport::test(
                        pendingAcknowledgment
                    );

                    if (ack.has_value()) {
                        isDestinationAcknowledgmentActive = false;

                        validateAck(
                            ack.value(),
                            messagePendingForward->id,
                            DESTINO,
                            "o Servidor"
                        );

                        const Message removedMessage =
                            removeMessageAfterAck(queue, "o Servidor");

                        if (isFinishMessage(removedMessage)) {
                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Confirmacao (ACK) de finalizacao recebida. Ciclo do Servidor encerrado."
                            );

                            isNetworkShutdownRequested.store(true);
                        } else {
                            SimulationState::appendHistory(
                                removedMessage.id,
                                "Servidor",
                                "CONFIRMACAO_RECEBIDA_DESTINO"
                            );

                            Logger::info(
                                rank,
                                "SERVIDOR",
                                "Confirmacao (ACK) recebida do Destino. Mensagem limpa da fila local."
                            );
                        }

                        messagePendingForward.reset();
                    }
                }

                networkPause();
            }

            while (!pendingAcksToDispatch.empty()) {
                testPendingSends(pendingAcksToDispatch);
                networkPause();
            }
        } catch (...) {
            exception = std::current_exception();
            isNetworkShutdownRequested.store(true);
        }
    }
}

void runServer(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool isNetworkShutdownRequested{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;

    std::thread applicationThread(
        runServerApplication,
        rank,
        std::cref(isNetworkShutdownRequested),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runServerNetwork,
        rank,
        std::ref(queue),
        std::ref(isNetworkShutdownRequested),
        std::ref(networkException)
    );

    applicationThread.join();
    networkThread.join();

    checkThreadException(applicationException);
    checkThreadException(networkException);
}

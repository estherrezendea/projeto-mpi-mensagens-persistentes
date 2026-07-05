#include "DestinationProcess.hpp"

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
#include <string>
#include <thread>

namespace {
    void runDestinationApplication(
        const int rank,
        PersistentQueue& queue,
        const std::atomic_bool& hasReceivedFinishSignal,
        std::atomic_bool& isApplicationShutdownRequested,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!isApplicationShutdownRequested.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type != SimulationCommandType::Sleep) {
                        continue;
                    }

                    for (int remaining = command.seconds;
                         remaining > 0 && !isApplicationShutdownRequested.load();
                         --remaining) {
                        SimulationState::markSleeping(rank, remaining);
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Processo de aplicacao suspenso (idle mode)."
                        );
                        std::this_thread::sleep_for(
                            std::chrono::seconds(1)
                        );
                    }

                    const auto pendingAfterSleep = queue.messages();

                    for (const Message& message : pendingAfterSleep) {
                        if (!isFinishMessage(message)) {
                            SimulationState::appendHistory(
                                message.id,
                                "Destino",
                                "APLICACAO_SUSPENSA"
                            );
                            SimulationState::appendHistory(
                                message.id,
                                "Destino",
                                "APLICACAO_RETOMADA"
                            );
                        }
                    }

                    SimulationState::markAwake(rank);
                    Logger::info(rank, "DESTINO", "Processo de aplicacao retomado.");
                }

                const auto pendingMessage = queue.peek();

                if (!pendingMessage.has_value()) {
                    if (hasReceivedFinishSignal.load()) {
                        isApplicationShutdownRequested.store(true);
                        SimulationState::markStopped(rank);
                    }

                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(100)
                    );
                    continue;
                }

                const Message removedMessage =
                    removeMessageAfterAck(queue, "o Destino");

                if (isFinishMessage(removedMessage)) {
                    Logger::info(
                        rank,
                        "DESTINO",
                        "Sinal de finalizacao processado. Encerrando o modulo de aplicacao."
                    );

                    isApplicationShutdownRequested.store(true);
                    SimulationState::markStopped(rank);
                } else {
                    SimulationState::appendHistory(
                        removedMessage.id,
                        "Destino",
                        "MENSAGEM_PROCESSADA_COM_SUCESSO"
                    );
                    SimulationState::appendProcessedMessage(
                        rank,
                        removedMessage
                    );

                    Logger::info(rank, "DESTINO", "Processando mensagem.");
                    Logger::info(
                        rank,
                        "DESTINO",
                        "Ciclo de vida da mensagem finalizado com sucesso."
                    );
                }
            }
        } catch (...) {
            exception = std::current_exception();
            isApplicationShutdownRequested.store(true);
        }
    }

    void runDestinationNetwork(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& hasReceivedFinishSignal,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::list<MpiMessageTransport::PendingSend> pendingAcksToDispatch;

            Logger::info(
                rank,
                "DESTINO-REDE",
                "Ciclo de rede MPI assincrono inicializado no Destino."
            );

            while (!hasReceivedFinishSignal.load()) {
                testPendingSends(pendingAcksToDispatch);

                if (MpiMessageTransport::hasMessage(SERVIDOR)) {
                    const Message receivedMessage =
                        MpiMessageTransport::receiveAvailable(SERVIDOR);

                    queue.enqueue(receivedMessage);

                    if (isFinishMessage(receivedMessage)) {
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Sinal de finalizacao (Tombstone) recebido e gravado em disco."
                        );
                    } else {
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "RECEBIDA_PELA_REDE"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "GRAVADA_FILA_DESTINO"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "AGUARDANDO_PROCESSAMENTO_APLICACAO"
                        );

                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem recebida via protocolo MPI."
                        );
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem serializada e gravada com seguranca em disco."
                        );
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem em espera na fila de aplicacao (aguardando idle terminar)."
                        );
                    }

                    registerPendingSend(
                        pendingAcksToDispatch,
                        createAck(
                            receivedMessage.id,
                            DESTINO,
                            SERVIDOR,
                            "Mensagem persistida pelo Destino."
                        ),
                        SERVIDOR
                    );

                    Logger::info(
                        rank,
                        "DESTINO-REDE",
                        "Confirmacao (ACK) assincrona enfileirada para o Servidor."
                    );

                    if (isFinishMessage(receivedMessage)) {
                        hasReceivedFinishSignal.store(true);
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
            hasReceivedFinishSignal.store(true);
        }
    }
}

void runDestination(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool hasReceivedFinishSignal{false};
    std::atomic_bool isApplicationShutdownRequested{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;

    std::thread applicationThread(
        runDestinationApplication,
        rank,
        std::ref(queue),
        std::cref(hasReceivedFinishSignal),
        std::ref(isApplicationShutdownRequested),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runDestinationNetwork,
        rank,
        std::ref(queue),
        std::ref(hasReceivedFinishSignal),
        std::ref(networkException)
    );

    applicationThread.join();
    networkThread.join();

    checkThreadException(applicationException);
    checkThreadException(networkException);
}

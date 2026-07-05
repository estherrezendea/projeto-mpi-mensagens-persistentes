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
        const std::atomic_bool& finishReceivedByNetwork,
        std::atomic_bool& applicationFinished,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!applicationFinished.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type != SimulationCommandType::Sleep) {
                        continue;
                    }

                    for (int remaining = command.seconds;
                         remaining > 0 && !applicationFinished.load();
                         --remaining) {
                        SimulationState::markSleeping(rank, remaining);
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Aplicacao dormindo."
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
                                "APP_SLEEPING"
                            );
                            SimulationState::appendHistory(
                                message.id,
                                "Destino",
                                "APP_AWAKE"
                            );
                        }
                    }

                    SimulationState::markAwake(rank);
                    Logger::info(rank, "DESTINO", "Aplicacao acordou.");
                }

                const auto pendingMessage = queue.peek();

                if (!pendingMessage.has_value()) {
                    if (finishReceivedByNetwork.load()) {
                        applicationFinished.store(true);
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
                        "Finalizacao processada. Destino encerrou a aplicacao."
                    );

                    applicationFinished.store(true);
                    SimulationState::markStopped(rank);
                } else {
                    SimulationState::appendHistory(
                        removedMessage.id,
                        "Destino",
                        "PROCESSED"
                    );
                    SimulationState::appendProcessedMessage(
                        rank,
                        removedMessage
                    );

                    Logger::info(rank, "DESTINO", "Processando mensagem.");
                    Logger::info(
                        rank,
                        "DESTINO",
                        "Mensagem concluida."
                    );
                }
            }
        } catch (...) {
            exception = std::current_exception();
            applicationFinished.store(true);
        }
    }

    void runDestinationNetwork(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& finishReceivedByNetwork,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::list<MpiMessageTransport::PendingSend> pendingAckSends;

            Logger::info(
                rank,
                "DESTINO-REDE",
                "Loop MPI iniciado. Recebimento e ACK independem do sono da aplicacao."
            );

            while (!finishReceivedByNetwork.load()) {
                testPendingSends(pendingAckSends);

                if (MpiMessageTransport::hasMessage(SERVIDOR)) {
                    const Message receivedMessage =
                        MpiMessageTransport::receiveAvailable(SERVIDOR);

                    queue.enqueue(receivedMessage);

                    if (isFinishMessage(receivedMessage)) {
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem de finalizacao recebida e persistida."
                        );
                    } else {
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "RECEIVED"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "DESTINATION_PERSISTED"
                        );
                        SimulationState::appendHistory(
                            receivedMessage.id,
                            "Destino",
                            "WAITING_PROCESSING"
                        );

                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem recebida."
                        );
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem persistida."
                        );
                        Logger::info(
                            rank,
                            "DESTINO",
                            "Mensagem aguardando processamento."
                        );
                    }

                    registerPendingSend(
                        pendingAckSends,
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
                        "ACK assincrono registrado para o Servidor."
                    );

                    if (isFinishMessage(receivedMessage)) {
                        finishReceivedByNetwork.store(true);
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
            finishReceivedByNetwork.store(true);
        }
    }
}

void runDestination(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool finishReceivedByNetwork{false};
    std::atomic_bool applicationFinished{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;

    std::thread applicationThread(
        runDestinationApplication,
        rank,
        std::ref(queue),
        std::cref(finishReceivedByNetwork),
        std::ref(applicationFinished),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runDestinationNetwork,
        rank,
        std::ref(queue),
        std::ref(finishReceivedByNetwork),
        std::ref(networkException)
    );

    applicationThread.join();
    networkThread.join();

    checkThreadException(applicationException);
    checkThreadException(networkException);
}

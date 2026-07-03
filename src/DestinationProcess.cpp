#include "DestinationProcess.hpp"

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "SimulationRoles.hpp"
#include "SimulationSupport.hpp"
#include "SleepSimulator.hpp"

#include <atomic>
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
        try {
            while (!applicationFinished.load()) {
                sleepRandomly(
                    rank,
                    "DESTINO-APP",
                    "A aplicacao pode dormir; a thread MPI continua recebendo e enviando ACKs.",
                    randomGenerator
                );

                const auto pendingMessage = queue.peek();

                if (!pendingMessage.has_value()) {
                    if (finishReceivedByNetwork.load()) {
                        applicationFinished.store(true);
                    }

                    continue;
                }

                const Message removedMessage =
                    removeMessageAfterAck(queue, "o Destino");

                if (isFinishMessage(removedMessage)) {
                    Logger::info(
                        rank,
                        "DESTINO-APP",
                        "Finalizacao processada. Destino encerrou a aplicacao."
                    );

                    applicationFinished.store(true);
                } else {
                    Logger::info(
                        rank,
                        "DESTINO-APP",
                        "Mensagem #" +
                        std::to_string(removedMessage.id) +
                        " processada com sucesso: \"" +
                        removedMessage.content + "\""
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
                            "DESTINO-REDE",
                            "Mensagem de finalizacao recebida e persistida."
                        );
                    } else {
                        Logger::info(
                            rank,
                            "DESTINO-REDE",
                            "Mensagem #" + std::to_string(receivedMessage.id) +
                            " recebida e persistida: \"" +
                            receivedMessage.content + "\""
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

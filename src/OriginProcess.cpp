#include "OriginProcess.hpp"

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "SimulationRoles.hpp"
#include "SimulationSupport.hpp"
#include "SleepSimulator.hpp"

#include <atomic>
#include <exception>
#include <functional>
#include <optional>
#include <string>
#include <thread>

namespace {
    void runOriginApplication(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& messagesReady,
        std::atomic_bool& networkFinished,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        try {
            Logger::info(
                rank,
                "ORIGEM-APP",
                "Criando e persistindo mensagens na fila local."
            );

            for (const Message& message : createDemoMessages()) {
                queue.enqueue(message);

                if (isFinishMessage(message)) {
                    Logger::info(
                        rank,
                        "ORIGEM-APP",
                        "Mensagem de finalizacao persistida na fila."
                    );
                } else {
                    Logger::info(
                        rank,
                        "ORIGEM-APP",
                        "Mensagem #" + std::to_string(message.id) +
                        " persistida: \"" + message.content + "\""
                    );
                }
            }

            messagesReady.store(true);

            while (!networkFinished.load()) {
                sleepRandomly(
                    rank,
                    "ORIGEM-APP",
                    "A aplicacao esta indisponivel; a thread MPI continua enviando.",
                    randomGenerator
                );
            }
        } catch (...) {
            exception = std::current_exception();
            networkFinished.store(true);
        }
    }

    void runOriginNetwork(
        const int rank,
        PersistentQueue& queue,
        const std::atomic_bool& messagesReady,
        std::atomic_bool& networkFinished,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::optional<Message> inFlightMessage;
            MpiMessageTransport::PendingSend pendingSend;
            MpiMessageTransport::PendingReceive pendingAckReceive;
            bool sendActive = false;
            bool ackActive = false;

            Logger::info(
                rank,
                "ORIGEM-REDE",
                "Loop MPI iniciado e independente da thread de aplicacao."
            );

            while (!networkFinished.load()) {
                if (!inFlightMessage.has_value() && messagesReady.load()) {
                    const auto pendingMessage = queue.peek();

                    if (pendingMessage.has_value()) {
                        inFlightMessage = pendingMessage.value();

                        MpiMessageTransport::startReceive(
                            pendingAckReceive,
                            SERVIDOR
                        );
                        ackActive = true;

                        MpiMessageTransport::startSend(
                            pendingSend,
                            inFlightMessage.value(),
                            SERVIDOR
                        );
                        sendActive = true;

                        Logger::info(
                            rank,
                            "ORIGEM-REDE",
                            "Envio assincrono da mensagem #" +
                            std::to_string(inFlightMessage->id) +
                            " para o Servidor iniciado."
                        );
                    }
                }

                if (sendActive && MpiMessageTransport::test(pendingSend)) {
                    sendActive = false;
                }

                if (ackActive) {
                    const auto ack = MpiMessageTransport::test(
                        pendingAckReceive
                    );

                    if (ack.has_value()) {
                        ackActive = false;
                        validateAck(
                            ack.value(),
                            inFlightMessage->id,
                            SERVIDOR,
                            "a Origem"
                        );

                        const Message removedMessage =
                            removeMessageAfterAck(queue, "a Origem");

                        if (isFinishMessage(removedMessage)) {
                            Logger::info(
                                rank,
                                "ORIGEM-REDE",
                                "ACK da finalizacao recebido. Origem encerrou os envios."
                            );

                            networkFinished.store(true);
                        } else {
                            Logger::info(
                                rank,
                                "ORIGEM-REDE",
                                "ACK recebido para a mensagem #" +
                                std::to_string(removedMessage.id) +
                                ". Mensagem removida da fila local."
                            );
                        }

                        inFlightMessage.reset();
                    }
                }

                networkPause();
            }
        } catch (...) {
            exception = std::current_exception();
            networkFinished.store(true);
        }
    }
}

void runOrigin(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool messagesReady{false};
    std::atomic_bool networkFinished{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;

    std::thread applicationThread(
        runOriginApplication,
        rank,
        std::ref(queue),
        std::ref(messagesReady),
        std::ref(networkFinished),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runOriginNetwork,
        rank,
        std::ref(queue),
        std::cref(messagesReady),
        std::ref(networkFinished),
        std::ref(networkException)
    );

    applicationThread.join();
    networkThread.join();

    checkThreadException(applicationException);
    checkThreadException(networkException);
}

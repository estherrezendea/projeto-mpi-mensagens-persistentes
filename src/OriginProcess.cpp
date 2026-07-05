#include "OriginProcess.hpp"

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "SimulationConsole.hpp"
#include "SimulationRoles.hpp"
#include "SimulationState.hpp"
#include "SimulationSupport.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <functional>
#include <optional>
#include <string>
#include <thread>

namespace {
    void sleepApplication(
        const int rank,
        const int seconds,
        const std::atomic_bool& networkFinished
    ) {
        for (int remaining = seconds;
             remaining > 0 && !networkFinished.load();
             --remaining) {
            SimulationState::markSleeping(rank, remaining);
            Logger::info(rank, "ORIGEM", "Aplicacao dormindo.");
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        SimulationState::markAwake(rank);
        Logger::info(rank, "ORIGEM", "Aplicacao acordou.");
    }

    void createUserMessage(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& messagesReady,
        const SimulationCommand& command
    ) {
        using namespace SimulationRoles;

        const Message message = createMessage(
            command.messageId,
            ORIGEM,
            SERVIDOR,
            MessageType::Normal,
            command.content
        );

        SimulationState::appendHistory(
            message.id,
            "Origem",
            "CREATED",
            message.content
        );
        Logger::info(rank, "ORIGEM", "Mensagem criada.");

        queue.enqueue(message);

        SimulationState::appendHistory(
            message.id,
            "Origem",
            "PERSISTED"
        );
        Logger::info(rank, "ORIGEM", "Mensagem persistida.");

        messagesReady.store(true);
    }

    void createFinishMessage(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& messagesReady
    ) {
        using namespace SimulationRoles;

        const Message finishMessage = createMessage(
            999999,
            ORIGEM,
            SERVIDOR,
            MessageType::Finish,
            "FIM_DA_SIMULACAO"
        );

        queue.enqueue(finishMessage);
        messagesReady.store(true);

        SimulationState::appendHistory(
            finishMessage.id,
            "Origem",
            "FINISH",
            finishMessage.content
        );

        Logger::info(rank, "ORIGEM", "Finalizacao solicitada.");
    }

    void runOriginApplication(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& messagesReady,
        std::atomic_bool& networkFinished,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!networkFinished.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type == SimulationCommandType::Sleep) {
                        sleepApplication(
                            rank,
                            command.seconds,
                            networkFinished
                        );
                    } else if (
                        command.type == SimulationCommandType::SendMessage
                    ) {
                        createUserMessage(
                            rank,
                            queue,
                            messagesReady,
                            command
                        );
                    } else if (command.type == SimulationCommandType::Stop) {
                        createFinishMessage(
                            rank,
                            queue,
                            messagesReady
                        );
                    }
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

                        if (!isFinishMessage(inFlightMessage.value())) {
                            SimulationState::appendHistory(
                                inFlightMessage->id,
                                "Origem",
                                "SENT_TO_SERVER"
                            );

                            Logger::info(
                                rank,
                                "ORIGEM",
                                "Mensagem enviada ao servidor."
                            );
                        }
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
                                "ORIGEM",
                                "ACK da finalizacao recebido."
                            );

                            networkFinished.store(true);
                        } else {
                            SimulationState::appendHistory(
                                removedMessage.id,
                                "Origem",
                                "ACK_FROM_SERVER"
                            );

                            Logger::info(
                                rank,
                                "ORIGEM",
                                "ACK recebido. Mensagem removida da fila."
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
    std::atomic_bool consoleFinished{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;
    std::exception_ptr consoleException;

    std::thread consoleThread(
        [&consoleFinished, &consoleException]() {
            try {
                SimulationConsole console(consoleFinished);
                console.run();
            } catch (...) {
                consoleException = std::current_exception();
                consoleFinished.store(true);
            }
        }
    );

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

    consoleThread.join();
    applicationThread.join();
    networkThread.join();

    checkThreadException(consoleException);
    checkThreadException(applicationException);
    checkThreadException(networkException);
}

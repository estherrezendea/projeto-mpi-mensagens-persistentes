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
        const std::atomic_bool& isNetworkShutdownRequested
    ) {
        for (int remaining = seconds;
             remaining > 0 && !isNetworkShutdownRequested.load();
             --remaining) {
            SimulationState::markSleeping(rank, remaining);
            Logger::info(rank, "ORIGEM", "Processo de aplicacao suspenso (idle mode).");
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        SimulationState::markAwake(rank);
        Logger::info(rank, "ORIGEM", "Processo de aplicacao retomado.");
    }

    void createUserMessage(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& isMessageQueueReady,
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
            "GERADA_PELA_ORIGEM",
            message.content
        );
        Logger::info(rank, "ORIGEM", "Nova mensagem instanciada na Origem.");

        queue.enqueue(message);

        SimulationState::appendHistory(
            message.id,
            "Origem",
            "GRAVADA_FILA_ORIGEM"
        );
        Logger::info(rank, "ORIGEM", "Mensagem serializada e gravada com seguranca em disco.");

        isMessageQueueReady.store(true);
    }

    void createFinishMessage(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& isMessageQueueReady
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
        isMessageQueueReady.store(true);

        SimulationState::appendHistory(
            finishMessage.id,
            "Origem",
            "SINAL_FINALIZACAO_GERADO",
            finishMessage.content
        );

        Logger::info(rank, "ORIGEM", "Sinal de finalizacao (Tombstone) instanciado.");
    }

    void runOriginApplication(
        const int rank,
        PersistentQueue& queue,
        std::atomic_bool& isMessageQueueReady,
        std::atomic_bool& isNetworkShutdownRequested,
        std::exception_ptr& exception,
        std::mt19937 randomGenerator
    ) {
        (void)randomGenerator;

        try {
            SimulationState::markAwake(rank);

            while (!isNetworkShutdownRequested.load()) {
                const auto commands = SimulationState::consumeCommands(rank);

                for (const SimulationCommand& command : commands) {
                    if (command.type == SimulationCommandType::Sleep) {
                        sleepApplication(
                            rank,
                            command.seconds,
                            isNetworkShutdownRequested
                        );
                    } else if (
                        command.type == SimulationCommandType::SendMessage
                    ) {
                        createUserMessage(
                            rank,
                            queue,
                            isMessageQueueReady,
                            command
                        );
                    } else if (command.type == SimulationCommandType::Stop) {
                        createFinishMessage(
                            rank,
                            queue,
                            isMessageQueueReady
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
            isNetworkShutdownRequested.store(true);
        }
    }

    void runOriginNetwork(
        const int rank,
        PersistentQueue& queue,
        const std::atomic_bool& isMessageQueueReady,
        std::atomic_bool& isNetworkShutdownRequested,
        std::exception_ptr& exception
    ) {
        try {
            using namespace SimulationRoles;

            std::optional<Message> inFlightMessage;
            MpiMessageTransport::PendingSend pendingSend;
            MpiMessageTransport::PendingReceive pendingAcknowledgment;
            bool isTransmissionActive = false;
            bool isAcknowledgmentActive = false;

            Logger::info(
                rank,
                "ORIGEM-REDE",
                "Ciclo de rede MPI assincrono inicializado (thread separada)."
            );

            while (!isNetworkShutdownRequested.load()) {
                if (!inFlightMessage.has_value() && isMessageQueueReady.load()) {
                    const auto pendingMessage = queue.peek();

                    if (pendingMessage.has_value()) {
                        inFlightMessage = pendingMessage.value();

                        MpiMessageTransport::startReceive(
                            pendingAcknowledgment,
                            SERVIDOR
                        );
                        isAcknowledgmentActive = true;

                        MpiMessageTransport::startSend(
                            pendingSend,
                            inFlightMessage.value(),
                            SERVIDOR
                        );
                        isTransmissionActive = true;

                        if (!isFinishMessage(inFlightMessage.value())) {
                            SimulationState::appendHistory(
                                inFlightMessage->id,
                                "Origem",
                                "ENVIADA_AO_SERVIDOR_MPI"
                            );

                            Logger::info(
                                rank,
                                "ORIGEM",
                                "Mensagem despachada ao no Servidor via comunicacao MPI."
                            );
                        }
                    }
                }

                if (isTransmissionActive && MpiMessageTransport::test(pendingSend)) {
                    isTransmissionActive = false;
                }

                if (isAcknowledgmentActive) {
                    const auto ack = MpiMessageTransport::test(
                        pendingAcknowledgment
                    );

                    if (ack.has_value()) {
                        isAcknowledgmentActive = false;
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
                                "Confirmacao (ACK) de finalizacao recebida do Servidor."
                            );

                            isNetworkShutdownRequested.store(true);
                        } else {
                            SimulationState::appendHistory(
                                removedMessage.id,
                                "Origem",
                                "CONFIRMACAO_RECEBIDA_SERVIDOR"
                            );

                            Logger::info(
                                rank,
                                "ORIGEM",
                                "Confirmacao (ACK) recebida. Mensagem limpa da fila local."
                            );
                        }

                        inFlightMessage.reset();
                    }
                }

                networkPause();
            }
        } catch (...) {
            exception = std::current_exception();
            isNetworkShutdownRequested.store(true);
        }
    }
}

void runOrigin(
    const int rank,
    PersistentQueue& queue,
    std::mt19937 randomGenerator
) {
    std::atomic_bool isMessageQueueReady{false};
    std::atomic_bool isNetworkShutdownRequested{false};
    std::atomic_bool isConsoleShutdownRequested{false};
    std::exception_ptr applicationException;
    std::exception_ptr networkException;
    std::exception_ptr consoleException;

    std::thread consoleThread(
        [&isConsoleShutdownRequested, &consoleException]() {
            try {
                SimulationConsole console(isConsoleShutdownRequested);
                console.run();
            } catch (...) {
                consoleException = std::current_exception();
                isConsoleShutdownRequested.store(true);
            }
        }
    );

    std::thread applicationThread(
        runOriginApplication,
        rank,
        std::ref(queue),
        std::ref(isMessageQueueReady),
        std::ref(isNetworkShutdownRequested),
        std::ref(applicationException),
        randomGenerator
    );

    std::thread networkThread(
        runOriginNetwork,
        rank,
        std::ref(queue),
        std::cref(isMessageQueueReady),
        std::ref(isNetworkShutdownRequested),
        std::ref(networkException)
    );

    consoleThread.join();
    applicationThread.join();
    networkThread.join();

    checkThreadException(consoleException);
    checkThreadException(applicationException);
    checkThreadException(networkException);
}

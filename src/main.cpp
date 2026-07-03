#include <mpi.h>

#include "Logger.hpp"
#include "MpiMessageTransport.hpp"
#include "PersistentQueue.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
    constexpr int ORIGEM = 0;
    constexpr int SERVIDOR = 1;
    constexpr int DESTINO = 2;

    bool shouldResetQueues(int argc, char* argv[]) {
        for (int index = 1; index < argc; index++) {
            if (std::string(argv[index]) == "--reset") {
                return true;
            }
        }

        return false;
    }

    Message createMessage(
        const int id,
        const int senderRank,
        const int receiverRank,
        const MessageType type,
        const std::string& content
    ) {
        return Message{
            id,
            senderRank,
            receiverRank,
            type,
            content
        };
    }

    Message createAck(
        const int messageId,
        const int senderRank,
        const int receiverRank,
        const std::string& content
    ) {
        return createMessage(
            messageId,
            senderRank,
            receiverRank,
            MessageType::Ack,
            content
        );
    }

    bool isFinishMessage(const Message& message) {
        return message.type == MessageType::Finish;
    }

    void validateAck(
        const Message& ack,
        const int expectedMessageId,
        const int expectedSender,
        const std::string& receiverName
    ) {
        const bool isInvalid =
            ack.type != MessageType::Ack ||
            ack.id != expectedMessageId ||
            ack.senderRank != expectedSender;

        if (isInvalid) {
            throw std::runtime_error(
                "ACK invalido recebido por " + receiverName + "."
            );
        }
    }

    Message removeMessageAfterAck(
        PersistentQueue& queue,
        const std::string& nodeName
    ) {
        const auto removedMessage = queue.dequeue();

        if (!removedMessage.has_value()) {
            throw std::runtime_error(
                "Tentativa de remover mensagem de uma fila vazia em " +
                nodeName + "."
            );
        }

        return removedMessage.value();
    }

    void sleepRandomly(
        const int rank,
        const std::string& component,
        const std::string& reason,
        std::mt19937& randomGenerator
    ) {
        std::uniform_int_distribution<int> secondsDistribution(1, 3);

        const int seconds = secondsDistribution(randomGenerator);

        Logger::info(
            rank,
            component,
            "Entrando em espera aleatoria por " +
            std::to_string(seconds) +
            " segundo(s). " +
            reason
        );

        std::this_thread::sleep_for(
            std::chrono::seconds(seconds)
        );

        Logger::info(
            rank,
            component,
            "Acordou e retomou a execucao."
        );
    }

    std::vector<Message> createDemoMessages() {
        return {
            createMessage(
                1,
                ORIGEM,
                SERVIDOR,
                MessageType::Normal,
                "Solicitacao de processamento de imagem LT-102"
            ),
            createMessage(
                2,
                ORIGEM,
                SERVIDOR,
                MessageType::Normal,
                "Atualizacao cadastral do ativo AT-045"
            ),
            createMessage(
                3,
                ORIGEM,
                SERVIDOR,
                MessageType::Normal,
                "Relatorio de inspecao da torre T-883"
            ),
            createMessage(
                4,
                ORIGEM,
                SERVIDOR,
                MessageType::Normal,
                "Alerta de vegetacao proxima a rede"
            ),
            createMessage(
                5,
                ORIGEM,
                SERVIDOR,
                MessageType::Normal,
                "Solicitacao de reprocessamento de analise"
            ),
            createMessage(
                999,
                ORIGEM,
                SERVIDOR,
                MessageType::Finish,
                "FIM_DA_SIMULACAO"
            )
        };
    }

    void runOrigin(
        const int rank,
        PersistentQueue& queue,
        std::mt19937& randomGenerator
    ) {
        const std::vector<Message> messages = createDemoMessages();

        Logger::info(
            rank,
            "ORIGEM",
            "Criando e persistindo mensagens na fila local."
        );

        for (const Message& message : messages) {
            queue.enqueue(message);

            if (isFinishMessage(message)) {
                Logger::info(
                    rank,
                    "ORIGEM",
                    "Mensagem de finalizacao persistida na fila."
                );
            } else {
                Logger::info(
                    rank,
                    "ORIGEM",
                    "Mensagem #" + std::to_string(message.id) +
                    " persistida: \"" + message.content + "\""
                );
            }
        }

        Logger::info(
            rank,
            "ORIGEM",
            "Todas as mensagens foram persistidas. Iniciando envio FIFO."
        );

        while (true) {
            const auto pendingMessage = queue.peek();

            if (!pendingMessage.has_value()) {
                throw std::runtime_error(
                    "A fila da Origem ficou vazia antes da finalizacao."
                );
            }

            const Message messageToSend = pendingMessage.value();

            if (!isFinishMessage(messageToSend)) {
                sleepRandomly(
                    rank,
                    "ORIGEM",
                    "A mensagem permanece persistida na fila local.",
                    randomGenerator
                );
            }

            MpiMessageTransport::PendingSend pendingSend;

            MpiMessageTransport::startSend(
                pendingSend,
                messageToSend,
                SERVIDOR
            );

            if (isFinishMessage(messageToSend)) {
                Logger::info(
                    rank,
                    "REDE",
                    "Envio assincrono da mensagem de finalizacao iniciado."
                );
            } else {
                Logger::info(
                    rank,
                    "REDE",
                    "Envio assincrono da mensagem #" +
                    std::to_string(messageToSend.id) +
                    " para o Servidor iniciado."
                );
            }

            MpiMessageTransport::PendingReceive pendingAckReceive;

            MpiMessageTransport::startReceive(
                pendingAckReceive,
                SERVIDOR
            );

            MpiMessageTransport::wait(pendingSend);

            const Message ack =
                MpiMessageTransport::wait(pendingAckReceive);

            validateAck(
                ack,
                messageToSend.id,
                SERVIDOR,
                "a Origem"
            );

            const Message removedMessage =
                removeMessageAfterAck(queue, "a Origem");

            if (isFinishMessage(removedMessage)) {
                Logger::info(
                    rank,
                    "ORIGEM",
                    "ACK da finalizacao recebido. Origem encerrou os envios."
                );

                break;
            }

            Logger::info(
                rank,
                "ORIGEM",
                "ACK recebido para a mensagem #" +
                std::to_string(removedMessage.id) +
                ". Mensagem removida da fila local."
            );
        }
    }

    void runServer(
        const int rank,
        PersistentQueue& queue,
        std::mt19937& randomGenerator
    ) {
        Logger::info(
            rank,
            "SERVIDOR",
            "Servidor pronto para receber mensagens da Origem."
        );

        while (true) {
            MpiMessageTransport::PendingReceive pendingReceive;

            MpiMessageTransport::startReceive(
                pendingReceive,
                ORIGEM
            );

            Logger::info(
                rank,
                "REDE",
                "Recebimento assincrono da Origem iniciado."
            );

            const Message receivedMessage =
                MpiMessageTransport::wait(pendingReceive);

            queue.enqueue(receivedMessage);

            if (isFinishMessage(receivedMessage)) {
                Logger::info(
                    rank,
                    "SERVIDOR",
                    "Mensagem de finalizacao recebida e persistida."
                );
            } else {
                Logger::info(
                    rank,
                    "SERVIDOR",
                    "Mensagem #" + std::to_string(receivedMessage.id) +
                    " recebida e persistida: \"" +
                    receivedMessage.content + "\""
                );
            }

            const Message ackForOrigin = createAck(
                receivedMessage.id,
                SERVIDOR,
                ORIGEM,
                "Mensagem persistida pelo Servidor."
            );

            MpiMessageTransport::PendingSend pendingAckSend;

            MpiMessageTransport::startSend(
                pendingAckSend,
                ackForOrigin,
                ORIGEM
            );

            MpiMessageTransport::wait(pendingAckSend);

            Logger::info(
                rank,
                "REDE",
                "ACK enviado para a Origem."
            );

            if (!isFinishMessage(receivedMessage)) {
                sleepRandomly(
                    rank,
                    "SERVIDOR",
                    "A mensagem ja esta persistida na fila do servidor.",
                    randomGenerator
                );
            }

            /*
             * O servidor recupera a mensagem da propria fila persistente
             * antes de encaminha-la ao destino.
             */
            const auto pendingMessage = queue.peek();

            if (!pendingMessage.has_value()) {
                throw std::runtime_error(
                    "A fila do Servidor ficou vazia inesperadamente."
                );
            }

            Message forwardedMessage = pendingMessage.value();

            if (isFinishMessage(forwardedMessage)) {
                Logger::info(
                    rank,
                    "SERVIDOR",
                    "Mensagem de finalizacao recuperada da fila persistente."
                );
            } else {
                Logger::info(
                    rank,
                    "SERVIDOR",
                    "Mensagem #" +
                    std::to_string(forwardedMessage.id) +
                    " recuperada da fila persistente antes do encaminhamento."
                );
            }

            forwardedMessage.senderRank = SERVIDOR;
            forwardedMessage.receiverRank = DESTINO;

            MpiMessageTransport::PendingSend pendingForwardSend;

            MpiMessageTransport::startSend(
                pendingForwardSend,
                forwardedMessage,
                DESTINO
            );

            MpiMessageTransport::PendingReceive pendingAckReceive;

            MpiMessageTransport::startReceive(
                pendingAckReceive,
                DESTINO
            );

            if (isFinishMessage(forwardedMessage)) {
                Logger::info(
                    rank,
                    "REDE",
                    "Envio assincrono da finalizacao para o Destino iniciado."
                );
            } else {
                Logger::info(
                    rank,
                    "REDE",
                    "Envio assincrono da mensagem #" +
                    std::to_string(forwardedMessage.id) +
                    " para o Destino iniciado."
                );
            }

            MpiMessageTransport::wait(pendingForwardSend);

            const Message ackForServer =
                MpiMessageTransport::wait(pendingAckReceive);

            validateAck(
                ackForServer,
                forwardedMessage.id,
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

                break;
            }

            Logger::info(
                rank,
                "SERVIDOR",
                "ACK do Destino recebido para a mensagem #" +
                std::to_string(removedMessage.id) +
                ". Mensagem removida da fila."
            );
        }
    }

    void runDestination(
        const int rank,
        PersistentQueue& queue,
        std::mt19937& randomGenerator
    ) {
        Logger::info(
            rank,
            "DESTINO",
            "Destino pronto para receber e processar mensagens."
        );

        while (true) {
            MpiMessageTransport::PendingReceive pendingReceive;

            MpiMessageTransport::startReceive(
                pendingReceive,
                SERVIDOR
            );

            Logger::info(
                rank,
                "REDE",
                "Recebimento assincrono do Servidor iniciado."
            );

            const Message receivedMessage =
                MpiMessageTransport::wait(pendingReceive);

            queue.enqueue(receivedMessage);

            if (isFinishMessage(receivedMessage)) {
                Logger::info(
                    rank,
                    "DESTINO",
                    "Mensagem de finalizacao recebida e persistida."
                );
            } else {
                Logger::info(
                    rank,
                    "DESTINO",
                    "Mensagem #" + std::to_string(receivedMessage.id) +
                    " recebida e persistida: \"" +
                    receivedMessage.content + "\""
                );
            }

            const Message ackForServer = createAck(
                receivedMessage.id,
                DESTINO,
                SERVIDOR,
                "Mensagem persistida pelo Destino."
            );

            MpiMessageTransport::PendingSend pendingAckSend;

            MpiMessageTransport::startSend(
                pendingAckSend,
                ackForServer,
                SERVIDOR
            );

            MpiMessageTransport::wait(pendingAckSend);

            Logger::info(
                rank,
                "REDE",
                "ACK enviado para o Servidor."
            );

            if (!isFinishMessage(receivedMessage)) {
                sleepRandomly(
                    rank,
                    "DESTINO",
                    "A mensagem permanece persistida na fila do destino.",
                    randomGenerator
                );
            }

            const auto pendingMessage = queue.peek();

            if (!pendingMessage.has_value()) {
                throw std::runtime_error(
                    "A fila do Destino ficou vazia inesperadamente."
                );
            }

            const Message messageToProcess = pendingMessage.value();

            const Message removedMessage =
                removeMessageAfterAck(queue, "o Destino");

            if (isFinishMessage(removedMessage)) {
                Logger::info(
                    rank,
                    "DESTINO",
                    "Finalizacao processada. Destino encerrou."
                );

                break;
            }

            Logger::info(
                rank,
                "DESTINO",
                "Mensagem #" +
                std::to_string(messageToProcess.id) +
                " processada com sucesso: \"" +
                messageToProcess.content + "\""
            );
        }
    }
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int processCount = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processCount);

    if (processCount != 3) {
        if (rank == ORIGEM) {
            std::cerr
                << "Erro: execute com exatamente 3 processos.\n"
                << "Exemplo: mpirun -np 3 ./build/simulador_mpi --reset\n";
        }

        MPI_Finalize();
        return 1;
    }

    try {
        const std::filesystem::path queuePath =
            "data/fila_processo_" + std::to_string(rank) + ".txt";

        PersistentQueue queue(queuePath);

        if (shouldResetQueues(argc, argv)) {
            queue.clear();

            Logger::info(
                rank,
                "FILA",
                "Fila persistente limpa para uma nova simulacao."
            );
        }

        /*
         * Cada processo recebe uma semente diferente.
         * Assim, as duracoes de espera nao ficam identicas.
         */
        const unsigned int seed =
            static_cast<unsigned int>(
                std::chrono::steady_clock::now()
                    .time_since_epoch()
                    .count()
            ) +
            static_cast<unsigned int>(rank * 1000);

        std::mt19937 randomGenerator(seed);

        MPI_Barrier(MPI_COMM_WORLD);

        if (rank == ORIGEM) {
            runOrigin(rank, queue, randomGenerator);
        } else if (rank == SERVIDOR) {
            runServer(rank, queue, randomGenerator);
        } else if (rank == DESTINO) {
            runDestination(rank, queue, randomGenerator);
        }

        MPI_Barrier(MPI_COMM_WORLD);

        Logger::info(
            rank,
            "SISTEMA",
            "Simulacao concluida com sucesso."
        );
    } catch (const std::exception& exception) {
        std::cerr
            << "[Processo " << rank << "] Erro: "
            << exception.what()
            << std::endl;

        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Finalize();
    return 0;
}
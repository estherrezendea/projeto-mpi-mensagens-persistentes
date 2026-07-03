#include "SimulationSupport.hpp"

#include "SimulationRoles.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>

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

std::vector<Message> createDemoMessages() {
    using namespace SimulationRoles;

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

void checkThreadException(const std::exception_ptr& exception) {
    if (exception != nullptr) {
        std::rethrow_exception(exception);
    }
}

void networkPause() {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
}

void registerPendingSend(
    std::list<MpiMessageTransport::PendingSend>& pendingSends,
    const Message& message,
    const int destinationRank
) {
    pendingSends.emplace_back();
    MpiMessageTransport::startSend(
        pendingSends.back(),
        message,
        destinationRank
    );
}

void testPendingSends(
    std::list<MpiMessageTransport::PendingSend>& pendingSends
) {
    for (auto iterator = pendingSends.begin();
         iterator != pendingSends.end();) {
        if (MpiMessageTransport::test(*iterator)) {
            iterator = pendingSends.erase(iterator);
        } else {
            ++iterator;
        }
    }
}

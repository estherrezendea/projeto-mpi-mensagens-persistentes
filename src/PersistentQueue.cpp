#include "PersistentQueue.hpp"

#include <fstream>
#include <iomanip>
#include <stdexcept>
#include <utility>

PersistentQueue::PersistentQueue(std::filesystem::path filePath)
    : filePath_(std::move(filePath)) {
    const auto parentDirectory = filePath_.parent_path();

    if (!parentDirectory.empty()) {
        std::filesystem::create_directories(parentDirectory);
    }

    if (!std::filesystem::exists(filePath_)) {
        std::ofstream file(filePath_);

        if (!file) {
            throw std::runtime_error(
                "Não foi possível criar a fila persistente: " +
                filePath_.string()
            );
        }
    }
}

void PersistentQueue::enqueue(const Message& message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ofstream file(filePath_, std::ios::app);

    if (!file) {
        throw std::runtime_error(
            "Não foi possível abrir a fila para escrita: " +
            filePath_.string()
        );
    }

    file
        << message.id << ' '
        << message.senderRank << ' '
        << message.receiverRank << ' '
        << static_cast<int>(message.type) << ' '
        << std::quoted(message.content)
        << '\n';

    file.flush();
}

std::optional<Message> PersistentQueue::peek() const {
    std::lock_guard<std::mutex> lock(mutex_);

    const auto messages = readAllUnlocked();

    if (messages.empty()) {
        return std::nullopt;
    }

    return messages.front();
}

std::optional<Message> PersistentQueue::dequeue() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto messages = readAllUnlocked();

    if (messages.empty()) {
        return std::nullopt;
    }

    Message firstMessage = messages.front();
    messages.erase(messages.begin());

    writeAllUnlocked(messages);

    return firstMessage;
}

bool PersistentQueue::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);

    return readAllUnlocked().empty();
}

void PersistentQueue::clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    writeAllUnlocked({});
}

const std::filesystem::path& PersistentQueue::path() const {
    return filePath_;
}

std::vector<Message> PersistentQueue::readAllUnlocked() const {
    std::ifstream file(filePath_);

    if (!file) {
        throw std::runtime_error(
            "Não foi possível abrir a fila para leitura: " +
            filePath_.string()
        );
    }

    std::vector<Message> messages;

    int typeValue = 0;
    Message message;

    while (
        file
        >> message.id
        >> message.senderRank
        >> message.receiverRank
        >> typeValue
        >> std::quoted(message.content)
    ) {
        message.type = static_cast<MessageType>(typeValue);
        messages.push_back(message);
    }

    return messages;
}

void PersistentQueue::writeAllUnlocked(
    const std::vector<Message>& messages
) const {
    const std::filesystem::path temporaryPath =
        filePath_.string() + ".tmp";

    std::ofstream file(temporaryPath, std::ios::trunc);

    if (!file) {
        throw std::runtime_error(
            "Não foi possível atualizar a fila persistente: " +
            filePath_.string()
        );
    }

    for (const auto& message : messages) {
        file
            << message.id << ' '
            << message.senderRank << ' '
            << message.receiverRank << ' '
            << static_cast<int>(message.type) << ' '
            << std::quoted(message.content)
            << '\n';
    }

    file.flush();

    if (!file) {
        throw std::runtime_error(
            "Não foi possível gravar a fila temporária: " +
            temporaryPath.string()
        );
    }

    file.close();

    std::filesystem::remove(filePath_);
    std::filesystem::rename(temporaryPath, filePath_);
}

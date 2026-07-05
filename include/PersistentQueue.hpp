#pragma once

#include "Message.hpp"

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

class PersistentQueue {
public:
    explicit PersistentQueue(std::filesystem::path filePath);

    void enqueue(const Message& message);

    [[nodiscard]] std::optional<Message> peek() const;

    [[nodiscard]] std::optional<Message> dequeue();

    [[nodiscard]] bool empty() const;

    [[nodiscard]] std::vector<Message> messages() const;

    void clear();

    [[nodiscard]] const std::filesystem::path& path() const;

private:
    std::filesystem::path filePath_;
    mutable std::mutex mutex_;

    [[nodiscard]] std::vector<Message> readAllUnlocked() const;

    void writeAllUnlocked(const std::vector<Message>& messages) const;
};
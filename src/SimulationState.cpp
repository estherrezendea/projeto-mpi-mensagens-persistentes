#include "SimulationState.hpp"

#include "PersistentQueue.hpp"
#include "SimulationRoles.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <ctime>

namespace {
    constexpr const char* DATA_DIRECTORY = "data";

    std::filesystem::path dataPath(const std::string& fileName) {
        return std::filesystem::path(DATA_DIRECTORY) / fileName;
    }

    std::filesystem::path commandPath(const int rank) {
        return dataPath("comandos_processo_" + std::to_string(rank) + ".txt");
    }

    std::filesystem::path statusPath(const int rank) {
        return dataPath("estado_processo_" + std::to_string(rank) + ".txt");
    }

    std::filesystem::path queuePath(const int rank) {
        return dataPath("fila_processo_" + std::to_string(rank) + ".txt");
    }

    std::filesystem::path processedPath(const int rank) {
        return dataPath(
            "mensagens_processadas_" + std::to_string(rank) + ".txt"
        );
    }

    std::filesystem::path historyPath() {
        return dataPath("historico_mensagens.txt");
    }

    std::string nodeName(const int rank) {
        using namespace SimulationRoles;

        if (rank == ORIGEM) {
            return "ORIGEM";
        }

        if (rank == SERVIDOR) {
            return "SERVIDOR";
        }

        return "DESTINO";
    }

    long long nowMillis() {
        const auto now = std::chrono::system_clock::now();
        const auto millis = std::chrono::duration_cast<
            std::chrono::milliseconds
        >(now.time_since_epoch());

        return millis.count();
    }

    void writeTextFile(
        const std::filesystem::path& path,
        const std::string& content
    ) {
        const std::filesystem::path temporaryPath = path.string() + ".tmp";

        std::ofstream file(temporaryPath, std::ios::trunc);
        file << content;
        file.close();

        std::filesystem::remove(path);
        std::filesystem::rename(temporaryPath, path);
    }

    void clearFile(const std::filesystem::path& path) {
        std::ofstream file(path, std::ios::trunc);
    }

    std::vector<Message> readQueueFile(const int rank) {
        PersistentQueue queue(queuePath(rank));
        return queue.messages();
    }

    std::vector<Message> readProcessedMessages(const int rank) {
        std::ifstream file(processedPath(rank));
        std::vector<Message> messages;

        int messageId = 0;
        std::string content;

        while (file >> messageId >> std::quoted(content)) {
            Message message;
            message.id = messageId;
            message.content = content;
            messages.push_back(message);
        }

        return messages;
    }

    struct NodeStatus {
        std::string state{"ACORDADA"};
        int remainingSeconds{};
    };

    NodeStatus readStatus(const int rank) {
        std::ifstream file(statusPath(rank));
        NodeStatus status;

        file >> status.state >> status.remainingSeconds;

        if (!file) {
            status.state = "ACORDADA";
            status.remainingSeconds = 0;
        }

        return status;
    }

    void printMessageList(
        std::ostream& output,
        const std::vector<Message>& messages
    ) {
        if (messages.empty()) {
            output << "(nenhuma)\n";
            return;
        }

        for (const Message& message : messages) {
            if (message.type == MessageType::Finish) {
                continue;
            }

            output
                << '[' << message.id << "] "
                << message.content
                << '\n';
        }
    }

    std::string eventLabel(const std::string& event) {
        static const std::map<std::string, std::string> labels{
            {"CREATED", "Criada"},
            {"PERSISTED", "Persistida"},
            {"SENT_TO_SERVER", "Enviada ao Servidor"},
            {"ACK_FROM_SERVER", "ACK recebido do Servidor"},
            {"RECEIVED", "Recebida"},
            {"SERVER_PERSISTED", "Persistida"},
            {"READ_FROM_QUEUE", "Recuperada da fila persistente"},
            {"FORWARDED", "Encaminhada ao Destino"},
            {"ACK_FROM_DESTINATION", "ACK recebido do Destino"},
            {"DESTINATION_PERSISTED", "Persistida"},
            {"WAITING_PROCESSING", "Aguardando processamento"},
            {"APP_SLEEPING", "Aplicacao dormindo"},
            {"APP_AWAKE", "Aplicacao acordou"},
            {"PROCESSED", "Processada"},
            {"FINISH", "Finalizacao"}
        };

        const auto iterator = labels.find(event);

        if (iterator == labels.end()) {
            return event;
        }

        return iterator->second;
    }

    struct HistoryEvent {
        int messageId{};
        long long timestamp{};
        std::string node;
        std::string event;
        std::string content;
    };

    bool isCheckedEvent(const std::string& event) {
        return
            event != "APP_SLEEPING" &&
            event != "APP_AWAKE";
    }

    std::string timelineLabel(const HistoryEvent& event) {
        if (event.event == "APP_AWAKE") {
            return event.node + " acordou";
        }

        if (isCheckedEvent(event.event)) {
            return "✓ " + eventLabel(event.event);
        }

        return eventLabel(event.event);
    }

    std::string formatTimestamp(const long long timestamp) {
        const std::time_t seconds =
            static_cast<std::time_t>(timestamp / 1000);

        std::tm localTime{};

#if defined(_WIN32)
        localtime_s(&localTime, &seconds);
#else
        localtime_r(&seconds, &localTime);
#endif

        std::ostringstream output;
        output << std::put_time(&localTime, "%H:%M:%S");

        return output.str();
    }

    std::vector<HistoryEvent> readHistoryEvents() {
        std::ifstream file(historyPath());
        std::vector<HistoryEvent> events;

        HistoryEvent event;

        while (
            file
            >> event.messageId
            >> event.timestamp
            >> event.node
            >> event.event
            >> std::quoted(event.content)
        ) {
            events.push_back(event);
        }

        return events;
    }

    std::string commandName(const SimulationCommandType type) {
        if (type == SimulationCommandType::SendMessage) {
            return "SEND";
        }

        if (type == SimulationCommandType::Sleep) {
            return "SLEEP";
        }

        return "STOP";
    }
}

void SimulationState::prepareGlobalState() {
    std::filesystem::create_directories(DATA_DIRECTORY);
    clearFile(historyPath());
}

void SimulationState::prepareNodeState(const int rank) {
    std::filesystem::create_directories(DATA_DIRECTORY);
    clearFile(commandPath(rank));
    clearFile(processedPath(rank));
    markAwake(rank);
}

void SimulationState::appendCommand(
    const int rank,
    const SimulationCommand& command
) {
    std::ofstream file(commandPath(rank), std::ios::app);

    file
        << commandName(command.type) << ' '
        << command.messageId << ' '
        << command.seconds << ' '
        << std::quoted(command.content)
        << '\n';
}

std::vector<SimulationCommand> SimulationState::consumeCommands(
    const int rank
) {
    std::ifstream file(commandPath(rank));
    std::vector<SimulationCommand> commands;

    std::string type;

    while (file >> type) {
        SimulationCommand command;
        file
            >> command.messageId
            >> command.seconds
            >> std::quoted(command.content);

        if (!file) {
            break;
        }

        if (type == "SEND") {
            command.type = SimulationCommandType::SendMessage;
        } else if (type == "SLEEP") {
            command.type = SimulationCommandType::Sleep;
        } else {
            command.type = SimulationCommandType::Stop;
        }

        commands.push_back(command);
    }

    clearFile(commandPath(rank));

    return commands;
}

void SimulationState::markAwake(const int rank) {
    writeTextFile(statusPath(rank), "ACORDADA 0\n");
}

void SimulationState::markSleeping(
    const int rank,
    const int remainingSeconds
) {
    writeTextFile(
        statusPath(rank),
        "DORMINDO " + std::to_string(remainingSeconds) + '\n'
    );
}

void SimulationState::markStopped(const int rank) {
    writeTextFile(statusPath(rank), "ENCERRADA 0\n");
}

void SimulationState::appendProcessedMessage(
    const int rank,
    const Message& message
) {
    if (message.type == MessageType::Finish) {
        return;
    }

    std::ofstream file(processedPath(rank), std::ios::app);

    file
        << message.id << ' '
        << std::quoted(message.content)
        << '\n';
}

void SimulationState::appendHistory(
    const int messageId,
    const std::string& nodeName,
    const std::string& event,
    const std::string& content
) {
    std::ofstream file(historyPath(), std::ios::app);

    file
        << messageId << ' '
        << nowMillis() << ' '
        << nodeName << ' '
        << event << ' '
        << std::quoted(content)
        << '\n';
}

void SimulationState::printQueuesAndStatus(std::ostream& output) {
    using namespace SimulationRoles;

    output << "\n==============================\n";
    output << "ESTADO DA SIMULACAO\n";
    output << "==============================\n";

    for (const int rank : {ORIGEM, SERVIDOR, DESTINO}) {
        const NodeStatus status = readStatus(rank);

        output << "\n==============================\n";
        output << nodeName(rank) << "\n";
        output << "==============================\n\n";

        output << "Aplicacao:\n\n";

        if (status.state == "DORMINDO") {
            output
                << "DORMINDO (faltam "
                << status.remainingSeconds
                << " s)\n";
        } else {
            output << status.state << '\n';
        }

        output << "\nFila Persistente\n\n";
        printMessageList(output, readQueueFile(rank));

        output << "\nMensagens processadas\n\n";
        printMessageList(output, readProcessedMessages(rank));
    }

    output << '\n';
}

void SimulationState::printHistory(std::ostream& output) {
    const std::vector<HistoryEvent> events = readHistoryEvents();

    output << "\n=============================\n";
    output << "HISTORICO\n";
    output << "=============================\n";

    if (events.empty()) {
        output << "\n(nenhuma mensagem registrada)\n\n";
        return;
    }

    std::map<int, std::vector<HistoryEvent>> byMessage;

    for (const HistoryEvent& event : events) {
        byMessage[event.messageId].push_back(event);
    }

    for (auto& [messageId, messageEvents] : byMessage) {
        std::sort(
            messageEvents.begin(),
            messageEvents.end(),
            [](const HistoryEvent& left, const HistoryEvent& right) {
                return left.timestamp < right.timestamp;
            }
        );

        output << "\nMensagem #" << messageId << "\n";

        if (!messageEvents.front().content.empty()) {
            output << "Conteudo: " << messageEvents.front().content << "\n";
        }

        std::string currentNode;

        for (const HistoryEvent& event : messageEvents) {
            if (event.node != currentNode) {
                currentNode = event.node;
                output << "\n" << currentNode << ":\n";
            }

            output << eventLabel(event.event) << "\n";
        }

        const double totalSeconds =
            static_cast<double>(
                messageEvents.back().timestamp -
                messageEvents.front().timestamp
            ) / 1000.0;

        output
            << "\nTempo total: "
            << std::fixed
            << std::setprecision(1)
            << totalSeconds
            << " s\n";
    }

    output << '\n';
}

void SimulationState::printMessageTimeline(
    std::ostream& output,
    const int messageId
) {
    std::vector<HistoryEvent> events;

    for (const HistoryEvent& event : readHistoryEvents()) {
        if (event.messageId == messageId) {
            events.push_back(event);
        }
    }

    output << "\n========================\n";
    output << "Mensagem #" << messageId << "\n";
    output << "========================\n";

    if (events.empty()) {
        output << "\nMensagem nao encontrada no historico.\n\n";
        return;
    }

    std::sort(
        events.begin(),
        events.end(),
        [](const HistoryEvent& left, const HistoryEvent& right) {
            return left.timestamp < right.timestamp;
        }
    );

    if (!events.front().content.empty()) {
        output << "\nConteudo: " << events.front().content << "\n";
    }

    for (const HistoryEvent& event : events) {
        output
            << "\n"
            << formatTimestamp(event.timestamp)
            << "\n\n"
            << event.node
            << "\n\n"
            << timelineLabel(event)
            << "\n";
    }

    const double totalSeconds =
        static_cast<double>(
            events.back().timestamp -
            events.front().timestamp
        ) / 1000.0;

    output
        << "\nTempo total: "
        << std::fixed
        << std::setprecision(1)
        << totalSeconds
        << " s\n\n";
}

#include "SimulationConsole.hpp"

#include "SimulationRoles.hpp"
#include "SimulationState.hpp"
#include "Logger.hpp"

#include <iostream>
#include <limits>
#include <string>

SimulationConsole::SimulationConsole(std::atomic_bool& stopRequested)
    : stopRequested_(stopRequested) {
}

void SimulationConsole::run() {
    while (!stopRequested_.load()) {
        Logger::setPaused(true);
        printMenu();

        const int option = readInt("Escolha: ");
        Logger::setPaused(false);

        if (option == 1) {
            sendMessage();
        } else if (option == 2) {
            sleepProcess(SimulationRoles::ORIGEM);
        } else if (option == 3) {
            sleepProcess(SimulationRoles::SERVIDOR);
        } else if (option == 4) {
            sleepProcess(SimulationRoles::DESTINO);
        } else if (option == 5) {
            SimulationState::printQueuesAndStatus(std::cout);
        } else if (option == 6) {
            SimulationState::printHistory(std::cout);
        } else if (option == 7) {
            SimulationCommand command;
            command.type = SimulationCommandType::Stop;

            SimulationState::appendCommand(
                SimulationRoles::ORIGEM,
                command
            );

            stopRequested_.store(true);
        } else if (option == 8) {
            showMessageTimeline();
        } else {
            std::cout << "\nOpcao invalida.\n";
        }
    }
}

void SimulationConsole::printMenu() const {
    std::cout
        << "\n=========================================\n"
        << "SIMULADOR DE MENSAGENS PERSISTENTES MPI\n"
        << "=========================================\n\n"
        << "1 - Enviar nova mensagem\n"
        << "2 - Colocar Origem para dormir\n"
        << "3 - Colocar Servidor para dormir\n"
        << "4 - Colocar Destino para dormir\n"
        << "5 - Mostrar estado das filas\n"
        << "6 - Mostrar historico de mensagens\n"
        << "7 - Encerrar simulacao\n"
        << "8 - Passo a passo da mensagem\n\n";
}

void SimulationConsole::sendMessage() {
    Logger::setPaused(true);
    std::cout << "\nDigite a mensagem:\n\n> ";

    std::string content;
    std::getline(std::cin >> std::ws, content);
    Logger::setPaused(false);

    if (content.empty()) {
        std::cout << "\nMensagem vazia ignorada.\n";
        return;
    }

    SimulationCommand command;
    command.type = SimulationCommandType::SendMessage;
    command.messageId = nextMessageId_++;
    command.content = content;

    SimulationState::appendCommand(
        SimulationRoles::ORIGEM,
        command
    );

    std::cout
        << "\nMensagem #"
        << command.messageId
        << " enviada para a aplicacao da Origem.\n";
}

void SimulationConsole::sleepProcess(const int rank) {
    Logger::setPaused(true);
    const int seconds = readInt("\nQuantos segundos?\n\n> ");
    Logger::setPaused(false);

    if (seconds <= 0) {
        std::cout << "\nDuracao invalida.\n";
        return;
    }

    SimulationCommand command;
    command.type = SimulationCommandType::Sleep;
    command.seconds = seconds;

    SimulationState::appendCommand(rank, command);

    std::cout << "\nComando de sono registrado.\n";
}

void SimulationConsole::showMessageTimeline() {
    Logger::setPaused(true);
    const int messageId = readInt("\nMensagem:\n\n> ");
    Logger::setPaused(false);

    if (messageId <= 0) {
        std::cout << "\nNumero de mensagem invalido.\n";
        return;
    }

    SimulationState::printMessageTimeline(std::cout, messageId);
}

int SimulationConsole::readInt(const char* prompt) const {
    int value = 0;

    std::cout << prompt;
    std::cin >> value;

    if (!std::cin) {
        std::cin.clear();
        std::cin.ignore(
            std::numeric_limits<std::streamsize>::max(),
            '\n'
        );

        return -1;
    }

    std::cin.ignore(
        std::numeric_limits<std::streamsize>::max(),
        '\n'
    );

    return value;
}

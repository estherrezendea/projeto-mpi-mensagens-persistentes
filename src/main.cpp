#include <mpi.h>

#include "DestinationProcess.hpp"
#include "Logger.hpp"
#include "OriginProcess.hpp"
#include "PersistentQueue.hpp"
#include "ServerProcess.hpp"
#include "SimulationRoles.hpp"
#include "SimulationState.hpp"

#include <chrono>
#include <exception>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>

namespace {
    bool shouldResetQueues(int argc, char* argv[]) {
        for (int index = 1; index < argc; index++) {
            if (std::string(argv[index]) == "--reset") {
                return true;
            }
        }

        return false;
    }

    std::mt19937 createRandomGenerator(const int rank) {
        const unsigned int seed =
            static_cast<unsigned int>(
                std::chrono::steady_clock::now()
                    .time_since_epoch()
                    .count()
            ) +
            static_cast<unsigned int>(rank * 1000);

        return std::mt19937(seed);
    }

    bool validateThreadSupport(
        const int rank,
        const int providedThreadLevel
    ) {
        if (providedThreadLevel >= MPI_THREAD_SERIALIZED) {
            return true;
        }

        if (rank == SimulationRoles::ORIGEM) {
            std::cerr
                << "Erro: esta implementacao MPI nao fornece suporte suficiente a threads.\n"
                << "Nivel minimo necessario: MPI_THREAD_SERIALIZED.\n";
        }

        return false;
    }

    bool validateProcessCount(
        const int rank,
        const int processCount
    ) {
        if (processCount == 3) {
            return true;
        }

        if (rank == SimulationRoles::ORIGEM) {
            std::cerr
                << "Erro: execute com exatamente 3 processos.\n"
                << "Exemplo: mpirun -np 3 ./build/simulador_mpi --reset\n";
        }

        return false;
    }

    void runProcessByRank(
        const int rank,
        PersistentQueue& queue,
        std::mt19937 randomGenerator
    ) {
        using namespace SimulationRoles;

        if (rank == ORIGEM) {
            runOrigin(rank, queue, randomGenerator);
        } else if (rank == SERVIDOR) {
            runServer(rank, queue, randomGenerator);
        } else if (rank == DESTINO) {
            runDestination(rank, queue, randomGenerator);
        }
    }
}

int main(int argc, char* argv[]) {
    int providedThreadLevel = 0;

    MPI_Init_thread(
        &argc,
        &argv,
        MPI_THREAD_SERIALIZED,
        &providedThreadLevel
    );

    int rank = 0;
    int processCount = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processCount);

    if (
        !validateThreadSupport(rank, providedThreadLevel) ||
        !validateProcessCount(rank, processCount)
    ) {
        MPI_Finalize();
        return 1;
    }

    try {
        if (rank == SimulationRoles::ORIGEM) {
            SimulationState::prepareGlobalState();
        }

        MPI_Barrier(MPI_COMM_WORLD);

        const std::filesystem::path queuePath =
            "data/fila_processo_" + std::to_string(rank) + ".txt";

        PersistentQueue queue(queuePath);
        SimulationState::prepareNodeState(rank);

        if (shouldResetQueues(argc, argv)) {
            queue.clear();

            Logger::info(
                rank,
                "FILA",
                "Fila persistente limpa para uma nova simulacao."
            );
        }

        MPI_Barrier(MPI_COMM_WORLD);

        runProcessByRank(
            rank,
            queue,
            createRandomGenerator(rank)
        );

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
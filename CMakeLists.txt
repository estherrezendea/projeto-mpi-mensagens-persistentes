cmake_minimum_required(VERSION 3.20)

project(MensagensPersistentesMPI VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(MPI REQUIRED)
find_package(Threads REQUIRED)

add_executable(simulador_mpi
    src/main.cpp
    src/PersistentQueue.cpp
    src/MpiMessageTransport.cpp
)

target_include_directories(simulador_mpi PRIVATE include)

target_link_libraries(simulador_mpi
    PRIVATE
    MPI::MPI_CXX
    Threads::Threads
)
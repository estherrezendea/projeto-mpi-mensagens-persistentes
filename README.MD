````md
# Simulação de Mensagens Persistentes com MPI

## 1. Objetivo

Este projeto implementa uma simulação de comunicação orientada a mensagens utilizando MPI em C++.

A simulação possui três processos:

- **Processo 0 — Origem**
- **Processo 1 — Servidor / Middleware**
- **Processo 2 — Destino**

Cada processo possui uma fila persistente própria, armazenada em arquivo. As mensagens são enviadas entre os processos com MPI e só são removidas da fila do processo remetente após a confirmação de que foram persistidas no próximo processo.

O projeto também simula atrasos aleatórios em cada processo. Cada processo possui uma thread de aplicação, onde ocorrem os `sleep`, e uma thread de rede, dedicada ao MPI. Assim, mesmo quando a aplicação entra em espera, a rede continua livre para receber mensagens, persistir em disco e enviar ACKs.

---

## 2. Arquitetura da Simulação

```text
                    MPI Isend / Irecv
       ┌──────────────────────────────────────┐
       │                                      ▼
┌──────────────┐                       ┌──────────────┐
│   ORIGEM     │                       │   SERVIDOR   │
│ Processo 0   │ ────────────────────> │ Processo 1   │
│              │                       │ Middleware   │
│ fila 0.txt   │ <──── ACK ─────────── │ fila 1.txt   │
└──────────────┘                       └──────────────┘
                                              │
                                              │ MPI Isend / Irecv
                                              ▼
                                       ┌──────────────┐
                                       │   DESTINO    │
                                       │ Processo 2   │
                                       │ fila 2.txt   │
                                       └──────────────┘
                                              │
                                              └──── ACK para o Servidor
````

Arquivos de persistência:

```text
data/
├── fila_processo_0.txt  -> Fila persistente da Origem
├── fila_processo_1.txt  -> Fila persistente do Servidor
└── fila_processo_2.txt  -> Fila persistente do Destino
```

---

## 3. Fluxo das Mensagens

### 3.1 Origem

A Origem:

1. Cria mensagens de demonstração.
2. Persiste todas as mensagens na própria fila.
3. Dorme por um intervalo aleatório antes de enviar cada mensagem.
4. Recupera a primeira mensagem da fila.
5. Inicia o envio assíncrono para o Servidor.
6. Aguarda o ACK do Servidor.
7. Remove a mensagem da própria fila somente após receber o ACK.

### 3.2 Servidor

O Servidor:

1. Recebe a mensagem enviada pela Origem.
2. Persiste a mensagem em sua própria fila.
3. Envia um ACK para a Origem.
4. Dorme por um intervalo aleatório.
5. Recupera a mensagem diretamente da fila persistente em arquivo.
6. Encaminha a mensagem recuperada para o Destino.
7. Aguarda o ACK do Destino.
8. Remove a mensagem de sua fila somente após receber o ACK.

O Servidor não apenas repassa a mensagem que acabou de chegar pela memória. Ele primeiro a persiste em arquivo e depois a recupera da própria fila antes de encaminhá-la.

### 3.3 Destino

O Destino:

1. Recebe a mensagem enviada pelo Servidor.
2. Persiste a mensagem na própria fila.
3. Envia um ACK para o Servidor.
4. Dorme por um intervalo aleatório.
5. Recupera a mensagem da fila persistente.
6. Processa a mensagem.
7. Remove a mensagem da fila.

---

## 4. Persistência e Confirmação

A persistência é simulada com arquivos `.txt`.

Cada mensagem permanece no arquivo de fila até que exista a confirmação de persistência no próximo processo.

Exemplo:

```text
Origem persiste mensagem
        ↓
Origem envia ao Servidor
        ↓
Servidor persiste mensagem
        ↓
Servidor envia ACK
        ↓
Origem remove mensagem da própria fila
```

O mesmo mecanismo é usado entre Servidor e Destino.

Essa abordagem evita que uma mensagem seja removida da fila antes de existir confirmação de que ela foi registrada no próximo processo.

---

## 5. Comunicação Assíncrona e Threads

A aplicação inicializa o MPI com suporte a threads:

```cpp
MPI_Init_thread(..., MPI_THREAD_SERIALIZED, ...)
```

Depois disso, cada processo executa duas threads:

```text
Thread de aplicação -> simula indisponibilidade, processamento e sleep
Thread de rede      -> executa MPI_Iprobe, MPI_Isend, MPI_Irecv e MPI_Test
```

A comunicação utiliza as primitivas não bloqueantes:

```cpp
MPI_Isend(...)
MPI_Irecv(...)
MPI_Iprobe(...)
MPI_Test(...)
```

A thread de aplicação pode dormir por alguns segundos sem impedir que a thread de rede receba mensagens, grave em arquivo e envie confirmações.

O Servidor mantém a regra principal do trabalho: ele recebe a mensagem da Origem, grava em disco, envia ACK para a Origem, lê a mensagem da fila persistente e só então encaminha ao Destino.

---

## 6. Mensagens Demonstrativas

A Origem cria cinco mensagens de demonstração:

```text
1. Solicitação de processamento de imagem LT-102
2. Atualização cadastral do ativo AT-045
3. Relatório de inspeção da torre T-883
4. Alerta de vegetação próxima à rede
5. Solicitação de reprocessamento de análise
```

Também é enviada uma mensagem especial de finalização:

```text
FIM_DA_SIMULACAO
```

Ela permite que Origem, Servidor e Destino encerrem corretamente sem permanecer bloqueados esperando novas mensagens.

---

## 7. Pré-requisitos

É necessário possuir:

* Compilador C++ com suporte a C++17;
* CMake;
* OpenMPI ou MPICH;
* `mpicxx`;
* `mpirun`.

No Ubuntu, a instalação pode ser feita com:

```bash
sudo apt update
sudo apt install build-essential cmake openmpi-bin libopenmpi-dev
```

---

## 8. Compilação

Na raiz do projeto, execute:

```bash
cmake -S . -B build
cmake --build build
```

---

## 9. Execução

Para iniciar uma nova simulação e limpar as filas anteriores:

```bash
mpirun -np 3 ./build/simulador_mpi --reset
```

O projeto exige exatamente três processos:

```text
Processo 0 -> Origem
Processo 1 -> Servidor
Processo 2 -> Destino
```

---

## 10. Exemplo de Saída Esperada

Durante a execução, devem aparecer mensagens semelhantes a estas:

```text
[Processo 0] [ORIGEM] Mensagem #1 persistida na fila local.
[Processo 0] [ORIGEM] Entrando em espera aleatória por 2 segundo(s).

[Processo 1] [SERVIDOR] Mensagem #1 recebida e persistida.
[Processo 1] [SERVIDOR] Mensagem #1 recuperada da fila persistente antes do encaminhamento.

[Processo 2] [DESTINO] Mensagem #1 recebida e persistida.
[Processo 2] [DESTINO] Mensagem #1 processada com sucesso.
```

A ordem global dos logs pode mudar a cada execução, pois os três processos executam concorrentemente.

---

## 11. Estrutura do Projeto

```text
projeto-mpi-mensagens-persistentes/
├── CMakeLists.txt
├── README.md
├── data/
│   ├── fila_processo_0.txt
│   ├── fila_processo_1.txt
│   └── fila_processo_2.txt
├── include/
│   ├── DestinationProcess.hpp
│   ├── Logger.hpp
│   ├── Message.hpp
│   ├── MpiMessageTransport.hpp
│   ├── OriginProcess.hpp
│   ├── PersistentQueue.hpp
│   ├── ServerProcess.hpp
│   ├── SimulationRoles.hpp
│   ├── SimulationSupport.hpp
│   └── SleepSimulator.hpp
└── src/
    ├── DestinationProcess.cpp
    ├── main.cpp
    ├── MpiMessageTransport.cpp
    ├── OriginProcess.cpp
    ├── PersistentQueue.cpp
    ├── ServerProcess.cpp
    ├── SimulationSupport.cpp
    └── SleepSimulator.cpp
```

---

## 12. Requisitos Atendidos

| Requisito                            | Implementação                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------------- |
| Processo de origem                   | Processo MPI de rank 0                                                        |
| Processo servidor                    | Processo MPI de rank 1                                                        |
| Processo destino                     | Processo MPI de rank 2                                                        |
| Fila persistente em cada processo    | Arquivos `fila_processo_0.txt`, `fila_processo_1.txt` e `fila_processo_2.txt` |
| Mensagens persistentes               | Cada mensagem é gravada em arquivo antes de ser encaminhada ou processada     |
| Sono aleatório                       | A thread de aplicação de cada processo dorme de forma aleatória               |
| Mensagens chegam ao destino          | A thread de rede continua ativa mesmo quando a aplicação está dormindo        |
| Uso de MPI                           | Comunicação com `MPI_Isend`, `MPI_Irecv`, `MPI_Iprobe` e `MPI_Test`           |
| Recuperação do arquivo pelo servidor | Servidor utiliza a fila persistente antes de encaminhar a mensagem            |
| Demonstração de funcionamento        | Cinco mensagens de texto e uma mensagem de finalização                        |

```

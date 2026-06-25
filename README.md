# TP Sistemas Distribuidos: Money Laundering Analysis

## Integrantes

| Nombre | Padrón |
|---|---|
| Bautista Boeri | 110898 |
| Luciana Larrosa | 110476 |
| Carolina Racedo | 110550 |

## Requisitos

- Docker y Docker Compose
- Python 3.x

## Ejecución

### Configuración

El sistema se configura mediante variables de Makefile. Los valores por defecto son:

| Variable | Default | Descripción |
|---|---|---|
| `N_CLIENTS` | 2 | Cantidad de clientes |
| `N_GATEWAYS` | 2 | Cantidad de gateways |
| `REPLICAS` | 3 | Réplicas base para workers |
| `COMPOSE_FILE` | `docker-compose.yaml` | Archivo de compose generado |
| `DATASET_DIR` | `./data` | Directorio del dataset |
| `TRANSACTIONS_FILE` | `HI-Small_Trans.csv` | Nombre del archivo de transacciones |
| `ACCOUNTS_FILE` | `HI-Small_accounts.csv` | Nombre del archivo de cuentas |
| `OUTPUT_DIR` | `./output` | Directorio de resultados del sistema |
| `EXPECTED_DIR` | `./expected_output` | Directorio de resultados esperados para correctitud |

> Cada worker puede configurarse individualmente sobrescribiendo su variable. Por defecto todos toman el valor de `REPLICAS`, excepto `LOW_AMOUNT_REDUCERS` que es 1.

### Comandos

| Comando | Descripción |
|---|---|
| `make compose` | Genera el archivo de compose |
| `make build` | Construye las imágenes |
| `make up` | Levanta el sistema |
| `make down` | Detiene el sistema |
| `make logs` | Muestra los logs de todos los servicios |
| `make logs SERVICE=<service>` | Muestra los logs de un servicio específico |
| `make clean` | Elimina contenedores, imágenes locales y directorios de salida |
| `make clean-all` | Elimina contenedores, imágenes locales, directorios de salida y recursos no utilizados de Docker |
| `make verify-output` | Verifica la correctitud de la salida sin levantar el sistema |
| `make output-test` | Levanta el sistema, espera a que todos los clientes terminen y verifica la correctitud de la salida |
| `make` (o `make all`) | Ejecuta el flujo completo |

---

## Ejecución con Chaos Monkey

### Configuración

> Una ronda de *chaos* consiste en matar `CHAOS_KILLS_PER_ROUND` nodos no protegidos aleatorios dejando siempre al menos `CHAOS_WATCHDOG_FLOOR` watchdogs vivos.

| Variable | Default | Descripción |
|---|---|---|
| `CHAOS_INTERVAL` | 30 | Segundos entre rondas |
| `CHAOS_KILLS_PER_ROUND` | 3 | Nodos a matar por ronda |
| `CHAOS_WATCHDOG_FLOOR` | 1 | Mínimo de watchdogs vivos |
| `CHAOS_INJECT_START_ROUND` | 3 | Ronda a partir de la cual inyectar clientes dinámicos |
| `CHAOS_INJECT_CLIENT_COUNT` | 3 | Cantidad de clientes dinámicos a inyectar |
| `CHAOS_INJECT_DATASET_SIZE` | `Small` | Tamaño del dataset para clientes dinámicos (`Small`, `Medium`, `Large`) |
| `PROTECTED_PREFIXES` | `rabbitmq proxy client` | Prefijos de nodos protegidos |

### Comandos

| Comando | Descripción |
|---|---|
| `make chaos-monkey` | Ejecuta rondas de *chaos* hasta que todos los clientes terminen, inyectando clientes dinámicos a partir de una ronda específica |
| `make chaos-monkey-cli` | Permite controlar el chaos manualmente: ejecutar rondas, inyectar clientes, matar todos los nodos no protegidos o matar un nodo específico |
| `make chaos-output-test` | Levanta el sistema, genera fallos con Chaos Monkey, espera a que todos los clientes terminen y verifica la correctitud |
| `make chaos-cli-output-test` | Levanta el sistema, permite controlar el chaos manualmente con Chaos Monkey CLI, espera a que todos los clientes terminen y verifica la correctitud |
| `make chaos-all` | Ejecuta el flujo completo con Chaos Monkey |
| `make chaos-cli-all` | Ejecuta el flujo completo con Chaos Monkey CLI |
| `make volume-view NODE=<nombre>` | Muestra el volumen persistido de un worker específico |
| `make volume-cli` | Explorador interactivo de volúmenes de workers |

# TP: Money Laundering Analysis

## Integrantes

| Nombre | Padrón |
|---|---|
| Bautista Boeri | 110898 |
| Luciana Larrosa | 110476 |
| Carolina Racedo | 110550 |

## Requisitos

- Docker y Docker Compose
- Python 3.x

## Configuración

El sistema se configura mediante variables de Makefile. Los valores por defecto son:

| Variable | Default | Descripcion |
|---|---|---|
| `N_CLIENTS` | 2 | Cantidad de clientes |
| `N_GATEWAYS` | 2 | Cantidad de gateways |
| `REPLICAS` | 3 | Replicas base para workers |
| `COMPOSE_FILE` | `docker-compose.yaml` | Archivo de compose generado |
| `DATASET_DIR` | `./data` | Directorio del dataset |
| `TRANSACTIONS_FILE` | `HI-Small_Trans.csv` | Nombre del archivo de transacciones |
| `ACCOUNTS_FILE` | `HI-Small_accounts.csv` | Nombre del archivo de cuentas |
| `OUTPUT_DIR` | `./output` | Directorio de resultados del sistema |
| `EXPECTED_DIR` | `./expected_output` | Directorio de resultados esperados para correctitud |
| `SLEEP_TIME` | 30 | Segundos de espera para el exit test |

Cada worker puede configurarse individualmente sobrescribiendo su variable. Por defecto todos toman el valor de `REPLICAS`, excepto `LOW_AMOUNT_REDUCERS` que es 1.

## Ejecución

### Generar el archivo de compose

```bash
make compose
```

O con parametros personalizados:

```bash
make compose REPLICAS=5 N_CLIENTS=3 N_GATEWAYS=2 ...
```

### Construir las imagenes

```bash
make build
```

### Levantar el sistema

```bash
make up
```

### Ver logs

```bash
make logs
```

### Detener el sistema

```bash
make down
```

### Limpieza

Baja contenedores, elimina imagenes locales y directorios de salida:

```bash
make clean
```

O con `docker system prune` para eliminar los recursos no utilizados:

```bash
make clean-all
```

---

### Prueba de correctitud

Levanta el sistema (`make up`), espera a que los clientes terminen (`make wait-clients`), genera la salida esperada mediante una ejecución serial (`make build-expected`) y verifica que los resultados coincidan (`make verify-output`):

```bash
make output-test
```

### Prueba de graceful shutdown

Levanta el sistema, lo detiene luego de `SLEEP_TIME` segundos (`make up-and-stop`) y verifica que todos los contenedores hayan registrado un _graceful shutdown_ (`make verify-shutdown`) y que al menos uno haya terminado con código 0 (`make verify-exit-codes`):

```bash
make exit-test
```

---

### Ejecutar todo el flujo

```bash
make
```

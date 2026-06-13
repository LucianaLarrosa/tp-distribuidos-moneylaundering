import argparse

import yaml

# --- Configuration ---

DEFAULT_CLIENTS = 1
DEFAULT_GATEWAYS = 1
DEFAULT_REPLICAS = 3
LOW_AMOUNT_REDUCERS = 1

# --- Middleware Configuration ---

EXCHANGE_RAW_DATA = "raw_data"
EXCHANGE_FILTERED_TRANSACTIONS = "filtered_transactions"
EXCHANGE_DATE_FILTER_OUTPUT = "date_filter_output"
EXCHANGE_BANK_CATALOG = "bank_catalog"
EXCHANGE_BANK_MAX_OUTPUT = "bank_max_output"
EXCHANGE_BANK_MAX_SHARDED = "bank_max_sharded"
EXCHANGE_PAYMENT_FORMAT_SHARDS = "payment_format_shards"
EXCHANGE_PAYMENT_FORMAT_AVERAGES = "payment_format_averages"
EXCHANGE_BIDIRECTIONAL_SHARDER_OUTPUT = "bidirectional_sharder_output"
EXCHANGE_ACCOUNT_FREQ_FILTER_OUTPUT = "account_freq_filter_output"
EXCHANGE_PATH_MAPPER_OUTPUT = "path_mapper_output"
EXCHANGE_PATH_FREQ_FILTER_OUTPUT = "path_freq_filter_output"
EXCHANGE_QUERY_RESULTS = "query_results"
QUEUE_CURRENCY_MAPPER_INPUT = "currency_mapper_input"
QUEUE_CURRENCY_MAPPER_OUTPUT = "currency_mapper_output"
QUEUE_LOW_AMOUNT_AGGREGATOR_OUTPUT = "low_amount_aggregator_output"
QUEUE_BANK_MAX_RESULTS = "bank_max_results"

# --- Watchdog / Health Configuration ---

PING_PONG_HOST = "0.0.0.0"
PING_PORT = "9001"
PONG_PORT = "9000"
PING_TIMEOUT_SECONDS = "2"
CHECK_INTERVAL_SECONDS = "5"
MAX_RETRIES = "3"
ELECTION_PORT = "9002"
ELECTION_TIMEOUT_SECONDS = "5"
LEADER_PROBE_MISS_THRESHOLD = "3"
DEFAULT_PROTECTED_PREFIXES = "rabbitmq gateway proxy client"

# --- Query IDs ---

QUERY_1_ID = 1
QUERY_2_ID = 2
QUERY_3_ID = 3
QUERY_4_ID = 4
QUERY_5_ID = 5

# --- Constants ---

NODE_PREFIX = "node."
MIN_REQUIRED_ACCOUNTS = "5"
TRANSACTION_DATE_FORMAT = "%Y/%m/%d %H:%M"
DATE_FROM_1 = "2022/09/01 00:00"
DATE_TO_1 = "2022/09/05 23:59"
DATE_FROM_2 = "2022/09/06 00:00"
DATE_TO_2 = "2022/09/15 23:59"
USD_CURRENCY = "US Dollar"
ROUTING_KEY_USD = "usd"
ROUTING_KEYS_ALL = "all"
ROUTING_KEYS_EOF = "eof"
ROUTING_KEY_PERIOD_1 = "period1"
ROUTING_KEY_PERIOD_2 = "period2"
ROUTING_KEY_TRANSACTION = "transaction"
ROUTING_KEY_ACCOUNT = "account"

# --- Batch Size ---

TRANSACTIONS_BATCH_SIZE = "1215"
ACCOUNTS_BATCH_SIZE = "1340"
BANK_MAX_PARTIAL_BATCH_SIZE = "3800"
PAYMENT_FORMAT_PARTIAL_BATCH_SIZE = "4200"
ACCOUNT_EDGE_BATCH_SIZE = "2680"
PATH_BATCH_SIZE = "1900"
Q4_RESULT_BATCH_SIZE = "5360"

# --- Containers ---


def _rabbitmq():
    return {
        "build": "./src/rabbitmq",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "environment": {
            "RABBITMQ_DEFAULT_USER": "guest",
            "RABBITMQ_DEFAULT_PASS": "guest",
        },
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"],
            "interval": "5s",
            "timeout": "10s",
            "retries": 15,
            "start_period": "10s",
        },
    }


def _gateway(i, transactions_field_mappers, accounts_field_mappers):
    return {
        "build": {"context": ".", "dockerfile": "src/gateway/Dockerfile"},
        "container_name": f"gateway_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"transactions_field_mapper_{j}": {"condition": "service_started"}
                for j in range(transactions_field_mappers)
            },
            **{
                f"accounts_field_mapper_{j}": {"condition": "service_started"}
                for j in range(accounts_field_mappers)
            },
        },
        "ports": [f"{5000 + i - 1}:5000"],
        "environment": {
            "GATEWAY_HOST": "0.0.0.0",
            "GATEWAY_PORT": "5000",
            "POOL_SIZE": "4",
            "RABBITMQ_HOST": "rabbitmq",
            "RAW_DATA_EXCHANGE": EXCHANGE_RAW_DATA,
            "TRANSACTION_ROUTING_KEY": ROUTING_KEY_TRANSACTION,
            "ACCOUNT_ROUTING_KEY": ROUTING_KEY_ACCOUNT,
            "QUERY_RESULTS_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "EXPECTED_QUERY_IDS": f"{QUERY_1_ID},{QUERY_2_ID},{QUERY_3_ID},{QUERY_4_ID},{QUERY_5_ID}",
        },
    }


def _proxy(gateways):
    gateway_hosts = ",".join(f"gateway_{i}" for i in range(1, gateways + 1))
    return {
        "build": {"context": ".", "dockerfile": "src/proxy/Dockerfile"},
        "container_name": "proxy",
        "depends_on": {
            f"gateway_{i}": {"condition": "service_started"}
            for i in range(1, gateways + 1)
        },
        "ports": ["6000:6000"],
        "environment": {
            "PROXY_HOST": "0.0.0.0",
            "PROXY_PORT": "6000",
            "GATEWAY_HOSTS": gateway_hosts,
            "GATEWAY_PORT": "5000",
        },
    }


def _client(i):
    return {
        "build": {"context": ".", "dockerfile": "src/client/Dockerfile"},
        "container_name": f"client_{i}",
        "depends_on": {"proxy": {"condition": "service_started"}},
        "volumes": [
            "${DATASET_DIR:-./data}:/data:ro",
            "${OUTPUT_DIR:-./output}:/output",
        ],
        "environment": {
            "PROXY_HOST": "proxy",
            "PROXY_PORT": "6000",
            "INPUT_CSV_TRANSACTIONS": "/data/${TRANSACTIONS_FILE:-HI-Small_Trans.csv}",
            "INPUT_CSV_ACCOUNTS": "/data/${ACCOUNTS_FILE:-HI-Small_accounts.csv}",
            "TRANSACTIONS_BATCH_SIZE": TRANSACTIONS_BATCH_SIZE,
            "ACCOUNTS_BATCH_SIZE": ACCOUNTS_BATCH_SIZE,
            "EXPECTED_QUERY_IDS": f"{QUERY_1_ID},{QUERY_2_ID},{QUERY_3_ID},{QUERY_4_ID},{QUERY_5_ID}",
            "OUTPUT_DIR": "/output",
            "CLIENT_ID": str(i),
        },
    }


def _transactions_field_mapper(i, date_filters, bank_max_aggregators, amount_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/transactions_field_mapper/Dockerfile",
        },
        "container_name": f"transactions_field_mapper_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"date_filter_{j}": {"condition": "service_started"}
                for j in range(date_filters)
            },
            **{
                f"bank_max_aggregator_{j}": {"condition": "service_started"}
                for j in range(bank_max_aggregators)
            },
            **{
                f"amount_filter_{j}": {"condition": "service_started"}
                for j in range(amount_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "RAW_DATA_EXCHANGE": EXCHANGE_RAW_DATA,
            "INPUT_ROUTING_KEY": ROUTING_KEY_TRANSACTION,
            "INPUT_QUEUE_NAME": "transactions_field_mapper_input",
            "OUTPUT_EXCHANGE": EXCHANGE_FILTERED_TRANSACTIONS,
            "OUTPUT_ROUTING_KEY_USD": ROUTING_KEY_USD,
            "OUTPUT_ROUTING_KEY_ALL": ROUTING_KEYS_ALL,
            "OUTPUT_ROUTING_KEY_EOF": ROUTING_KEYS_EOF,
            "USD_CURRENCY": USD_CURRENCY,
        },
    }


def _date_filter(
    i,
    payment_format_filters,
    payment_format_aggregators,
    anomaly_filters,
    bidirectional_sharders,
):
    return {
        "build": {"context": ".", "dockerfile": "src/workers/date_filter/Dockerfile"},
        "container_name": f"date_filter_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"payment_format_filter_{j}": {"condition": "service_started"}
                for j in range(payment_format_filters)
            },
            **{
                f"payment_format_aggregator_{j}": {"condition": "service_started"}
                for j in range(payment_format_aggregators)
            },
            **{
                f"anomaly_filter_{j}": {"condition": "service_started"}
                for j in range(anomaly_filters)
            },
            **{
                f"bidirectional_sharder_{j}": {"condition": "service_started"}
                for j in range(bidirectional_sharders)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_FILTERED_TRANSACTIONS,
            "INPUT_ROUTING_KEY": f"{ROUTING_KEYS_ALL},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE_NAME": "date_filter_input",
            "OUTPUT_EXCHANGE": EXCHANGE_DATE_FILTER_OUTPUT,
            "DATE_FORMAT": TRANSACTION_DATE_FORMAT,
            "DATE_FROM_1": DATE_FROM_1,
            "DATE_TO_1": DATE_TO_1,
            "DATE_FROM_2": DATE_FROM_2,
            "DATE_TO_2": DATE_TO_2,
            "USD_CURRENCY": USD_CURRENCY,
            "OUTPUT_ROUTING_KEY_USD": ROUTING_KEY_USD,
            "OUTPUT_ROUTING_KEY_ALL": ROUTING_KEYS_ALL,
            "OUTPUT_ROUTING_KEY_PERIOD_1": ROUTING_KEY_PERIOD_1,
            "OUTPUT_ROUTING_KEY_PERIOD_2": ROUTING_KEY_PERIOD_2,
            "OUTPUT_ROUTING_KEY_EOF": ROUTING_KEYS_EOF,
        },
    }


def _payment_format_filter(i, currency_mappers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/payment_format_filter/Dockerfile",
        },
        "container_name": f"payment_format_filter_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"currency_mapper_{j}": {"condition": "service_started"}
                for j in range(currency_mappers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_DATE_FILTER_OUTPUT,
            "INPUT_ROUTING_KEY": f"{ROUTING_KEYS_ALL}.{ROUTING_KEY_PERIOD_1},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE_NAME": "payment_format_filter_input",
            "OUTPUT_QUEUE": QUEUE_CURRENCY_MAPPER_INPUT,
            "VALID_PAYMENT_FORMATS": "Wire,ACH",
            "USD_CURRENCY": USD_CURRENCY,
        },
    }


def _currency_mapper(i, low_amount_aggregators):
    laa_deps = {
        f"low_amount_aggregator_{j}": {"condition": "service_started"}
        for j in range(low_amount_aggregators)
    }
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/currency_mapper/Dockerfile",
        },
        "container_name": f"currency_mapper_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}, **laa_deps},
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_QUEUE": QUEUE_CURRENCY_MAPPER_INPUT,
            "OUTPUT_QUEUE": QUEUE_CURRENCY_MAPPER_OUTPUT,
            "TARGET_CURRENCY": USD_CURRENCY,
            "FRANKFURTER_URL": "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD",
            "FRANKFURTER_TIMEOUT_SECONDS": "10",
            "RATES_DATE_FIELD": "date",
            "RATES_QUOTE_FIELD": "quote",
            "RATES_RATE_FIELD": "rate",
            "RATES_DATE_FORMAT": "%Y-%m-%d",
            "TRANSACTION_DATE_FORMAT": TRANSACTION_DATE_FORMAT,
        },
    }


def _low_amount_aggregator(i, low_amount_aggregators, low_amount_reducers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/low_amount_aggregator/Dockerfile",
        },
        "container_name": f"low_amount_aggregator_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"low_amount_reducer_{j}": {"condition": "service_started"}
                for j in range(low_amount_reducers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_QUEUE": QUEUE_CURRENCY_MAPPER_OUTPUT,
            "OUTPUT_QUEUE": QUEUE_LOW_AMOUNT_AGGREGATOR_OUTPUT,
            "CONTROL_EXCHANGE": "low_amount_aggregator_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(low_amount_aggregators),
            "AMOUNT_THRESHOLD": "1.0",
        },
    }


def _low_amount_reducer(i, low_amount_reducers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/low_amount_reducer/Dockerfile",
        },
        "container_name": f"low_amount_reducer_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_QUEUE": QUEUE_LOW_AMOUNT_AGGREGATOR_OUTPUT,
            "OUTPUT_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "QUERY_ID": str(QUERY_5_ID),
            "CONTROL_EXCHANGE": "low_amount_reducer_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(low_amount_reducers),
        },
    }


def _accounts_field_mapper(i, bank_mappers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/accounts_field_mapper/Dockerfile",
        },
        "container_name": f"accounts_field_mapper_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"bank_mapper_{j}": {"condition": "service_started"}
                for j in range(bank_mappers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "RAW_DATA_EXCHANGE": EXCHANGE_RAW_DATA,
            "INPUT_ROUTING_KEY": ROUTING_KEY_ACCOUNT,
            "INPUT_QUEUE_NAME": "accounts_field_mapper_input",
            "OUTPUT_EXCHANGE": EXCHANGE_BANK_CATALOG,
            "OUTPUT_ROUTING_KEYS": ",".join(str(j) for j in range(bank_mappers)),
            "OUTPUT_NODE_COUNT": str(bank_mappers),
            "OUTPUT_NODE_PREFIX": "bank_mapper_side_input_node_",
        },
    }


def _bank_max_aggregator(i, bank_max_aggregators, bank_max_reducers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/bank_max_aggregator/Dockerfile",
        },
        "container_name": f"bank_max_aggregator_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"bank_max_reducer_{j}": {"condition": "service_started"}
                for j in range(bank_max_reducers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_FILTERED_TRANSACTIONS,
            "INPUT_BINDING_PATTERNS": f"{ROUTING_KEY_USD},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE": "bank_max_input",
            "OUTPUT_EXCHANGE": EXCHANGE_BANK_MAX_OUTPUT,
            "CONTROL_EXCHANGE": "bank_max_aggregator_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_max_aggregators),
            "BATCH_SIZE": BANK_MAX_PARTIAL_BATCH_SIZE,
            "NUM_SHARDS": str(bank_max_reducers),
        },
    }


def _bank_max_reducer(i, bank_max_reducers, bank_mappers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/bank_max_reducer/Dockerfile",
        },
        "container_name": f"bank_max_reducer_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"bank_mapper_{j}": {"condition": "service_started"}
                for j in range(bank_mappers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_BANK_MAX_OUTPUT,
            "SHARD_ID": str(i),
            "OUTPUT_EXCHANGE": EXCHANGE_BANK_MAX_SHARDED,
            "OUTPUT_ROUTING_KEYS": ",".join(str(j) for j in range(bank_mappers)),
            "OUTPUT_NODE_COUNT": str(bank_mappers),
            "CONTROL_EXCHANGE": "bank_max_reducer_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_max_reducers),
            "BATCH_SIZE": BANK_MAX_PARTIAL_BATCH_SIZE,
        },
    }


def _amount_filter(i):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/amount_filter/Dockerfile",
        },
        "container_name": f"amount_filter_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_FILTERED_TRANSACTIONS,
            "INPUT_ROUTING_KEY": ROUTING_KEY_USD,
            "INPUT_EOF_ROUTING_KEY": ROUTING_KEYS_EOF,
            "INPUT_QUEUE_NAME": "amount_filter_input",
            "OUTPUT_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "QUERY_ID": str(QUERY_1_ID),
            "AMOUNT_THRESHOLD": "50.0",
        },
    }


def _bank_mapper(i, bank_mappers):
    return {
        "build": {"context": ".", "dockerfile": "src/workers/bank_mapper/Dockerfile"},
        "container_name": f"bank_mapper_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "volumes": [f"bank_mapper_spill_{i}:/tmp/bank_mapper"],
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_BANK_MAX_SHARDED,
            "SHARD_ID": str(i),
            "OUTPUT_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "QUERY_ID": str(QUERY_2_ID),
            "BANKS_EXCHANGE": EXCHANGE_BANK_CATALOG,
            "SIDE_INPUT_NODE_PREFIX": "bank_mapper_side_input_node_",
            "CONTROL_EXCHANGE": "bank_mapper_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_mappers),
            "SPILL_DIR": "/tmp/bank_mapper",
        },
    }


def _payment_format_aggregator(i, payment_format_aggregators, payment_format_reducers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/payment_format_aggregator/Dockerfile",
        },
        "container_name": f"payment_format_aggregator_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"payment_format_reducer_{j}": {"condition": "service_started"}
                for j in range(payment_format_reducers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_DATE_FILTER_OUTPUT,
            "INPUT_BINDING_PATTERNS": f"{ROUTING_KEY_USD}.{ROUTING_KEY_PERIOD_1},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE": "payment_format_aggregator_input",
            "OUTPUT_EXCHANGE": EXCHANGE_PAYMENT_FORMAT_SHARDS,
            "CONTROL_EXCHANGE": "payment_format_aggregator_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(payment_format_aggregators),
            "BATCH_SIZE": PAYMENT_FORMAT_PARTIAL_BATCH_SIZE,
            "NUM_SHARDS": str(payment_format_reducers),
        },
    }


def _payment_format_reducer(i, payment_format_reducers, anomaly_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/payment_format_reducer/Dockerfile",
        },
        "container_name": f"payment_format_reducer_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"anomaly_filter_{j}": {"condition": "service_started"}
                for j in range(anomaly_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_PAYMENT_FORMAT_SHARDS,
            "SHARD_ID": str(i),
            "OUTPUT_EXCHANGE": EXCHANGE_PAYMENT_FORMAT_AVERAGES,
            "CONTROL_EXCHANGE": "payment_format_reducer_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(payment_format_reducers),
        },
    }


def _anomaly_filter(i, anomaly_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/anomaly_filter/Dockerfile",
        },
        "container_name": f"anomaly_filter_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "volumes": [f"anomaly_filter_spill_{i}:/tmp/anomaly_filter"],
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_DATE_FILTER_OUTPUT,
            "INPUT_ROUTING_KEYS": f"{ROUTING_KEY_USD}.{ROUTING_KEY_PERIOD_2},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE_NAME": "anomaly_filter_input",
            "AVG_EXCHANGE": EXCHANGE_PAYMENT_FORMAT_AVERAGES,
            "OUTPUT_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "QUERY_ID": str(QUERY_3_ID),
            "CONTROL_EXCHANGE": "anomaly_filter_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(anomaly_filters),
            "SPILL_DIR": "/tmp/anomaly_filter",
            "ANOMALY_THRESHOLD": "0.01",
        },
    }


def _bidirectional_sharder(i, bidirectional_sharders, account_frequency_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/bidirectional_sharder/Dockerfile",
        },
        "container_name": f"bidirectional_sharder_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"account_frequency_filter_{j}": {"condition": "service_started"}
                for j in range(account_frequency_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_DATE_FILTER_OUTPUT,
            "INPUT_ROUTING_KEY": f"{ROUTING_KEY_USD}.{ROUTING_KEY_PERIOD_1},{ROUTING_KEYS_EOF}",
            "INPUT_QUEUE_NAME": "bidirectional_sharder_input",
            "OUTPUT_EXCHANGE": EXCHANGE_BIDIRECTIONAL_SHARDER_OUTPUT,
            "CONTROL_EXCHANGE": "bidirectional_sharder_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bidirectional_sharders),
            "OUTPUT_NODE_COUNT": str(account_frequency_filters),
            "OUTPUT_NODE_PREFIX": NODE_PREFIX,
        },
    }


def _account_frequency_filter(i, account_frequency_filters, path_mappers):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/account_frequency_filter/Dockerfile",
        },
        "container_name": f"account_frequency_filter_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"path_mapper_{j}": {"condition": "service_started"}
                for j in range(path_mappers)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_BIDIRECTIONAL_SHARDER_OUTPUT,
            "OUTPUT_EXCHANGE": EXCHANGE_ACCOUNT_FREQ_FILTER_OUTPUT,
            "CONTROL_EXCHANGE": "account_freq_filter_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(account_frequency_filters),
            "OUTPUT_NODE_COUNT": str(path_mappers),
            "OUTPUT_NODE_PREFIX": NODE_PREFIX,
            "MIN_REQUIRED_ACCOUNTS": MIN_REQUIRED_ACCOUNTS,
            "BATCH_SIZE": ACCOUNT_EDGE_BATCH_SIZE,
        },
    }


def _path_mapper(i, path_mappers, path_frequency_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/path_mapper/Dockerfile",
        },
        "container_name": f"path_mapper_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"path_frequency_filter_{j}": {"condition": "service_started"}
                for j in range(path_frequency_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_ACCOUNT_FREQ_FILTER_OUTPUT,
            "OUTPUT_EXCHANGE": EXCHANGE_PATH_MAPPER_OUTPUT,
            "CONTROL_EXCHANGE": "path_mapper_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(path_mappers),
            "OUTPUT_NODE_COUNT": str(path_frequency_filters),
            "OUTPUT_NODE_PREFIX": NODE_PREFIX,
            "BATCH_SIZE": PATH_BATCH_SIZE,
        },
    }


def _path_frequency_filter(i, path_frequency_filters, duplicate_account_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/path_frequency_filter/Dockerfile",
        },
        "container_name": f"path_frequency_filter_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"duplicate_account_filter_{j}": {"condition": "service_started"}
                for j in range(duplicate_account_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_PATH_MAPPER_OUTPUT,
            "OUTPUT_EXCHANGE": EXCHANGE_PATH_FREQ_FILTER_OUTPUT,
            "CONTROL_EXCHANGE": "path_freq_filter_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(path_frequency_filters),
            "OUTPUT_NODE_COUNT": str(duplicate_account_filters),
            "OUTPUT_NODE_PREFIX": NODE_PREFIX,
            "MIN_REQUIRED_ACCOUNTS": MIN_REQUIRED_ACCOUNTS,
            "BATCH_SIZE": Q4_RESULT_BATCH_SIZE,
        },
    }


def _duplicate_account_filter(i, duplicate_account_filters):
    return {
        "build": {
            "context": ".",
            "dockerfile": "src/workers/duplicate_account_filter/Dockerfile",
        },
        "container_name": f"duplicate_account_filter_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": EXCHANGE_PATH_FREQ_FILTER_OUTPUT,
            "OUTPUT_EXCHANGE": EXCHANGE_QUERY_RESULTS,
            "QUERY_ID": str(QUERY_4_ID),
            "CONTROL_EXCHANGE": "duplicate_account_filter_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(duplicate_account_filters),
            "BATCH_SIZE": Q4_RESULT_BATCH_SIZE,
        },
    }


def _watchdog(watchdog_id, watchdog_count, monitored_nodes):
    return {
        "build": {"context": ".", "dockerfile": "src/watchdog/Dockerfile"},
        "container_name": f"watchdog_{watchdog_id}",
        "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
        "environment": {
            "PING_PONG_HOST": PING_PONG_HOST,
            "PING_PORT": PING_PORT,
            "PONG_PORT": PONG_PORT,
            "PING_TIMEOUT_SECONDS": PING_TIMEOUT_SECONDS,
            "CHECK_INTERVAL_SECONDS": CHECK_INTERVAL_SECONDS,
            "MAX_RETRIES": MAX_RETRIES,
            "MONITORED_NODES": ",".join(monitored_nodes),
            "WATCHDOG_ID": str(watchdog_id),
            "WATCHDOG_COUNT": str(watchdog_count),
            "ELECTION_PORT": ELECTION_PORT,
            "ELECTION_TIMEOUT_SECONDS": ELECTION_TIMEOUT_SECONDS,
            "LEADER_PROBE_MISS_THRESHOLD": LEADER_PROBE_MISS_THRESHOLD,
        },
    }


def _enable_health(service):
    service["environment"].update(
        {
            "PING_PONG_HOST": PING_PONG_HOST,
            "PING_PORT": PING_PORT,
            "NODE_NAME": service["container_name"],
        }
    )


# --- Builder ---


def build_compose(
    clients,
    gateways,
    transactions_field_mappers,
    accounts_field_mappers,
    date_filters,
    payment_format_filters,
    currency_mappers,
    low_amount_aggregators,
    bank_max_aggregators,
    bank_max_reducers,
    low_amount_reducers,
    bank_mappers,
    amount_filters,
    payment_format_aggregators,
    payment_format_reducers,
    anomaly_filters,
    bidirectional_sharders,
    account_frequency_filters,
    path_mappers,
    path_frequency_filters,
    duplicate_account_filters,
    watchdogs,
    protected_prefixes,
):
    services = {}
    services["rabbitmq"] = _rabbitmq()
    for i in range(1, gateways + 1):
        services[f"gateway_{i}"] = _gateway(
            i, transactions_field_mappers, accounts_field_mappers
        )
    services["proxy"] = _proxy(gateways)
    for i in range(1, clients + 1):
        services[f"client_{i}"] = _client(i)
    for i in range(transactions_field_mappers):
        services[f"transactions_field_mapper_{i}"] = _transactions_field_mapper(
            i, date_filters, bank_max_aggregators, amount_filters
        )
    for i in range(date_filters):
        services[f"date_filter_{i}"] = _date_filter(
            i,
            payment_format_filters,
            payment_format_aggregators,
            anomaly_filters,
            bidirectional_sharders,
        )
    for i in range(bidirectional_sharders):
        services[f"bidirectional_sharder_{i}"] = _bidirectional_sharder(
            i, bidirectional_sharders, account_frequency_filters
        )
    for i in range(account_frequency_filters):
        services[f"account_frequency_filter_{i}"] = _account_frequency_filter(
            i, account_frequency_filters, path_mappers
        )
    for i in range(path_mappers):
        services[f"path_mapper_{i}"] = _path_mapper(
            i, path_mappers, path_frequency_filters
        )
    for i in range(duplicate_account_filters):
        services[f"duplicate_account_filter_{i}"] = _duplicate_account_filter(
            i, duplicate_account_filters
        )
    for i in range(path_frequency_filters):
        services[f"path_frequency_filter_{i}"] = _path_frequency_filter(
            i, path_frequency_filters, duplicate_account_filters
        )
    for i in range(payment_format_filters):
        services[f"payment_format_filter_{i}"] = _payment_format_filter(
            i, currency_mappers
        )
    for i in range(currency_mappers):
        services[f"currency_mapper_{i}"] = _currency_mapper(i, low_amount_aggregators)
    for i in range(low_amount_aggregators):
        services[f"low_amount_aggregator_{i}"] = _low_amount_aggregator(
            i, low_amount_aggregators, low_amount_reducers
        )
    for i in range(low_amount_reducers):
        services[f"low_amount_reducer_{i}"] = _low_amount_reducer(
            i, low_amount_reducers
        )
    for i in range(accounts_field_mappers):
        services[f"accounts_field_mapper_{i}"] = _accounts_field_mapper(i, bank_mappers)
    for i in range(bank_max_aggregators):
        services[f"bank_max_aggregator_{i}"] = _bank_max_aggregator(
            i, bank_max_aggregators, bank_max_reducers
        )
    for i in range(bank_max_reducers):
        services[f"bank_max_reducer_{i}"] = _bank_max_reducer(
            i, bank_max_reducers, bank_mappers
        )
    for i in range(bank_mappers):
        services[f"bank_mapper_{i}"] = _bank_mapper(i, bank_mappers)
    for i in range(amount_filters):
        services[f"amount_filter_{i}"] = _amount_filter(i)
    for i in range(payment_format_aggregators):
        services[f"payment_format_aggregator_{i}"] = _payment_format_aggregator(
            i, payment_format_aggregators, payment_format_reducers
        )
    for i in range(payment_format_reducers):
        services[f"payment_format_reducer_{i}"] = _payment_format_reducer(
            i, payment_format_reducers, anomaly_filters
        )
    for i in range(anomaly_filters):
        services[f"anomaly_filter_{i}"] = _anomaly_filter(i, anomaly_filters)
    unmonitored_prefixes = tuple(protected_prefixes.split())
    monitored_nodes = []
    for name, service in services.items():
        if name.startswith(unmonitored_prefixes):
            continue
        _enable_health(service)
        monitored_nodes.append(service["container_name"])
    for i in range(watchdogs):
        services[f"watchdog_{i}"] = _watchdog(i, watchdogs, monitored_nodes)
    volumes = {f"anomaly_filter_spill_{i}": None for i in range(anomaly_filters)}
    volumes.update({f"bank_mapper_spill_{i}": None for i in range(bank_mappers)})
    return {
        "name": "moneylaundering-client",
        "services": services,
        "volumes": volumes,
    }


# --- Main ---


def main():
    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yaml with configurable replica counts."
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=DEFAULT_CLIENTS,
        help="Number of client containers to spawn.",
    )
    parser.add_argument(
        "--gateways",
        type=int,
        default=DEFAULT_GATEWAYS,
        help="Number of gateway containers to spawn.",
    )

    parser.add_argument(
        "--transactions-field-mappers", type=int, default=DEFAULT_REPLICAS
    )
    parser.add_argument("--accounts-field-mappers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--date-filters", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--payment-format-filters", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--currency-mappers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--low-amount-aggregators", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--bank-max-aggregators", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--bank-max-reducers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--low-amount-reducers", type=int, default=LOW_AMOUNT_REDUCERS)
    parser.add_argument("--bank-mappers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--amount-filters", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument(
        "--payment-format-aggregators", type=int, default=DEFAULT_REPLICAS
    )
    parser.add_argument("--payment-format-reducers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--anomaly-filters", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--bidirectional-sharders", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument(
        "--account-frequency-filters", type=int, default=DEFAULT_REPLICAS
    )
    parser.add_argument("--path-mappers", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument("--path-frequency-filters", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument(
        "--duplicate-account-filters", type=int, default=DEFAULT_REPLICAS
    )
    parser.add_argument("--watchdogs", type=int, default=DEFAULT_REPLICAS)
    parser.add_argument(
        "--protected-prefixes",
        default=DEFAULT_PROTECTED_PREFIXES,
        help="Space-separated service name prefixes that are neither health-monitored nor killed by chaos.",
    )
    parser.add_argument("--output-file", required=True)
    args = parser.parse_args()

    counts = [
        ("--clients", args.clients),
        ("--gateways", args.gateways),
        ("--transactions-field-mappers", args.transactions_field_mappers),
        ("--accounts-field-mappers", args.accounts_field_mappers),
        ("--date-filters", args.date_filters),
        ("--payment-format-filters", args.payment_format_filters),
        ("--currency-mappers", args.currency_mappers),
        ("--low-amount-aggregators", args.low_amount_aggregators),
        ("--bank-max-aggregators", args.bank_max_aggregators),
        ("--bank-max-reducers", args.bank_max_reducers),
        ("--low-amount-reducers", args.low_amount_reducers),
        ("--bank-mappers", args.bank_mappers),
        ("--amount-filters", args.amount_filters),
        ("--payment-format-aggregators", args.payment_format_aggregators),
        ("--payment-format-reducers", args.payment_format_reducers),
        ("--anomaly-filters", args.anomaly_filters),
        ("--bidirectional-sharders", args.bidirectional_sharders),
        ("--account-frequency-filters", args.account_frequency_filters),
        ("--path-mappers", args.path_mappers),
        ("--path-frequency-filters", args.path_frequency_filters),
        ("--duplicate-account-filters", args.duplicate_account_filters),
        ("--watchdogs", args.watchdogs),
    ]
    for flag, value in counts:
        if value < 1:
            parser.error(f"{flag} must be >= 1 (got {value})")

    compose = build_compose(
        clients=args.clients,
        gateways=args.gateways,
        transactions_field_mappers=args.transactions_field_mappers,
        accounts_field_mappers=args.accounts_field_mappers,
        date_filters=args.date_filters,
        payment_format_filters=args.payment_format_filters,
        currency_mappers=args.currency_mappers,
        low_amount_aggregators=args.low_amount_aggregators,
        bank_max_aggregators=args.bank_max_aggregators,
        bank_max_reducers=args.bank_max_reducers,
        low_amount_reducers=args.low_amount_reducers,
        bank_mappers=args.bank_mappers,
        amount_filters=args.amount_filters,
        payment_format_aggregators=args.payment_format_aggregators,
        payment_format_reducers=args.payment_format_reducers,
        anomaly_filters=args.anomaly_filters,
        bidirectional_sharders=args.bidirectional_sharders,
        account_frequency_filters=args.account_frequency_filters,
        path_mappers=args.path_mappers,
        path_frequency_filters=args.path_frequency_filters,
        duplicate_account_filters=args.duplicate_account_filters,
        watchdogs=args.watchdogs,
        protected_prefixes=args.protected_prefixes,
    )

    output = yaml.safe_dump(compose, sort_keys=False, default_flow_style=False)
    with open(args.output_file, "w") as f:
        f.write(output)


if __name__ == "__main__":
    main()

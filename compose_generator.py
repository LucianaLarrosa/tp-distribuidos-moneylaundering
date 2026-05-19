import argparse
import sys

import yaml

DEFAULT_REPLICAS = 3
LOW_AMOUNT_REDUCERS = 1
NODE_PREFIX = "node."

DATE_FROM_1 = "2022/09/01 00:00"
DATE_TO_1 = "2022/09/05 23:59"
DATE_FROM_2 = "2022/09/06 00:00"
DATE_TO_2 = "2022/09/15 23:59"


def _rabbitmq():
    return {
        "build": "./src/rabbitmq",
        "container_name": "rabbitmq",
        "ports": ["5672:5672", "15672:15672"],
        "healthcheck": {
            "test": ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"],
            "interval": "5s",
            "timeout": "10s",
            "retries": 15,
            "start_period": "10s",
        },
    }


def _gateway(transactions_field_mappers, accounts_field_mappers):
    return {
        "build": {"context": ".", "dockerfile": "src/gateway/Dockerfile"},
        "container_name": "gateway_1",
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
        "ports": ["5000:5000"],
        "environment": {
            "GATEWAY_HOST": "0.0.0.0",
            "GATEWAY_PORT": "5000",
            "POOL_SIZE": "4",
            "RABBITMQ_HOST": "rabbitmq",
            "RAW_DATA_EXCHANGE": "raw_data",
            "TRANSACTION_ROUTING_KEY": "transaction",
            "ACCOUNT_ROUTING_KEY": "account",
            "QUERY_RESULTS_EXCHANGE": "query_results",
        },
    }


def _client():
    return {
        "build": {"context": ".", "dockerfile": "src/client/Dockerfile"},
        "container_name": "client_1",
        "depends_on": {"gateway_1": {"condition": "service_started"}},
        "volumes": [
            "${DATASET_PATH:-./data}:/data:ro",
            "./output:/output",
        ],
        "environment": {
            "SERVER_HOST": "gateway_1",
            "SERVER_PORT": "5000",
            "INPUT_CSV_TRANSACTIONS": "/data/HI-Small_Trans.csv",
            "INPUT_CSV_ACCOUNTS": "/data/HI-Small_accounts.csv",
            "BATCH_SIZE": "1000",
            "EXPECTED_QUERY_IDS": "2,5",
            "OUTPUT_DIR": "/output",
        },
    }


def _transactions_field_mapper(i, date_filters, bank_max_aggregators):
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
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "RAW_DATA_EXCHANGE": "raw_data",
            "INPUT_ROUTING_KEY": "transaction",
            "INPUT_QUEUE_NAME": "transactions_field_mapper_input",
            "OUTPUT_EXCHANGE": "filtered_transactions",
            "OUTPUT_ROUTING_KEY_USD": "usd",
            "OUTPUT_ROUTING_KEY_NOUSD": "nousd",
            "OUTPUT_ROUTING_KEY_EOF": "eof",
            "USD_CURRENCY": "US Dollar",
        },
    }


def _date_filter(i, payment_format_filters):
    return {
        "build": {"context": ".", "dockerfile": "src/workers/date_filter/Dockerfile"},
        "container_name": f"date_filter_{i}",
        "depends_on": {
            "rabbitmq": {"condition": "service_healthy"},
            **{
                f"payment_format_filter_{j}": {"condition": "service_started"}
                for j in range(payment_format_filters)
            },
        },
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_EXCHANGE": "filtered_transactions",
            "INPUT_ROUTING_KEY": "#",
            "INPUT_QUEUE_NAME": "date_filter_input",
            "OUTPUT_EXCHANGE": "date_filter_output",
            "DATE_FORMAT": "%Y/%m/%d %H:%M",
            "DATE_FROM_1": DATE_FROM_1,
            "DATE_TO_1": DATE_TO_1,
            "DATE_FROM_2": DATE_FROM_2,
            "DATE_TO_2": DATE_TO_2,
            "USD_CURRENCY": "US Dollar",
            "OUTPUT_ROUTING_KEY_USD": "usd",
            "OUTPUT_ROUTING_KEY_NO_USD": "nousd",
            "OUTPUT_ROUTING_KEY_PERIOD_1": "period1",
            "OUTPUT_ROUTING_KEY_PERIOD_2": "period2",
            "OUTPUT_ROUTING_KEY_EOF": "eof",
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
            "INPUT_EXCHANGE": "date_filter_output",
            "INPUT_ROUTING_KEY": "*.period1,eof",
            "INPUT_QUEUE_NAME": "payment_format_filter_input",
            "OUTPUT_QUEUE": "currency_mapper_input",
            "VALID_PAYMENT_FORMATS": "Wire,ACH",
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
            "INPUT_QUEUE": "currency_mapper_input",
            "OUTPUT_QUEUE": "currency_mapper_output",
            "TARGET_CURRENCY": "US Dollar",
            "FRANKFURTER_URL": "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD",
            "FRANKFURTER_TIMEOUT_SECONDS": "10",
            "RATES_DATE_FIELD": "date",
            "RATES_QUOTE_FIELD": "quote",
            "RATES_RATE_FIELD": "rate",
            "RATES_DATE_FORMAT": "%Y-%m-%d",
            "TRANSACTION_DATE_FORMAT": "%Y/%m/%d %H:%M",
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
            "INPUT_QUEUE": "currency_mapper_output",
            "OUTPUT_QUEUE": "low_amount_aggregator_output",
            "CONTROL_EXCHANGE": "low_amount_aggregator_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(low_amount_aggregators),
            "MAX_AMOUNT": "1.0",
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
            "INPUT_QUEUE": "low_amount_aggregator_output",
            "OUTPUT_EXCHANGE": "query_results",
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
            "RAW_DATA_EXCHANGE": "raw_data",
            "INPUT_ROUTING_KEY": "account",
            "INPUT_QUEUE_NAME": "accounts_field_mapper_input",
            "OUTPUT_EXCHANGE": "bank_catalog",
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
            "INPUT_EXCHANGE": "filtered_transactions",
            "INPUT_BINDING_PATTERNS": "usd,eof",
            "INPUT_QUEUE": "bank_max_input",
            "OUTPUT_EXCHANGE": "bank_max_output",
            "CONTROL_EXCHANGE": "bank_max_aggregator_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_max_aggregators),
            "BATCH_SIZE": "5",
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
            "INPUT_EXCHANGE": "bank_max_output",
            "SHARD_ID": str(i),
            "OUTPUT_QUEUE": "bank_max_results",
            "CONTROL_EXCHANGE": "bank_max_reducer_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_max_reducers),
            "BATCH_SIZE": "20",
        },
    }


def _bank_mapper(i, bank_mappers):
    return {
        "build": {"context": ".", "dockerfile": "src/workers/bank_mapper/Dockerfile"},
        "container_name": f"bank_mapper_{i}",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": {
            "RABBITMQ_HOST": "rabbitmq",
            "INPUT_QUEUE": "bank_max_results",
            "OUTPUT_EXCHANGE": "query_results",
            "BANKS_EXCHANGE": "bank_catalog",
            "CONTROL_EXCHANGE": "bank_mapper_control",
            "NODE_PREFIX": NODE_PREFIX,
            "NODE_ID": str(i),
            "RING_SIZE": str(bank_mappers),
        },
    }


def build_compose(
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
):
    services = {}
    services["rabbitmq"] = _rabbitmq()
    services["gateway_1"] = _gateway(transactions_field_mappers, accounts_field_mappers)
    services["client_1"] = _client()
    for i in range(transactions_field_mappers):
        services[f"transactions_field_mapper_{i}"] = _transactions_field_mapper(
            i, date_filters, bank_max_aggregators
        )
    for i in range(date_filters):
        services[f"date_filter_{i}"] = _date_filter(i, payment_format_filters)
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
    return {"name": "moneylaundering-client", "services": services}


def _resolve_count(value, replicas):
    if value is None:
        return replicas
    return value


def main():
    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yaml with configurable replica counts."
    )
    parser.add_argument(
        "--replicas",
        type=int,
        default=DEFAULT_REPLICAS,
        help="Default replica count for every scalable worker.",
    )
    parser.add_argument("--transactions-field-mappers", type=int, default=None)
    parser.add_argument("--accounts-field-mappers", type=int, default=None)
    parser.add_argument("--date-filters", type=int, default=None)
    parser.add_argument("--payment-format-filters", type=int, default=None)
    parser.add_argument("--currency-mappers", type=int, default=None)
    parser.add_argument("--low-amount-aggregators", type=int, default=None)
    parser.add_argument("--bank-max-aggregators", type=int, default=None)
    parser.add_argument("--bank-max-reducers", type=int, default=None)
    parser.add_argument("--low-amount-reducers", type=int, default=LOW_AMOUNT_REDUCERS)
    parser.add_argument("--bank-mappers", type=int, default=None)
    parser.add_argument("--output", default=None, help="Output file (default: stdout).")
    args = parser.parse_args()

    if args.replicas < 1:
        parser.error(f"--replicas must be >= 1 (got {args.replicas})")

    transactions_field_mappers = _resolve_count(
        args.transactions_field_mappers, args.replicas
    )
    accounts_field_mappers = _resolve_count(args.accounts_field_mappers, args.replicas)
    date_filters = _resolve_count(args.date_filters, args.replicas)
    payment_format_filters = _resolve_count(args.payment_format_filters, args.replicas)
    currency_mappers = _resolve_count(args.currency_mappers, args.replicas)
    low_amount_aggregators = _resolve_count(args.low_amount_aggregators, args.replicas)
    bank_max_aggregators = _resolve_count(args.bank_max_aggregators, args.replicas)
    bank_max_reducers = _resolve_count(args.bank_max_reducers, args.replicas)
    low_amount_reducers = args.low_amount_reducers
    bank_mappers = _resolve_count(args.bank_mappers, args.replicas)

    counts = [
        ("--transactions-field-mappers", transactions_field_mappers),
        ("--accounts-field-mappers", accounts_field_mappers),
        ("--date-filters", date_filters),
        ("--payment-format-filters", payment_format_filters),
        ("--currency-mappers", currency_mappers),
        ("--low-amount-aggregators", low_amount_aggregators),
        ("--bank-max-aggregators", bank_max_aggregators),
        ("--bank-max-reducers", bank_max_reducers),
        ("--low-amount-reducers", low_amount_reducers),
        ("--bank-mappers", bank_mappers),
    ]
    for flag, value in counts:
        if value < 1:
            parser.error(f"{flag} must be >= 1 (got {value})")

    compose = build_compose(
        transactions_field_mappers=transactions_field_mappers,
        accounts_field_mappers=accounts_field_mappers,
        date_filters=date_filters,
        payment_format_filters=payment_format_filters,
        currency_mappers=currency_mappers,
        low_amount_aggregators=low_amount_aggregators,
        bank_max_aggregators=bank_max_aggregators,
        bank_max_reducers=bank_max_reducers,
        low_amount_reducers=low_amount_reducers,
        bank_mappers=bank_mappers,
    )

    output = yaml.safe_dump(compose, sort_keys=False, default_flow_style=False)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
    else:
        sys.stdout.write(output)


if __name__ == "__main__":
    main()

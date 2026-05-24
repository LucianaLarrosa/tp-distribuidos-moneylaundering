import csv
import logging
import os
from collections import defaultdict
from datetime import datetime

import requests

# --- Configuration ---

DATASET_DIR = os.environ.get("DATASET_DIR", "./data")
EXPECTED_DIR = os.environ.get("EXPECTED_DIR", "./expected_output")

TRANSACTIONS_FILE_PATH = os.path.join(
    DATASET_DIR, os.environ.get("TRANSACTIONS_FILE", "HI-Small_Trans.csv")
)
ACCOUNTS_FILE = os.path.join(
    DATASET_DIR, os.environ.get("ACCOUNTS_FILE", "HI-Small_accounts.csv")
)

_FRANKFURTER_URL = (
    "https://api.frankfurter.dev/v2/rates?from=2022-09-01&to=2022-09-05&base=USD"
)

# --- Constants ---

_USD_CURRENCY = "US Dollar"
_TRANSACTIONS_DATE_FORMAT = "%Y/%m/%d %H:%M"
_AMOUNT_THRESHOLD_QUERY_1 = 50.0
_PERCENTAGE_THRESHOLD = 0.01
_MIN_REQUIRED_ACCOUNTS = 5
_FRANKFURTER_TIMEOUT_SECONDS = 10
_RATES_DATE_FIELD = "date"
_RATES_QUOTE_FIELD = "quote"
_RATES_RATE_FIELD = "rate"
_CURRENCY_NAME_TO_ISO = {
    "australian dollar": "AUD",
    "bitcoin": "BTC",
    "brazil real": "BRL",
    "canadian dollar": "CAD",
    "euro": "EUR",
    "mexican peso": "MXN",
    "ruble": "RUB",
    "rupee": "INR",
    "saudi riyal": "SAR",
    "shekel": "ILS",
    "swiss franc": "CHF",
    "uk pound": "GBP",
    "yen": "JPY",
    "yuan": "CNY",
}
# Bitcoin rates taken from investing.com
_BTC_RATES = {
    "2022-09-01": 1.0 / 19793.1,
    "2022-09-02": 1.0 / 199999.0,
    "2022-09-03": 1.0 / 19831.4,
    "2022-09-04": 1.0 / 19952.7,
    "2022-09-05": 1.0 / 20126.1,
}
_DEFAULT_RATE = 1.0
_RATES_DATE_FORMAT = "%Y-%m-%d"
_DECIMAL_PLACES = 2
_VALID_PAYMENT_FORMATS = {"Wire", "ACH"}
_AMOUNT_THRESHOLD_QUERY_5 = 1.0

# Transactions: Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering
TIMESTAMP_INDEX = 0
FROM_BANK_INDEX = 1
FROM_ACCOUNT_INDEX = 2
TO_BANK_INDEX = 3
TO_ACCOUNT_INDEX = 4
AMOUNT_PAID_INDEX = 7
PAYMENT_CURRENCY_INDEX = 8
PAYMENT_FORMAT_INDEX = 9

# Accounts: Bank Name,Bank ID,Account Number,Entity ID,Entity Name
BANK_NAME_INDEX = 0
BANK_ID_INDEX = 1


# --- Auxiliary functions ---


def _fetch_rates():
    try:
        response = requests.get(
            _FRANKFURTER_URL,
            timeout=_FRANKFURTER_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        rates = {}
        for entry in response.json():
            rates.setdefault(entry[_RATES_DATE_FIELD], {})[
                entry[_RATES_QUOTE_FIELD]
            ] = entry[_RATES_RATE_FIELD]
        for date_str, btc_rate in _BTC_RATES.items():
            rates.setdefault(date_str, {})["BTC"] = btc_rate
        return rates
    except Exception as exc:
        logging.warning("Could not fetch Frankfurter rates (%s); using rate=1.0", exc)
        return dict(_BTC_RATES)


_RATES = _fetch_rates()


def _resolve_rate(amount, currency, timestamp):
    iso_code = _CURRENCY_NAME_TO_ISO.get(currency.lower())
    date_str = _parse_date(timestamp).strftime(_RATES_DATE_FORMAT)
    rate = (
        _RATES.get(date_str, {}).get(iso_code, _DEFAULT_RATE)
        if iso_code
        else _DEFAULT_RATE
    )
    return round(float(amount) * (1.0 / rate), _DECIMAL_PLACES)


def _parse_date(timestamp):
    return datetime.strptime(timestamp, _TRANSACTIONS_DATE_FORMAT)


_PERIOD_1_START_DATE = _parse_date("2022/09/01 00:00")
_PERIOD_1_END_DATE = _parse_date("2022/09/05 23:59")
_PERIOD_2_START_DATE = _parse_date("2022/09/06 00:00")
_PERIOD_2_END_DATE = _parse_date("2022/09/15 23:59")


def _iter_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        yield from reader


def _iter_transactions():
    return _iter_csv(TRANSACTIONS_FILE_PATH)


def _iter_accounts():
    return _iter_csv(ACCOUNTS_FILE)


# --- Query builders ---


def _build_query_1():
    """
    Cuenta de origen, cuenta de destino y monto para transacciones USD menores a 50.
    """
    query_1_output_path = os.path.join(EXPECTED_DIR, "q1_expected.csv")
    with open(query_1_output_path, "w", newline="", encoding="utf-8") as output_file:
        output_csv_writer = csv.writer(output_file)
        for transaction in _iter_transactions():
            if transaction[PAYMENT_CURRENCY_INDEX] != _USD_CURRENCY:
                continue
            amount_paid = float(transaction[AMOUNT_PAID_INDEX])
            if amount_paid < _AMOUNT_THRESHOLD_QUERY_1:
                output_csv_writer.writerow(
                    [
                        transaction[FROM_BANK_INDEX],
                        transaction[FROM_ACCOUNT_INDEX],
                        transaction[TO_BANK_INDEX],
                        transaction[TO_ACCOUNT_INDEX],
                        amount_paid,
                    ]
                )


def _build_query_2():
    """
    Nombre de banco, cuenta de origen y monto de la max. transacción USD de cada banco.
    """
    bank_names = {}
    for account in _iter_accounts():
        normalized_bank_id = str(int(account[BANK_ID_INDEX]))
        bank_names[normalized_bank_id] = account[BANK_NAME_INDEX]

    bank_max_transactions = {}
    for transaction in _iter_transactions():
        if transaction[PAYMENT_CURRENCY_INDEX] != _USD_CURRENCY:
            continue
        normalized_bank_id = str(int(transaction[FROM_BANK_INDEX]))
        amount_paid = float(transaction[AMOUNT_PAID_INDEX])
        from_account = transaction[FROM_ACCOUNT_INDEX]
        if (
            normalized_bank_id not in bank_max_transactions
            or amount_paid > bank_max_transactions[normalized_bank_id][0]
            or (
                from_account < bank_max_transactions[normalized_bank_id][1]
                and amount_paid == bank_max_transactions[normalized_bank_id][0]
            )
        ):
            bank_max_transactions[normalized_bank_id] = (
                amount_paid,
                transaction[FROM_ACCOUNT_INDEX],
            )

    query_2_output_path = os.path.join(EXPECTED_DIR, "q2_expected.csv")
    with open(query_2_output_path, "w", newline="", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        for normalized_bank_id, (
            amount_paid,
            account_id,
        ) in bank_max_transactions.items():
            if normalized_bank_id not in bank_names:
                continue
            csv_writer.writerow(
                [bank_names[normalized_bank_id], account_id, amount_paid]
            )


def _build_query_3():
    """
    Cuenta de origen y monto de transacciones USD en el período [2022-09-06, 2022-09-15]
    con monto menor a 1 centésimo del promedio encontrado para el mismo formato de
    pago en el período [2022-09-01, 2022-09-05]
    """
    payment_format_totals = {}
    for transaction in _iter_transactions():
        if (
            transaction[PAYMENT_CURRENCY_INDEX] == _USD_CURRENCY
            and _PERIOD_1_START_DATE
            <= _parse_date(transaction[TIMESTAMP_INDEX])
            <= _PERIOD_1_END_DATE
        ):
            payment_format = transaction[PAYMENT_FORMAT_INDEX]
            total, count = payment_format_totals.get(payment_format, (0.0, 0))
            payment_format_totals[payment_format] = (
                total + float(transaction[AMOUNT_PAID_INDEX]),
                count + 1,
            )

    payment_format_average = {
        payment_format: total / count
        for payment_format, (total, count) in payment_format_totals.items()
    }

    query_3_output_path = os.path.join(EXPECTED_DIR, "q3_expected.csv")
    with open(query_3_output_path, "w", newline="", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        for transaction in _iter_transactions():
            if not (
                transaction[PAYMENT_CURRENCY_INDEX] == _USD_CURRENCY
                and _PERIOD_2_START_DATE
                <= _parse_date(transaction[TIMESTAMP_INDEX])
                <= _PERIOD_2_END_DATE
            ):
                continue
            payment_format = transaction[PAYMENT_FORMAT_INDEX]
            if payment_format not in payment_format_average:
                continue
            amount_paid = float(transaction[AMOUNT_PAID_INDEX])
            if (
                amount_paid
                < payment_format_average[payment_format] * _PERCENTAGE_THRESHOLD
            ):
                csv_writer.writerow(
                    [
                        transaction[FROM_BANK_INDEX],
                        transaction[FROM_ACCOUNT_INDEX],
                        amount_paid,
                    ]
                )


def _build_query_4():
    """
    Cuentas que cumplan con el patrón scatter-gather con una sola cuenta de separación,
    para cuentas que hayan realizado transferencias en USD hacia 5 cuentas distintas dentro
    del período [2022-09-01, 2022-09-05]
    """
    scatter_map = defaultdict(set)
    for transaction in _iter_transactions():
        if (
            transaction[PAYMENT_CURRENCY_INDEX] == _USD_CURRENCY
            and _PERIOD_1_START_DATE
            <= _parse_date(transaction[TIMESTAMP_INDEX])
            <= _PERIOD_1_END_DATE
        ):
            scatter_map[
                (transaction[FROM_BANK_INDEX], transaction[FROM_ACCOUNT_INDEX])
            ].add((transaction[TO_BANK_INDEX], transaction[TO_ACCOUNT_INDEX]))

    scatter_accounts = {
        from_account: to_account
        for from_account, to_account in scatter_map.items()
        if len(to_account) >= _MIN_REQUIRED_ACCOUNTS
    }

    intermediate_to_scatter = defaultdict(set)
    for from_account, to_accounts in scatter_accounts.items():
        for to_account in to_accounts:
            intermediate_to_scatter[to_account].add(from_account)

    scatter_gather_map = defaultdict(lambda: defaultdict(set))
    for transaction in _iter_transactions():
        from_account = (transaction[FROM_BANK_INDEX], transaction[FROM_ACCOUNT_INDEX])
        if (
            from_account in intermediate_to_scatter
            and _PERIOD_1_START_DATE
            <= _parse_date(transaction[TIMESTAMP_INDEX])
            <= _PERIOD_1_END_DATE
        ):
            to_account = (transaction[TO_BANK_INDEX], transaction[TO_ACCOUNT_INDEX])
            for scatter_source_account in intermediate_to_scatter[from_account]:
                scatter_gather_map[scatter_source_account][to_account].add(from_account)

    result = []
    for scatter_source_account, gather_destination_map in scatter_gather_map.items():
        for (
            gather_destination_account,
            intermediate_accounts,
        ) in gather_destination_map.items():
            if len(intermediate_accounts) >= _MIN_REQUIRED_ACCOUNTS:
                result.append((scatter_source_account, gather_destination_account))

    query_4_output_path = os.path.join(EXPECTED_DIR, "q4_expected.csv")
    with open(query_4_output_path, "w", newline="", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        for (source_bank, source_account), (
            destination_bank,
            destination_account,
        ) in result:
            csv_writer.writerow([source_bank, source_account])
            csv_writer.writerow([destination_bank, destination_account])


def _build_query_5():
    """
    Cantidad de transacciones del período [2022-09-01, 2022-09-05] con formato de pago
    "Wire" o "ACH" cuyo monto convertido a USD sea menor a 1
    """
    count = 0
    for transaction in _iter_transactions():
        if (
            _PERIOD_1_START_DATE
            <= _parse_date(transaction[TIMESTAMP_INDEX])
            <= _PERIOD_1_END_DATE
            and transaction[PAYMENT_FORMAT_INDEX] in _VALID_PAYMENT_FORMATS
            and _resolve_rate(
                transaction[AMOUNT_PAID_INDEX],
                transaction[PAYMENT_CURRENCY_INDEX],
                transaction[TIMESTAMP_INDEX],
            )
            < _AMOUNT_THRESHOLD_QUERY_5
        ):
            count += 1

    query_5_output_path = os.path.join(EXPECTED_DIR, "q5_expected.csv")
    with open(query_5_output_path, "w", encoding="utf-8") as output_file:
        output_file.write(f"{count}\r\n")


def build_expected_outputs():
    os.makedirs(EXPECTED_DIR, exist_ok=True)
    _build_query_1()
    logging.info("Query 1 output built successfully.")
    _build_query_2()
    logging.info("Query 2 output built successfully.")
    _build_query_3()
    logging.info("Query 3 output built successfully.")
    _build_query_4()
    logging.info("Query 4 output built successfully.")
    _build_query_5()
    logging.info("Query 5 output built successfully.")


# --- Main ---


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    build_expected_outputs()
    logging.info("Done. Outputs written to %s/", EXPECTED_DIR)


if __name__ == "__main__":
    main()

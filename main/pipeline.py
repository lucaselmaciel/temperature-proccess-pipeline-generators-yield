from __future__ import annotations
import csv
from collections import deque
from typing import Iterable, Iterator, Callable, Dict, Any

from utils.type_validations import _is_float

Row = Dict[str, Any]


def read_csv(
    path: str, *, encoding: str = "utf-8", delimiter: str = ","
) -> Iterator[Row]:
    with open(path, "r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            yield row


def read_multiple_csv(paths: Iterable[str]) -> Iterator[Row]:
    for path in paths:
        yield from read_csv(path)


def select_columns(rows: Iterable[Row], *cols: str) -> Iterator[Row]:
    for row in rows:
        yield {c: row[c] for c in cols if c in row}


def filter_rows(rows: Iterable[Row], pred: Callable[[Row], bool]) -> Iterator[Row]:
    for row in rows:
        if pred(row):
            yield row


def map_rows(rows: Iterable[Row], fn: Callable[[Row], Row]) -> Iterator[Row]:
    for row in rows:
        yield fn(row)


def moving_average(rows: Iterable[Row], col: str, window: int = 7) -> Iterator[Row]:
    buf: deque[float] = deque(maxlen=window)
    total: float = 0.0
    for row in rows:
        try:
            value: float = float(row[col])
        except (KeyError, TypeError, ValueError):
            value = float("nan")

        if len(buf) == buf.maxlen:
            total -= buf[0]
        buf.append(value)
        total += value

        ma: float = total / len(buf) if buf else float("nan")
        out: Row = dict(row)
        out[f"moving_average_{col}"] = ma
        yield out


def pipeline(
    paths: Iterable[str], *, window: int = 7, temp_col: str = "temp"
) -> Iterator[Row]:
    rows: Iterable[Row] = read_multiple_csv(paths)
    rows = select_columns(rows, "date", temp_col, "city")
    rows = filter_rows(
        rows,
        lambda r: r.get(temp_col) not in (None, "")
        and not str(r[temp_col]).isspace()
        and _is_float(r[temp_col])
    )

    def to_float(row: Row) -> Row:
        rr: Row = dict(row)
        rr[temp_col] = float(rr[temp_col])
        return rr

    rows = map_rows(rows, to_float)
    return moving_average(rows, col=temp_col, window=window)

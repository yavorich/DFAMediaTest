import pandas as pd
import sqlite3
from datetime import date
import random
import argparse


def connect(dbname: str) -> sqlite3.Connection:
    return sqlite3.connect(dbname)


def disconnect(conn: sqlite3.Connection) -> None:
    conn.close()


def save_table(name: str, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    df.to_sql(name, conn, if_exists="replace")


def show_table(name: str, conn: sqlite3.Connection) -> None:
    print(pd.read_sql(f"SELECT * FROM {name}", conn))


def add_date_field(df: pd.DataFrame) -> None:
    # рандомно генерируем даты, так, чтобы были повторения
    dates = [date(2023, 11, random.randint(1, 10)) for i in range(len(df))]
    dates = list(sorted(dates))
    df.insert(1, "date", dates)
    df["date"] = df["date"].apply(str)


def parse(filepath: str) -> pd.DataFrame:
    df = pd.read_excel(filepath, header=[0, 1, 2], index_col=0)
    df = df.rename(columns=lambda col: ""
                   if str(col).startswith("Unnamed") else col)
    add_date_field(df)

    # вывод полученной таблицы
    print("\n-------Original table-------\n\n", df, "\n")

    # сжатие многоуровневых колонок
    df.columns = ["_".join(c).strip("_") for c in df.columns.to_flat_index()]
    return df


def create_total_by_date(conn: sqlite3.Connection, table_name: str) -> None:
    cur = conn.cursor()

    # получаем колонки с целочисленным типом данных
    integer_columns_query = (
        f"SELECT name FROM pragma_table_info('{table_name}')"
        + "WHERE type = 'INTEGER' AND name NOT LIKE 'index';"
    )
    cur.execute(integer_columns_query)
    cols = [col[0] for col in cur.fetchall()]
    agg_cols = [cols[i].replace("_data1", "") for i in range(0, len(cols), 2)]

    # удаляем старую таблицу Total, если есть
    cur.execute("DROP TABLE IF EXISTS Total;")

    # формируем подзапрос на агрегацию Qlig/Qoil
    agg_query = (
        "SELECT date, "
        + ", ".join(
            [
                f"{col + '_data1'} + {col + '_data2'}"
                + f" as {col}" for col in agg_cols
            ]
        )
        + f" FROM '{table_name}'"
    )

    # оборачиваем в запрос, группирующий по дате
    total_query = (
        "CREATE TABLE Total AS SELECT date, "
        + ", ".join([f"SUM({col}) AS sum_{col}" for col in agg_cols])
        + f" FROM ({agg_query}) GROUP BY date;"
    )

    # выполняем запрос
    cur.execute(total_query)

    # вывод полученного результата
    show_table("Total", conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data_path", default="data/company_data.xlsx")
    parser.add_argument("--db_path", default="db/testbase.db")
    parser.add_argument("--table_name", default="CompanyData")

    args = parser.parse_args()

    df = parse(args.data_path)
    conn = connect(args.db_path)

    save_table(args.table_name, conn, df)
    create_total_by_date(conn, args.table_name)

    disconnect(conn)

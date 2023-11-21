import sqlite3


class SQLite:
    def __init__(self, db_path: str) -> None:
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def create_total_by_date(self, table_name: str,
                             result_table_name: str) -> None:

        # получаем колонки с целочисленным типом данных
        integer_columns_query = (
            f"SELECT name FROM pragma_table_info('{table_name}')"
            + "WHERE type = 'INTEGER' AND name NOT LIKE 'index';"
        )
        self.cursor.execute(integer_columns_query)
        cols = [col[0] for col in self.cursor.fetchall()]
        agg_cols = [cols[i].replace("_data1", "")
                    for i in range(0, len(cols), 2)]

        # удаляем старую таблицу result, если есть
        self.cursor.execute(f"DROP TABLE IF EXISTS {result_table_name};")

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
            f"CREATE TABLE {result_table_name} AS SELECT date, "
            + ", ".join([f"SUM({col}) AS sum_{col}" for col in agg_cols])
            + f" FROM ({agg_query}) GROUP BY date;"
        )

        # выполняем запрос
        self.cursor.execute(total_query)

    def close(self) -> None:
        self.connection.close()

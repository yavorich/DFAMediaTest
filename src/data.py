import random
import pandas as pd
from datetime import date
from db import SQLite


class Datasource:
    def __init__(self, data_path: str, db: SQLite):
        self.db = db
        self.df = self.parse(data_path)

    def parse(self, data_path: str) -> pd.DataFrame:
        df = self.load_from_excel(data_path)
        self.add_date_field(df)  # рандомное заполнение поля date
        self.save_as_excel(df)  # сохранение результата до сжатия колонок
        self.flat_columns(df)  # сжатие многоуровневых колонок
        return df

    def add_date_field(self, df: pd.DataFrame) -> None:
        # рандомно генерируем даты, так, чтобы были повторения
        dates = [date(2023, 11, random.randint(1, 10))
                 for i in range(len(df))]
        dates = list(sorted(dates))
        df.insert(1, "date", dates)
        # переводим в строковый тип, т.к. SQLite не поддерживает datetime
        df["date"] = df["date"].apply(str)

    def load_from_excel(self, filepath: str) -> pd.DataFrame:
        df = pd.read_excel(filepath, header=[0, 1, 2], index_col=0)
        df = df.rename(columns=lambda col: ""
                       if str(col).startswith("Unnamed") else col)
        return df

    def flat_columns(self, df: pd.DataFrame) -> None:
        df.columns = ["_".join(c).strip("_")
                      for c in df.columns.to_flat_index()]

    def save_to_db(self, table_name) -> None:
        self.df.to_sql(table_name, self.db.connection, if_exists="replace")

    def save_as_excel(self, df: pd.DataFrame) -> None:
        df.to_excel("data/result.xlsx")

    def show_db_table(self, table_name: str) -> None:
        # визуализация таблицы в терминале
        print(pd.read_sql(f"SELECT * FROM {table_name}", self.db.connection))

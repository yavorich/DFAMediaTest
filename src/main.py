import argparse
from db import SQLite
from data import Datasource

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data_path", default="data/company_data.xlsx")
    parser.add_argument("--db_path", default="db/testbase.db")
    parser.add_argument("--table_name", default="CompanyData")
    parser.add_argument("--result_table_name", default="Total")

    args = parser.parse_args()
    db = SQLite(args.db_path)
    data = Datasource(args.data_path, db)
    data.save_to_db(args.table_name)
    db.create_total_by_date(args.table_name, args.result_table_name)
    data.show_db_table(args.result_table_name)
    db.close()

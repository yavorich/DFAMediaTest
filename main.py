import argparse
from src.datasource import Datasource

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data_path", required=True)
    parser.add_argument("--db_path", default="db/testbase.db")
    parser.add_argument("--table_name", default="CompanyData")
    parser.add_argument("--result_table_name", default="Total")

    args = parser.parse_args()
    datasource = Datasource(args.data_path, args.db_path)
    datasource.save_to_db(args.table_name)
    datasource.create_total_by_date(args.table_name, args.result_table_name)
    datasource.show_db_table(args.result_table_name)
    datasource.close()

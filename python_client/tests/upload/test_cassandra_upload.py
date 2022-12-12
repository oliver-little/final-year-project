from client.upload.cassandra_upload import *

def test_infer_columns_from_csv(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("A, B, C, D, E\n 2022-12-12T11:40:55Z , 1 , 1.01, true, hello")

    column_names, column_types = infer_columns_from_csv(csv_file)
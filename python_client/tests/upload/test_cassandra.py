import pytest

from cassandra.cluster import Session

from cluster_client.upload.cassandra import *
from cluster_client.connector.cassandra import CassandraConnector

import csv

def write_csv(path, data):
    with open(path, "w") as file:
        writer = csv.writer(file)
        writer.writerows(data)

def test_is_float():
    assert is_float("1.01") == True
    assert is_float("1.00000000000000000001") == True
    assert is_float("1") == True
    assert is_float("Abcd") == False
    assert is_float("1.0a") == False

def test_is_bool():
    assert is_bool("true") == True
    assert is_bool("TRUE") == True
    assert is_bool("false") == True
    assert is_bool("FALSE") == True
    assert is_bool("truea") == False
    assert is_bool("Abcd") == False

def test_is_iso_datetime():
    assert is_iso_datetime("2022-12-12T11:40:55Z") == True
    assert is_iso_datetime("2022-12-13T11:36:35+00:00") == True
    assert is_iso_datetime("20221213T113635Z") == True
    assert is_iso_datetime("2022-12-12") == True
    assert is_iso_datetime("2022-12-12T11:40:55Za") == False


def test_infer_columns_from_csv(tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["A", "B", "C", "D", "E"],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    column_names, column_types = infer_columns_from_csv(csv_file)
    assert column_names == ["A", "B", "C", "D", "E"], "Invalid column names"
    assert column_types == ["timestamp", "bigint", "double", "boolean", "text"], "Invalid column types"

    data = [
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    column_types = infer_columns_from_csv(csv_file, detect_headers=False)
    assert column_types == ["timestamp", "bigint", "double", "boolean", "text"], "Invalid column types"

def test_infer_columns_from_csv_type_change(tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["A", "B", "C", "D", "E"],
        ["2022-12-12T11:40:55Z", "2022-12-12T11:40:55Z", "2022-12-12T11:40:55Z", "2022-12-12T11:40:55Z", "2022-12-12T11:40:55Z"],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    _, column_types = infer_columns_from_csv(csv_file)
    assert column_types == ["timestamp", "bigint", "double", "boolean", "text"], "Invalid column types"

def test_infer_columns_from_csv_empty_csv(tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["A", "B", "C", "D", "E"]
    ]
    write_csv(csv_file, data)

    with pytest.raises(ValueError):
        infer_columns_from_csv(csv_file)

    data = [
    ]
    write_csv(csv_file, data)

    with pytest.raises(ValueError):
        infer_columns_from_csv(csv_file)

def test_create_from_csv(mocker):
    mock_infer = mocker.patch("cluster_client.upload.cassandra.infer_columns_from_csv", return_value=(["A", "B"], ["text", "bigint"]))
    mock_create = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.create_table")
    mock_insert = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.insert_from_csv")

    mock_connector, mock_session = get_mock_connector(mocker)
    mock_connector.has_table.return_value = False

    upload_handler = CassandraUploadHandler(mock_connector)
    upload_handler.create_from_csv("test.csv", "test", "table", ["A"], ["B"])

    mock_infer.assert_called_once_with("test.csv", ",", '"', "\n", detect_headers=True)
    mock_create.assert_called_once_with("test", "table", ["A", "B"], ["text", "bigint"], ["A"], ["B"])
    mock_insert.assert_called_once_with("test.csv", "test", "table", ["A", "B"], {}, 1, ',', '"', "\n")

def test_create_from_csv_provided_columns(mocker):
    mock_infer = mocker.patch("cluster_client.upload.cassandra.infer_columns_from_csv", return_value=["text", "bigint"])
    mock_create = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.create_table")
    mock_insert = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.insert_from_csv")

    mock_connector, mock_session = get_mock_connector(mocker)
    mock_connector.has_table.return_value = False

    upload_handler = CassandraUploadHandler(mock_connector)
    upload_handler.create_from_csv("test.csv", "test", "table", ["A"], ["B"], column_names=["A", "B"])

    mock_infer.assert_called_once_with("test.csv", ",", '"', "\n", detect_headers=False)
    mock_create.assert_called_once_with("test", "table", ["A", "B"], ["text", "bigint"], ["A"], ["B"])
    mock_insert.assert_called_once_with("test.csv", "test", "table", ["A", "B"], {}, 1, ',', '"', "\n")

def test_create_from_csv_provided_columns_and_types(mocker):
    mock_infer = mocker.patch("cluster_client.upload.cassandra.infer_columns_from_csv")
    mock_create = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.create_table")
    mock_insert = mocker.patch("cluster_client.upload.cassandra.CassandraUploadHandler.insert_from_csv")

    mock_connector, mock_session = get_mock_connector(mocker)
    mock_connector.has_table.return_value = False

    upload_handler = CassandraUploadHandler(mock_connector)
    upload_handler.create_from_csv("test.csv", "test", "table", ["A"], ["B"], column_names=["A", "B"], column_types=["text", "bigint"])

    assert not mock_infer.called
    mock_create.assert_called_once_with("test", "table", ["A", "B"], ["text", "bigint"], ["A"], ["B"])
    mock_insert.assert_called_once_with("test.csv", "test", "table", ["A", "B"], {}, 1, ',', '"', "\n")




def get_mock_connector(mocker):
    mock_connector = mocker.MagicMock()
    mock_session = mocker.MagicMock()
    mock_connector.get_session.return_value = mock_session
    return mock_connector, mock_session

def test_create_table(mocker):
    mock_connector, mock_session = get_mock_connector(mocker)
    mock_connector.has_table.return_value = False

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.create_table("test", "table", ["A", "B1", "C", "D", "E"], ["timestamp", "bigint", "double", "boolean", "text"], ["A", "B1"], ["C"])

    assert mock_connector.get_session.called
    mock_connector.create_keyspace.assert_called_once_with("test")
    mock_connector.has_table.assert_called_once_with("test", "table")
    mock_session.execute.assert_called_once_with("CREATE TABLE test.table (A timestamp, B1 bigint, C double, D boolean, E text, PRIMARY KEY ((A, B1), C));")

def test_create_table_invalid(mocker):
    mock_connector, mock_session = get_mock_connector(mocker)
    mock_connector.has_table.return_value = False

    upload_handler = CassandraUploadHandler(mock_connector)
    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", [], ["timestamp", "bigint", "double", "boolean", "text"], ["A", "B"], ["C"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A", "B", "C", "D", "E"], [], ["A", "B"], ["C"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A", "B", "C", "D", "E"], ["timestamp", "bigint", "double", "boolean"], [], ["C"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A??", "B", "C", "D", "E"], ["timestamp", "bigint", "double", "boolean", "text"], ["A", "B"], ["C"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A", "B", "C", "D", "E"], ["timestamp1", "bigint", "double", "boolean", "text"], ["A", "B"], ["C"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A", "B", "C", "D", "E"], ["timestamp", "bigint", "double", "boolean", "text"], ["A", "B10"], ["C1"])

    with pytest.raises(ValueError):
        upload_handler.create_table("test", "table", ["A", "B", "C", "D", "E"], ["timestamp", "bigint", "double", "boolean", "text"], ["A", "B"], ["A"])


def test_insert_from_csv(mocker, tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["A", "B", "C", "D", "E"],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table")

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    # Use any here, as the left argument is the mocked prepared statement and we don't care to check that again
    mock_session.execute.assert_called_once_with(mocker.ANY, ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"])

def test_insert_from_csv_with_cols(mocker, tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table", ["A", "B", "C", "D", "E"])

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    # Use any here, as the left argument is the mocked prepared statement and we don't care to check that again
    mock_session.execute.assert_called_once_with(mocker.ANY, ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"])
 
def test_insert_from_csv_empty_csv(mocker, tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table", ["A", "B", "C", "D", "E"])

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    assert not mock_session.execute.called, "Execute was called when it shouldn't have been"

def test_insert_from_csv_skip_rows(mocker, tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        [],
        [],
        ["A", "B", "C", "D", "E"],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table", start_row=3)

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    mock_session.execute.assert_called_once_with(mocker.ANY, ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"])

    data = [
        [],
        [],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table", ["A", "B", "C", "D", "E"], start_row=3)

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    mock_session.execute.assert_called_once_with(mocker.ANY, ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"])

def test_insert_from_csv_converter(mocker, tmp_path):
    csv_file = tmp_path / "test.csv"
    data = [
        ["A", "B", "C", "D", "E"],
        ["2022-12-12T11:40:55Z", "1", "1.01", "true", "hello"]
    ]
    write_csv(csv_file, data)

    mock_connector, mock_session = get_mock_connector(mocker)

    upload_handler = CassandraUploadHandler(mock_connector)

    upload_handler.insert_from_csv(csv_file, "test", "test_table", row_converters={"B" : lambda x: str(int(x) + 1)})

    assert mock_connector.get_session.called
    mock_session.prepare.assert_called_once_with("INSERT INTO test.test_table (A, B, C, D, E) VALUES (?, ?, ?, ?, ?);")
    # Use any here, as the left argument is the mocked prepared statement and we don't care to check that again
    mock_session.execute.assert_called_once_with(mocker.ANY, ["2022-12-12T11:40:55Z", "2", "1.01", "true", "hello"])

from kedro_projetaai.utils.readers import VersionedDataset
from kedro_projetaai.utils.readers import (
    DatasetTypes,
    LoadLast,
    ReadFile,
    PathReader,
)
from kedro_projetaai.utils.extra_datasets_utils.path_patterns import match_date_pattern
import pandas as pd
import numpy as np
import os
import unittest
from typing import Union
import shutil
from pandas.core.groupby.generic import DataFrameGroupBy

TEMP_PREFIX = "temp_datasets/"


def generate_directory(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
    return


def save_files(
    df: Union[pd.DataFrame, DataFrameGroupBy],
    format: str,
    path: str,
    name_group_edit: bool = False,
):

    suffix_name = ""
    datasetype_obj = DatasetTypes[format]
    generate_directory(os.path.dirname(path))
    if isinstance(df, DataFrameGroupBy):
        path = path.split(".")[0]
        for name, group in df:
            if name_group_edit:
                name = name.replace("-", "/")
                name = "/" + name + "/"
                suffix_name = "file"
            generate_directory(os.path.dirname(path + "_" + name))
            datasetype_obj.write(
                group,
                path + "_" + name + suffix_name + "." + format,
                storage_options={},
            )
        return
    datasetype_obj.write(df, path, storage_options={})
    return


def remove_files():
    shutil.rmtree(TEMP_PREFIX)
    return


def generate_dataframe(n_rows: int, n_cols: int = 2) -> pd.DataFrame:

    dates = pd.date_range(
        start=(pd.to_datetime("today") - pd.DateOffset(days=n_rows - 1)).strftime(
            "%Y-%m-%d"
        ),
        end=pd.to_datetime("today").strftime("%Y-%m-%d"),
        freq="D",
        name="date",
        normalize=True,
    )

    df = pd.DataFrame(dates).assign(**{f"col_{i}": i for i in range(n_cols)})

    return df


# df = generate_dataframe(10, 2)


class test_datasets(unittest.TestCase):
    def setUp(self) -> None:
        if os.path.exists(TEMP_PREFIX):
            remove_files()
        return

    def test_generate_dataframe(self):
        df = generate_dataframe(10, 2)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        return

    def test_save_and_remove_files(self):
        df = generate_dataframe(10, 2)
        save_files(df, "csv", TEMP_PREFIX + "test.csv")
        self.assertTrue(os.path.exists(TEMP_PREFIX + "test.csv"))
        remove_files()
        self.assertFalse(os.path.exists(TEMP_PREFIX + "test.csv"))

    def test_ReadFile_excel(self):
        for extension in ["csv", "xlsx"]:
            df = generate_dataframe(10, 2)
            save_files(df, extension, TEMP_PREFIX + f"test.{extension}")
            # testing with ReadFile and no load_args
            readfile_obj = ReadFile(
                path=TEMP_PREFIX + f"test.{extension}", credentials=None
            )
            df_read = readfile_obj._load()
            df_read = df_read.drop(columns=["Unnamed: 0"])
            df_read["date"] = df_read["date"].astype("datetime64[ns]")
            self.assertIsInstance(df_read, pd.DataFrame)
            self.assertTrue(df.equals(df_read))
            self.assertFalse(df_read.empty)
            remove_files()
        return

    def test_ReadFile(self):
        for extension in ["parquet", "json"]:
            df = generate_dataframe(10, 2)
            save_files(df, extension, TEMP_PREFIX + f"test.{extension}")
            # testing with ReadFile and no load_args
            readfile_obj = ReadFile(
                path=TEMP_PREFIX + f"test.{extension}", credentials=None
            )
            df_read = readfile_obj._load()
            self.assertIsInstance(df_read, pd.DataFrame)
            self.assertTrue(df.equals(df_read))
            self.assertFalse(df_read.empty)
            remove_files()
        return

    def test_ReadFile_pickle(self):
        df = generate_dataframe(10, 2)
        save_files(df, "pickle", TEMP_PREFIX + "test.pickle")
        readfile_obj = ReadFile(path=TEMP_PREFIX + f"test.pickle", credentials=None)
        df_read = readfile_obj._load()
        self.assertIsInstance(df_read, pd.DataFrame)
        self.assertTrue(df.equals(df_read))
        self.assertFalse(df_read.empty)
        return

    def test_match_patterns(self):
        path = "test_2023-04-01.parquet"
        check, pattern = match_date_pattern(path)
        self.assertEqual(check, "2023-04-01")
        self.assertEqual(pattern, "\\d{4}-\\d{2}-\\d{2}")

        path = "test_20230401.parquet"
        check, pattern = match_date_pattern(path)
        self.assertEqual(check, "20230401")
        self.assertEqual(pattern, "\\d{4}\\d{2}\\d{2}")

        path = "test/test12345678/test_20230401.parquet"
        check, pattern = match_date_pattern(path)
        self.assertEqual(check, "20230401")
        self.assertEqual(pattern, "\\d{4}\\d{2}\\d{2}")

        path = "test/2023/04/01/file.parquet"
        check, pattern = match_date_pattern(path)
        self.assertEqual(check, "2023/04/01")
        self.assertEqual(pattern, "\\d{4}/\\d{2}/\\d{2}")

        path = "test/2023/04/01/file20230401.parquet"
        check, pattern = match_date_pattern(path)
        self.assertEqual(check, "2023/04/01")
        self.assertEqual(pattern, "\\d{4}/\\d{2}/\\d{2}")
        return

    def test_PathReader(self):
        df = generate_dataframe(270, 2)
        months_test = 5
        min_date = (df["date"].max() - pd.DateOffset(months=5)).strftime("%Y-%m-%d")
        max_date = df["date"].max().strftime("%Y-%m-%d")
        save_files(
            df.groupby(df["date"].dt.strftime("%Y-%m-%d")),
            "parquet",
            TEMP_PREFIX + "test.parquet",
        )
        readfile_obj = PathReader(
            path=TEMP_PREFIX,
            credentials=None,
            read_args={"time_scale": "M", "history_length": months_test},
            load_args={"columns": ["date", "col_0"]},
        )
        df_read = readfile_obj._load()
        self.assertTrue(df_read["date"].max().strftime("%Y-%m-%d") == max_date)
        self.assertTrue(df_read["date"].min().strftime("%Y-%m-%d") == min_date)
        self.assertTrue(list(df_read.columns) == ["date", "col_0"])
        remove_files()
        return

    def test_PathReader_t(self):
        df = generate_dataframe(270, 2)
        months_test = 5
        min_date = df["date"].max() - pd.DateOffset(months=5)
        save_files(
            df.groupby(df["date"].dt.strftime("%Y-%m-%d")),
            "parquet",
            TEMP_PREFIX + "test.parquet",
            name_group_edit=True,
        )
        readfile_obj = PathReader(
            path=TEMP_PREFIX,
            credentials=None,
            read_args={"time_scale": "M", "history_length": months_test},
            load_args={"columns": ["date", "col_0"]},
        )
        df_read = readfile_obj._load()
        self.assertTrue(min_date == df_read["date"].min())
        remove_files()
        return

    def test_PathReader_back_date(self):
        df = generate_dataframe(300, 2)
        months_test = 5
        max_date = df["date"].max() - pd.DateOffset(months=1)
        save_files(
            df.groupby(df["date"].dt.strftime("%Y-%m-%d")),
            "parquet",
            TEMP_PREFIX + "test.parquet",
            name_group_edit=True,
        )
        readfile_obj = PathReader(
            path=TEMP_PREFIX,
            credentials=None,
            read_args={"time_scale": "M", "history_length": months_test},
            load_args={"columns": ["date", "col_0"]},
            back_date=max_date.strftime("%Y-%m-%d"),
        )
        readfile_obj_no_back_date = PathReader(
            path=TEMP_PREFIX,
            credentials=None,
            read_args={"time_scale": "M", "history_length": months_test},
            load_args={"columns": ["date", "col_0"]},
        )
        readfile_obj_no_back_date = readfile_obj_no_back_date._load()
        df_read = readfile_obj._load()
        self.assertEqual(readfile_obj_no_back_date.shape, df_read.shape)
        self.assertTrue(max_date == df_read["date"].max())
        remove_files()
        return

    def test_VersionedDataSet(self):
        df = generate_dataframe(90, 2)
        today = pd.to_datetime("today").strftime("%Y-%m-%d")
        os.makedirs(TEMP_PREFIX + f"/{today}")
        df.to_parquet(TEMP_PREFIX + f"/{today}/" + f"test_{today}.parquet")
        filepath = TEMP_PREFIX + "{date_path}/test_{date_file}.parquet"
        readfile_obj = VersionedDataset(
            path=filepath,
            credentials=None,
            version_config={"date_path": "%Y-%m-%d", "date_file": "%Y-%m-%d"},
        )
        df_load = readfile_obj._load()
        self.assertTrue(df.equals(df_load))
        os.remove(TEMP_PREFIX + f"/{today}/" + f"test_{today}.parquet")
        self.assertFalse(
            os.path.exists(TEMP_PREFIX + f"/{today}/" + f"test_{today}.parquet")
        )
        readfile_obj._save(df_load)
        self.assertTrue(
            os.path.exists(TEMP_PREFIX + f"/{today}/" + f"test_{today}.parquet")
        )
        remove_files()
        return

    def test_abfs_path(self):
        path = "abfs://account_name.dfs.core.windows.net/test/test.parquet"
        readfile_obj = ReadFile(
            path=path, credentials={"account_name": "account_name", "anon": False}
        )
        self.assertTrue(readfile_obj.credentials["account_name"] == "account_name")
        self.assertTrue(readfile_obj.protocol == "abfs")
        readfile_obj.remove_account_url_from_path()
        self.assertTrue(readfile_obj.path == "abfs://test/test.parquet")
        return

    def test_rises_VersionedDataset(self):
        with self.assertRaises(ValueError):
            readfile_obj = VersionedDataset(
                path="test{not_date_path}{date_path}/test{date_file}.parquet",
                credentials=None,
                version_config={"date_path": "%Y-%m-%d", "date_file": "%Y-%m-%d"},
            )
            readfile_obj._generate_formatted_path()
        return

    def test_rises_not_supported_file_format(self):
        path = "test/test.p4rqu3t"
        readfile_obj = ReadFile(
            path=path, credentials={"account_name": "account_name", "anon": False}
        )
        with self.assertRaises(ValueError):
            readfile_obj._load()
        return

    def test_LoadLast(self):
        df = generate_dataframe(300, 2)
        max_date = df["date"].max()
        save_files(
            df.groupby(df["date"].dt.strftime("%Y-%m-%d")),
            "parquet",
            TEMP_PREFIX + "test.parquet",
            name_group_edit=True,
        )
        readfile_obj = LoadLast(
            path=TEMP_PREFIX, credentials=None, load_args={"columns": ["date", "col_0"]}
        )
        df_read = readfile_obj._load()
        self.assertTrue(max_date == df_read["date"].max())
        return

    def test_LoadLast_backdate(self):
        df = generate_dataframe(300, 2)
        back_date = (
            pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").sample(1).values[0]
        )
        save_files(
            df.groupby(df["date"].dt.strftime("%Y-%m-%d")),
            "parquet",
            TEMP_PREFIX + "test.parquet",
            name_group_edit=True,
        )
        readfile_obj = LoadLast(
            path=TEMP_PREFIX,
            credentials=None,
            load_args={"columns": ["date", "col_0"]},
            back_date=back_date,
        )
        df_read = readfile_obj._load()
        self.assertTrue(df_read["date"].max() == pd.to_datetime(back_date))
        return

    def test_dtypes(self):
        df = generate_dataframe(270, 2)
        df["col_0"] = df["col_0"].astype("string")
        save_files(df, "parquet", TEMP_PREFIX + "test.parquet")
        readfile_obj = ReadFile(
            path=TEMP_PREFIX + f"test.parquet",
            credentials={"account_name": "account_name", "anon": False},
            load_args={"dtypes": {"col_0": "int"}},
        )
        df_read = readfile_obj._load()
        self.assertIsInstance(df_read["col_0"].dtypes, np.dtypes.Int64DType)
        return

    # def test_this(self):
    #     path = 'abfs://data/refined/previsao_vendas/new_preprocessing_sales/sales_firme_diario_cliente_base/'
    #     readfile_obj = PathReader(path=path,
    #                             credentials={'account_name': 'stippdatalakedev',
    #                                          'anon': False},
    #                             read_args={'time_scale': 'M',
    #                                        'history_length': 5,
    #                                        'starting_weekday': 6})
    #     readfile_obj._load()
    #     return

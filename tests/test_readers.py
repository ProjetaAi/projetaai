"""This file will test the functions in the readers.py file."""

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
    """Generate a directory if it does not exist."""
    if not os.path.exists(path):
        os.makedirs(path)
    return


def save_files(
    df: Union[pd.DataFrame, DataFrameGroupBy],
    format: str,
    path: str,
    name_group_edit: bool = False,
    suffix_as_date: bool = False,
):
    """Save the files in the temp folder."""
    suffix_name = ""
    datasetype_obj = DatasetTypes[format]
    generate_directory(os.path.dirname(path))
    if isinstance(df, DataFrameGroupBy):
        path = path.split(".")[0]
        for name, group in df:
            if name_group_edit:
                name = name.replace("-", "/")
                name = "/" + name + "/"
                if suffix_as_date:
                    suffix_name = "file" + name.replace("/", "")
                else:
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
    """Remove all files in the temp folder."""
    shutil.rmtree(TEMP_PREFIX)
    return


def generate_dataframe(n_rows: int, n_cols: int = 2) -> pd.DataFrame:
    """Generate a dataframe with n_rows and n_cols.

    The dataframes we will generate to help test the functions.
    """
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


class test_datasets(unittest.TestCase):
    """This class will test the functions in the readers.py file."""

    def setUp(self) -> None:
        """
        This function will run before each test.

        setUp makes sure that the temp folder is empty before each test run.
        """
        if os.path.exists(TEMP_PREFIX):
            remove_files()
        return

    def test_generate_dataframe(self):
        """This test will test the generate_dataframe function."""
        df = generate_dataframe(10, 2)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        return

    def test_save_and_remove_files(self):
        """This test the support function created to save and remove files."""
        df = generate_dataframe(10, 2)
        save_files(df, "csv", TEMP_PREFIX + "test.csv")
        self.assertTrue(os.path.exists(TEMP_PREFIX + "test.csv"))
        remove_files()
        self.assertFalse(os.path.exists(TEMP_PREFIX + "test.csv"))

    def test_ReadFile(self):
        """This test will test all methods accepted by ReadFile.

        The only method we skip is pickle because it is tested in another test
        and needs a proper startup.
        """
        for extension in DatasetTypes.__members__.keys():
            if extension == "pickle":
                continue
            df = generate_dataframe(10, 2)
            save_files(df, extension, TEMP_PREFIX + f"test.{extension}")
            # testing with ReadFile and no load_args
            readfile_obj = ReadFile(
                path=TEMP_PREFIX + f"test.{extension}", credentials=None
            )
            df_read = readfile_obj._load()
            self.assertIsInstance(df_read, pd.DataFrame)
            if "Unnamed: 0" in df_read.columns:
                # This is a workaround for the fact
                # that excel files have an extra column
                df_read = df_read.drop(columns=["Unnamed: 0"])
            # can not use directly df.equals(df_read) because
            # the dtype of the columns might change depending on the saving method
            # but guaranteeing the dataframe shape in this test is enough
            self.assertTrue(df.shape == df_read.shape)
            self.assertFalse(df_read.empty)
            readfile_obj.path = TEMP_PREFIX + "test2." + extension
            readfile_obj._save(df_read)
            self.assertTrue(os.path.exists(TEMP_PREFIX + "test2." + extension))
            remove_files()
        return

    def test_ReadFile_pickle(self):
        """Testing the pickle saving and load.

        This test create and save a file in pickle format and read it with ReadFile
        and then it will use ReadFile to save the file again, checking if all
        the methods are working correctly.
        """
        df = generate_dataframe(10, 2)
        save_files(df, "pickle", TEMP_PREFIX + "test.pickle")
        readfile_obj = ReadFile(path=TEMP_PREFIX + "test.pickle", credentials=None)
        df_read = readfile_obj._load()
        self.assertIsInstance(df_read, pd.DataFrame)
        self.assertTrue(df.equals(df_read))
        self.assertFalse(df_read.empty)
        readfile_obj.path = TEMP_PREFIX + "test2.pickle"
        readfile_obj._save(df_read)
        self.assertTrue(os.path.exists(TEMP_PREFIX + "test2.pickle"))
        return

    def test_match_patterns(self):
        """Test if the match_date_pattern function is working correctly.

        This test multiple cases of date patterns.
        """
        path = "test_2023-04-01.parquet"
        match, pattern, date_format = match_date_pattern(path)
        self.assertEqual(match, "2023-04-01")
        self.assertEqual(pattern, "\\d{4}-\\d{2}-\\d{2}")

        path = "test_20230401.parquet"
        match, pattern, date_format = match_date_pattern(path)
        self.assertEqual(match, "20230401")
        self.assertEqual(pattern, "\\d{4}\\d{2}\\d{2}")

        path = "test/test12345678/test_20230401.parquet"
        match, pattern, date_format = match_date_pattern(path)
        self.assertEqual(match, "20230401")
        self.assertEqual(pattern, "\\d{4}\\d{2}\\d{2}")

        path = "test/2023/04/01/file.parquet"
        match, pattern, date_format = match_date_pattern(path)
        self.assertEqual(match, "2023/04/01")
        self.assertEqual(pattern, "\\d{4}/\\d{2}/\\d{2}")

        path = "test/2023/04/01/file20230401.parquet"
        match, pattern, date_format = match_date_pattern(path)
        self.assertEqual(match, "2023/04/01")
        self.assertEqual(pattern, "\\d{4}/\\d{2}/\\d{2}")
        return

    def test_PathReader_without_folders(self):
        """
        Test if the PathReader class is working correctly.

        This test specifically tests the case where the files are not
        separated in folders.
        It tests if it can read files saved as:
        ...
        ├── test_2023-09-07.parquet
        ├── test_2023-09-08.parquet
        ├── test_2023-09-09.parquet
        ├── test_2023-09-10.parquet
        └── test_2023-09-11.parquet
        ...
        """
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

    def test_PathReader_with_date_folders(self):
        """
        Test if the PathReader class is working correctly.

        This test specifically tests the case where the files are
        separated in folders by date.
        It tests if it can read files saved as:
        ...
        ├── 2022
        │   └── 12
        │       ├── 16
        │       │   └── file.parquet
        │       ├── 17
        │       │   └── file.parquet
        │       ├── 18
        │       │   └── file.parquet
        ...
        """
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
        """Test if the backdate is being applied correctly in PathReader."""
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
        """Test if the VersionedDataset class is working correctly."""
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

    def test_VersionedDataSet_weekday(self):
        """Test if the VersionedDataset class is working correctly.

        this in particular tests if it works well with starting_weekday"""
        df = generate_dataframe(90, 2)
        today = pd.to_datetime("today")
        today = today - pd.Timedelta(days=today.weekday())
        back_date = today.strftime("%Y-%m-%d")
        test_day = today - pd.Timedelta(days=2)
        test_day = test_day.strftime("%Y-%m-%d")
        os.makedirs(TEMP_PREFIX + f"/{test_day}")
        df.to_parquet(TEMP_PREFIX + f"/{test_day}/" + f"test_{test_day}.parquet")
        filepath = TEMP_PREFIX + "{date_path}/test_{date_file}.parquet"
        readfile_obj = VersionedDataset(
            path=filepath,
            credentials=None,
            version_config={"date_path": "%Y-%m-%d",
                            "date_file": "%Y-%m-%d",
                            'starting_weekday': 5},
            back_date=back_date,
        )
        df_load = readfile_obj._load()
        self.assertTrue(df_load.equals(df))
        remove_files()
        return

    def test_abfs_path(self):
        """Test if the abfs path is being read correctly."""
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
        """
        Test if the error is raised when the file format is not supported.

        This case specifically tests the case where the {not_date_path}
        shouldn't be in the path. The function should raise an error.
        It only supports {date_path} and {date_file}.
        """
        with self.assertRaises(ValueError):
            readfile_obj = VersionedDataset(
                path="test{not_date_path}{date_path}/test{date_file}.parquet",
                credentials=None,
                version_config={"date_path": "%Y-%m-%d", "date_file": "%Y-%m-%d"},
            )
            readfile_obj._generate_formatted_path()
        return

    def test_rises_not_supported_file_format(self):
        """Test if the error is raised when the file format is not supported."""
        path = "test/test.p4rqu3t"
        readfile_obj = ReadFile(
            path=path, credentials={"account_name": "account_name", "anon": False}
        )
        with self.assertRaises(ValueError):
            readfile_obj._load()
        return

    def test_LoadLast(self):
        """Test if the LoadLast class is working correctly."""
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
        """
        Test if the backdate is being applied correctly.

        This test specifically tests the case where the backdate is
        applied in the LoadLast class.
        """
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
        """Test if the dtypes are read and applied correctly."""
        df = generate_dataframe(270, 2)
        df["col_0"] = df["col_0"].astype("string")
        save_files(df, "parquet", TEMP_PREFIX + "test.parquet")
        readfile_obj = ReadFile(
            path=TEMP_PREFIX + "test.parquet",
            credentials={"account_name": "account_name", "anon": False},
            load_args={"dtypes": {"col_0": "int"}},
        )
        df_read = readfile_obj._load()
        self.assertIsInstance(df_read["col_0"].dtypes, np.dtypes.Int64DType)
        return

    def test_PathReader_year_month_folder_format(self):
        """This test will test the year/month folder format."""
        df = generate_dataframe(270, 2)
        max_date = df["date"].max()
        # our min_date will be 2 months before the max_date
        # but the day will be the first day of the month
        # because our files are saved in year/month format
        min_date = pd.to_datetime(
            (max_date - pd.DateOffset(months=2)).strftime("%Y-%m-01")
        )
        df["date_save"] = df["date"].dt.strftime("%Y-%m")
        save_files(
            df.groupby("date_save"),
            "parquet",
            TEMP_PREFIX + "test.parquet",
            name_group_edit=True,
        )
        readfile_obj = PathReader(
            path=TEMP_PREFIX,
            credentials=None,
            read_args={"time_scale": "M", "history_length": 2},
            load_args={"columns": ["date", "col_0"]},
            back_date=None,
        )
        df_read = readfile_obj._load()
        self.assertEqual(df_read["date"].max() == max_date, True)
        self.assertEqual(df_read["date"].min() == min_date, True)
        return

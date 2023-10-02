"""The ProjetAAI Data Readers.

This module serves as an extension to
the reading and writing methods offered by Kedro.
The primary objective is to standardize and centralize the
file-reading process within the catalog,
thereby enhancing code quality across different pipelines in the projects.

Features:
Flexibility: Our module offers greater flexibility by enabling users
to specify a wide array of parameters directly in the YAML configuration file.
This makes it adaptable to various use-cases without requiring modification
of the core reading methods.

Data Description:
One of the key advantages is the ability to include
detailed data descriptions within the catalog.
This helps users understand the data they are working
with more clearly, fostering data integrity and usage correctness.

Code Standardization:
By delegating the responsibility of file reading
to the catalog, we reduce the need for each user
to create bespoke reading methods. This
contributes to a uniform and standardized codebase,
making it easier to maintain and collaborate on projects.
"""

from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, Optional, Union
from kedro_projetaai.utils.extra_datasets_utils.path_patterns import (
    match_date_pattern,
    return_last_match,
)
import pandas as pd
import re
import logging
from kedro.io import AbstractDataSet
from fsspec.utils import infer_storage_options
from fsspec import filesystem
from copy import deepcopy
from kedro_projetaai.utils.extra_datasets_utils.pickle_methods import (
    pickle_load,
    pickle_dump,
)


class DatasetTypes(Enum):
    """Abstracts the read and write methods for each file extension."""

    parquet = (pd.read_parquet, pd.DataFrame.to_parquet)
    csv = (pd.read_csv, pd.DataFrame.to_csv)
    xlsx = (pd.read_excel, pd.DataFrame.to_excel)
    json = (pd.read_json, pd.DataFrame.to_json)
    pickle = (pickle_load, pickle_dump)

    def read(self, path: str, *args, **kwargs) -> Union[pd.DataFrame, bytes]:
        """Generic read function for methods in this class."""
        read_func = self.value[0]
        return read_func(path, *args, **kwargs)

    def write(
        self, df: Union[pd.DataFrame, bytes], path: str, *args, **kwargs
    ) -> object:
        """Generic write function for the methods in this class."""
        write_func = self.value[1]
        return write_func(df, path, *args, **kwargs)

    @classmethod
    def _missing_(cls, value: object):
        """Raises an error if the file extension is not supported."""
        choices = list(cls.__members__.keys())
        raise ValueError(
            "%r is not a valid %s, please choose from %s"
            % (value, cls.__name__, choices)
        )


class BaseDataset(AbstractDataSet):  # type: ignore
    """Base class for all datasets in this module."""

    @property
    def _filesystem(self) -> filesystem:
        """Creates the filesystem object based on the protocol."""
        return filesystem(self.protocol, **self.credentials)

    def __init__(
        self,
        path: str,
        load_args: Optional[dict] = None,
        credentials: Optional[dict] = None,
        save_args: Optional[dict] = None,
        version_config: Optional[dict] = None,
        back_date: Optional[str] = None,
    ) -> None:
        """Class initialization.

        args:
            path (str): path to the file
            load_args (dict): arguments to be passed to the read function.
            If it's trying to read a .parquet file, this are the arguments
            that pandas accepts in the read_parquet function.
            If not provided, the default arguments will be used, an
            empty dict will be used.
            credentials (dict): credentials to access the file, if not provided
            the default credentials will be used, an empty dict will be used.
            the default value here is in case the file is in the local machine.
            save_args (dict): arguments to be passed to the write function. If
            it's trying to write a .parquet file, this are the arguments that
            pandas accepts in the to_parquet function. If not provided, the
            default arguments will be used, an empty dict will be used.
            version_config (dict): arguments to be passed to the versioned
            dataset. If not provided, the default arguments will be used,
            an empty dict will be used.
            back_date (str): date to be used in the versioned dataset.
            If not provided, it's going to read the most recent file, given
            the version_config arguments provided.
        """
        self.path = path
        self.version_config = (
            version_config if version_config is not None else {"starting_weekday": None}
        )
        self.save_args = save_args if save_args is not None else {}
        self.load_args = load_args if load_args is not None else {}
        self.dtypes = self._get_dtypes_from_load_args(self.load_args)
        self.credentials = credentials if credentials is not None else {}
        self._back_date = self.format_back_date(back_date)
        self.protocol = infer_storage_options(self.path)["protocol"]
        self.default_formating()

    def default_formating(self):
        """Format the path.

        If the path is in the cloud, it will remove the account url
        from the path, so it can be used in the read and write functions.
        But if the path is in the local machine, it will add the file
        it will just create an empty dict for the storage options.
        """
        if self.protocol != "file":
            self._storage_options = deepcopy(self.credentials)
            self.remove_account_url_from_path()
        else:
            self._storage_options = {}

    def format_back_date(self, back_date: Union[str, None]) -> Union[str, None]:
        """Formats the back_date argument.

        Kedro yml files don't accept None as a value,
        so it will be passed as a string, this function
        """
        if back_date == "":
            return None
        return back_date

    def remove_account_url_from_path(self):
        """Fixing the path to be used in the cloud.

        If the path is in the cloud, it will remove the account url
        from the path, so it can be used in the read and write functions.
        """
        account_name = self.credentials["account_name"]
        account_url = f"{account_name}.dfs.core.windows.net/"

        if account_url in self.path:
            self.path = self.path.replace(account_url, "")

        return

    def _generate_first_day(self, date_format: str = "%Y-%m-%d") -> pd.Timestamp:
        """Generates the first day based on the back_date and the history_length.

        This is used to filter the files in the given path.
        """
        today = (
            pd.to_datetime("today")
            if self._back_date is None
            else pd.to_datetime(self._back_date, format=date_format)
        )
        last_specific_day = today - pd.Timedelta(days=self._generate_days_difference())
        return self._transform_to_timestamp(
            last_specific_day, format=date_format
        ).normalize()

    def _generate_last_day(self, first_day: pd.Timestamp) -> pd.Timestamp:
        """
        Generates the last day based on the first day and the history_length.

        This is used to filter the files in the given path.
        """
        last_day = first_day - pd.DateOffset(
            **{self.read_args["time_scale"]: self.read_args["history_length"]}
        )
        last_day = last_day - pd.Timedelta(days=self._generate_days_difference())
        return last_day.normalize()

    def _generate_days_difference(self) -> int:
        """
        Generates the days difference based on the first day and the history_length.

        This is used to filter the files in the given path.
        """
        if self.version_config.get("starting_weekday") is None:
            return 0
        else:
            first_day = self._generate_first_day()
            last_day = self._generate_last_day(first_day)
            return (last_day.weekday() - self.version_config["starting_weekday"]) % 7

    def file_manager(self, path: str) -> DatasetTypes:
        """Returns the file manager based on the file extension."""
        try:
            return DatasetTypes[self.get_file_extension(path)]
        except KeyError:
            raise ValueError(
                f"File extension not supported: {self.get_file_extension(path)}"
            )

    def get_file_extension(self, path: str) -> str:
        """Returns the file extension."""
        return path.split(".")[-1]

    def _dtypes_with_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converts the pandas dtypes to the provided dtypes."""
        if isinstance(df, pd.DataFrame):
            return df.astype(self.dtypes)
        return df

    def _save(self, df: pd.DataFrame, path: str) -> None:
        """The default save method for all classes that inherit from this class.

        It will always be called when writing a file.
        It already generates the necessary credentials to write the file in cloud.
        """
        file_manager = self.file_manager(path)
        file_manager.write(
            df=self._dtypes_with_pandas(df),
            path=path,
            **self.save_args,
            storage_options=self._storage_options,
        )
        return

    def _load(self, path: str) -> Union[pd.DataFrame, bytes]:
        """Default load method for all classes that inherit from this class.

        It will always be called when reading a file.
        It already generates the necessary credentials to read the file in cloud.
        """
        file_manager = self.file_manager(path)
        df = file_manager.read(
            path=path, **self.load_args, storage_options=self._storage_options
        )
        return self._dtypes_with_pandas(df)

    def _describe(self) -> dict[str, Any]:
        """Default description for kedro dataset."""
        return dict(path=self.path, protocol=self.protocol)

    def _get_date_from_pattern(self, path: str, date_pattern: str) -> str:

        matched_value = return_last_match(date_pattern, path)
        if matched_value is None:
            raise ValueError(f"Date pattern not found in {path}")
        return matched_value.replace("/", "-")

    def _get_dtypes_from_load_args(self, load_args: dict) -> dict[str, Any]:
        if "dtypes" in load_args:
            dtypes = load_args.pop("dtypes")
            if not isinstance(dtypes, dict):
                raise ValueError("dtypes must be a dict")
            return dtypes
        return {}

    def _add_protocol_to_path(self, path: str) -> pd.Timestamp:
        if self.protocol == "file" or path.startswith(self.protocol):
            return path
        return f"{self.protocol}://{path}"

    def _transform_to_timestamp(
        self, date: Union[str, pd.Timestamp], format: str = "%Y-%m-%d"
    ) -> pd.Timestamp:
        if isinstance(date, str):
            return pd.to_datetime(date, format=format)
        return pd.to_datetime(date.strftime(format=format), format=format)

    def _check_if_all_files_are_in_the_same_format(
        self, patters: set[tuple[str, str]]
    ) -> None:
        if len(patters) > 1:
            raise ValueError(
                f"Files in the given path have different date patterns: {patters}"
            )
        return

    def _wrapper_math_date_pattern(self, path: str) -> tuple[str, str]:
        _, date_pattern, date_format = match_date_pattern(path)
        return date_pattern, date_format

    def _get_all_date_patterns(self, path_list: list[str]) -> set[tuple[str, str]]:
        return set(self._wrapper_math_date_pattern(path) for path in path_list)

    def _check_and_get_patterns(self, path_list: list[str]) -> tuple[str, str]:

        ((date_pattern, date_format),) = self._get_all_date_patterns(path_list)
        self._check_if_all_files_are_in_the_same_format(
            set([(date_pattern, date_format)])
        )

        return date_pattern, date_format


class ReadFile(BaseDataset):
    """Reads a single file.

    Expands the pandas.ParquetDataset class to allow
    to use dtypes. It just to add a little more flexibility
    if working with star schema.
    """

    def __init__(
        self,
        path: str,
        credentials: Optional[dict[str, Any]] = None,
        load_args: Optional[dict[str, Any]] = None,
        save_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize the class.

        args:
            path (str): path to the file
            load_args (dict): arguments to be passed to the read function.
            If it's trying to read a .parquet file, this are the arguments
            that pandas accepts in the read_parquet function.
            If not provided, the default arguments will be used, an
            empty dict will be used.
            credentials (dict): credentials to access the file, if not provided
            the default credentials will be used, an empty dict will be used.
            the default value here is in case the file is in the local machine.
            save_args (dict): arguments to be passed to the write function. If
            it's trying to write a .parquet file, this are the arguments that
            pandas accepts in the to_parquet function. If not provided, the
            default arguments will be used, an empty dict will be used.
        """
        super().__init__(
            path=path,
            load_args=load_args,
            save_args=save_args,
            credentials=credentials,
        )

    def _load(self, path: Optional[str] = None) -> pd.DataFrame:
        """Calls the load function with the proper arguments."""
        df = super()._load(self.path)
        logging.info(f"Loaded {self.path}")
        return df

    def _save(self, df: pd.DataFrame, path: Optional[str] = None) -> None:
        """Calls the save function with the proper arguments."""
        return super()._save(df, self.path)


class VersionedDataset(BaseDataset):  # VendasVersionedDataset
    """To read versioned datasets.

    allow to create versions based on date, e.g.:
    /data/filename20210101.parquet
    and it's possible to create versions between weeks
    or run daily.

    This class can be expanded to allow other types of
    data versioning, e.g.:
    monthly, every 15 days, etc.
    """

    def __init__(
        self,
        path: str,
        credentials: Optional[dict] = None,
        load_args: Optional[dict[str, Any]] = None,
        version_config: Optional[dict[str, Any]] = None,
        back_date: Optional[str] = None,
        save_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize the class.

        Args:
            path (str): Path to the file.
            credentials (dict): Credentials to access the file, if not provided
                the default credentials will be used, an empty dict will be used.
                The default value here is in case the file is in the local machine.
                load_args (dict): Arguments to be passed to the read function.
                If it's trying to read a .parquet file, this are the arguments
                that pandas accepts in the read_parquet function.
                If not provided, the default arguments will be used, an
                empty dict will be used.
                save_args (dict): Arguments to be passed to the write function. If
                it's trying to write a .parquet file, this are the arguments that
                pandas accepts in the to_parquet function. If not provided, the
                default arguments will be used, an empty dict will be used.
                version_config (dict): Arguments to be passed to the versioned
                dataset. If not provided, the default arguments will be used,
                an empty dict will be used. The arguments are:
                starting_weekday: int, the starting weekday to use as reference
                to read data.
                date_path: str, the date format to be used in the path.
                date_file: str, the date format to be used in the file name.
                back_date (str): Date to be used in the versioned dataset.
        """
        super().__init__(
            path=path,
            load_args=load_args,
            save_args=save_args,
            credentials=credentials,
            version_config=version_config,
            back_date=back_date,
        )

    def _first_day_versioned(self) -> str:
        """
        Generate day_to_read variable.

        This is the weekday we are versioning the data.
        """
        day_to_read = (
            pd.to_datetime("today")
            if self._back_date is None
            else pd.to_datetime(self._back_date, format="%Y-%m-%d")
        )
        if self.version_config.get("starting_weekday", None):
            delta = (
                day_to_read.weekday() - self.version_config["starting_weekday"]
            ) % 7
        else:
            delta = 0
        day_to_read = day_to_read - pd.Timedelta(days=delta)
        return self._transform_to_timestamp(day_to_read).normalize()

    def _generate_formatted_path(self) -> str:
        """Formats the path based on the date_path and date_file placeholders."""
        self._rises_if_unformatted()
        self.day_to_read = self._first_day_versioned()
        formatted_path = self._format_wrapper(self.path)
        return formatted_path

    def _format_wrapper(self, path: str) -> str:
        return path.format(
            **self._path_formater(path, "date_path"),
            **self._path_formater(path, "date_file"),
        )

    def _path_formater(self, path: str, to_format: str) -> dict[str, str]:
        if f"{to_format}" in path:
            fmt = self.version_config.get(to_format)
            if fmt is None:
                raise ValueError(
                    f"""{to_format} must be provided in
                    version_config if it's in the path"""
                )
            return {f"{to_format}": self.day_to_read.strftime(fmt)}
        return {}

    def _rises_if_unformatted(self):
        placeholders = set(re.findall(r"\{(.*?)\}", self.path))
        placeholders.add("date_path")
        placeholders.add("date_file")
        if len(placeholders) > 2:
            placeholders.remove("date_path")
            placeholders.remove("date_file")
            raise ValueError(f"placeholders {placeholders} are not allowed in the path")
        return

    def _save(
        self, data: Union[pd.DataFrame, dict], path: Optional[str] = None
    ) -> None:
        """
        Save function for VersionedDataset.

        It calls the _generate_formatted_path method to format the path
        """
        formatted_path = self._generate_formatted_path()
        super()._save(data, formatted_path)  # type: ignore
        return

    def _load(self, path: Optional[str] = None) -> pd.DataFrame:
        """Load method for VersionedDataset.

        It calls the _generate_formatted_path method to format the path
        """
        formatted_path = self._generate_formatted_path()
        df = super()._load(formatted_path)
        return df


class PathReader(BaseDataset):
    """Reads all files in the given path."""

    def __init__(
        self,
        path: str,
        read_args: Optional[dict[str, Any]] = None,
        load_args: Optional[dict[str, Any]] = None,
        credentials: Optional[dict] = None,
        back_date: Optional[str] = None,
    ) -> None:
        """Initialize the class.

        Args:
            path (str): Path to the file.
            read_args (dict): Arguments to be passed to the read function.
                It's the arguments to specify how to read the files in the given path.
                The arguments are:
                time_scale: str[D, M, Y], the time scale to be
                used in the history_length argument.
                history_length: int, the number of days, months or years to
                read, based on time_scale.
                starting_weekday: int, the starting weekday to
                use as reference to read data.
        """
        self.read_args = self._if_read_args_is_none(read_args)
        super().__init__(
            path=path, load_args=load_args, credentials=credentials, back_date=back_date
        )

    def _if_read_args_is_none(
        self, read_args: Optional[dict[str, Any]]
    ) -> dict[str, Any]:
        """Raises an error if the read_args is None."""
        if read_args is None:
            return {}
        self._transform_load_config()
        return read_args

    def _validate_load_config(self) -> str:
        """Validates the time_scale argument in the read_args."""
        current_time_scale = self.read_args.get("time_scale", None)
        if current_time_scale is None:
            raise ValueError("time_scale must be provided in yml file")
        return current_time_scale

    def _transform_load_config(self, read_args: dict) -> None:
        """Transforms the time_scale to the pandas time scale."""
        time_scale_map = {"D": "days", "M": "months", "Y": "years"}
        current_time_scale = self._validate_load_config()
        read_args["time_scale"] = time_scale_map.get(current_time_scale, "days")
        return

    def _is_within_date_range(
        self,
        path: str,
        first_day: pd.Timestamp,
        last_day: pd.Timestamp,
        date_format: str,
        date_pattern: str,
    ) -> bool:
        """Checks if the file in the given path is within the date range."""
        date_str = self._get_date_from_pattern(path, date_pattern=date_pattern)
        date_str = self._transform_to_timestamp(date_str, format=date_format)
        return first_day >= date_str >= last_day

    def _get_paths(self) -> list[str]:
        path_list = self._filesystem.find(self.path)
        if path_list is False:
            raise ValueError(
                f"""No files found in the given path
                please check if it's correct: {self.path}"""
            )
        if self.read_args:
            path_list = self._filter(path_list)
        return path_list

    def _filter(self, path_list: list[str]) -> list[str]:
        """
        Gets the files in the given path that are within the date range.

        generate the first day based on the back_date, if provided.
        if not provided, it will generate the first day based on the
        starting_weekday provided in the version_config.
        """
        date_pattern, date_format = self._check_and_get_patterns(path_list)
        first_day = self._generate_first_day(date_format=date_format)
        last_day = self._generate_last_day(first_day)
        path_list = [
            path
            for path in path_list
            if self._is_within_date_range(
                path, first_day, last_day, date_format, date_pattern
            )
        ]
        if not path_list:
            raise ValueError("No files found in the given date range")
        return path_list

    def _to_pandas_dataframe(self, path: str) -> pd.DataFrame:
        """Reads the file in the given path and returns a pandas dataframe."""
        path = self._add_protocol_to_path(path)
        df = super()._load(path=path)
        return df

    def _load(self, path: Optional[str] = None) -> pd.DataFrame:
        """
        Load method for PathReader. Reads all files in the given path.

        If the thread_count argument is provided in the read_args, it will
        use the ThreadPoolExecutor to read the files in parallel.
        """
        if self.read_args.get("thread_count") is None:
            dfs = map(self._to_pandas_dataframe, self._get_paths())
            return pd.concat(dfs, ignore_index=True)

        with ThreadPoolExecutor(
            max_workers=self.read_args.get("thread_count")
        ) as executor:
            dfs = pd.concat(
                executor.map(self._to_pandas_dataframe, self._get_paths()),
                ignore_index=True,
            )
        logging.info(f"Loaded {self.path}")
        return dfs

    def _save(self):
        """
        No save method for PathReader.

        Could be implemented in the future.
        """
        raise NotImplementedError


class LoadLast(BaseDataset):
    """Reads the last file in the given path."""

    def __init__(
        self,
        path: str,
        load_args: Optional[dict] = None,
        credentials: Optional[dict] = None,
        save_args: Optional[dict] = None,
        back_date: Optional[str] = None,
    ) -> None:
        """Initialize the class.

        Args:
            path (str): Path to the file.
            load_args (dict): Arguments to be passed to the read function.
                If it's trying to read a .parquet file, this are the arguments
                that pandas accepts in the read_parquet function.
                If not provided, the default arguments will be used, an
                empty dict will be used.
                credentials (dict): Credentials to access the file, if not provided
                the default credentials will be used, an empty dict will be used.
                The default value here is in case the file is in the local machine.
                save_args (dict): Arguments to be passed to the write function. If
                it's trying to write a .parquet file, this are the arguments that
                pandas accepts in the to_parquet function. If not provided, the
                default arguments will be used, an empty dict will be used.
                back_date (str): Date to be used in the versioned dataset.
        """
        super().__init__(
            path=path,
            load_args=load_args,
            credentials=credentials,
            save_args=save_args,
            back_date=back_date,
        )

    def _get_last_from_path(self) -> str:
        """Returns the path of the most recent file in the given path."""
        path_list = self._filesystem.find(self.path)
        date_pattern, date_format = self._check_and_get_patterns(path_list)
        date_dict = {
            self._get_date_from_pattern(path, date_pattern): path for path in path_list
        }
        if self._back_date:
            date = max(filter(self._lower_than_back_date, date_dict.keys()))
        else:
            date = max(date_dict.keys())
        return date_dict.get(date, self._raise_if_none())

    def _raise_if_none(self) -> ValueError:
        """Raises an error if the file is not found."""
        msg = "" if self._back_date is None else f" given {self._back_date}"
        return ValueError(f"Couldn't fild the most recent file given {self.path}" + msg)

    def _lower_than_back_date(self, date: str) -> bool:
        """Checks if the date is lower than the back date."""
        return date <= self._back_date  # type: ignore

    def _load(self, path: Optional[str] = None) -> pd.DataFrame:
        """Load method for LoadLast. Reads the last file in the given path."""
        path = self._get_last_from_path()
        path = self._add_protocol_to_path(path)
        df = super()._load(path)  # type: ignore
        return df

    def _save(self):
        """Save method for LoadLast. It will only support read method."""
        raise ValueError("Save method not implemented for LoadLast")

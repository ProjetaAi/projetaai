# flake8: noqa: E501
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Any, Union
from kedro_projetaai.utils.datasets.path_patterns import (
    match_date_pattern,
    return_last_match,
)
import pandas as pd
import re
import logging
from kedro.io import AbstractDataSet
from fsspec.utils import infer_storage_options
from copy import deepcopy
from kedro_projetaai.utils.datasets.pickle_methods import pickle_load, pickle_dump

# TODO: default load_config and save_config.
# TODO: passar tudo isso para o projetaai de alguma maneira.
# Save versioned datasets


class DatasetTypes(Enum):
    parquet = (pd.read_parquet, pd.DataFrame.to_parquet)
    csv = (pd.read_csv, pd.DataFrame.to_csv)
    xlsx = (pd.read_excel, pd.DataFrame.to_excel)
    json = (pd.read_json, pd.DataFrame.to_json)
    pickle = (pickle_load, pickle_dump)

    def read(self, path, *args, **kwargs):
        read_func = self.value[0]
        return read_func(path, *args, **kwargs)

    def write(self, df, *args, **kwargs):
        write_func = self.value[1]
        return write_func(df, *args, **kwargs)

    @classmethod
    def _missing_(cls, value):
        choices = list(cls.__members__.keys())
        raise ValueError(
            "%r is not a valid %s, please choose from %s"
            % (value, cls.__name__, choices)
        )


class BaseDataset(AbstractDataSet):

    """base class for all datasets in projetaai"""

    @property
    def _filesystem(self):

        """
        função do filesystem para ser usada na leitura
        do lake e escrita no lake em outras funções.
        """

        from fsspec import filesystem  # pylint: disable=import-outside-toplevel

        return filesystem(self.protocol, **self.credentials)

    def __init__(
        self,
        path: str = None,
        load_args: dict = None,
        credentials: dict = None,
        save_args: dict = None,
        version_config: dict = None,
        back_date=None,
    ) -> None:

        """
        Inicialização da classe
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

        """
        default formating for the class
        """
        if self.protocol != "file":
            self._storage_options = deepcopy(self.credentials)
            self.remove_account_url_from_path()
        else:
            self._storage_options = {}

    def format_back_date(self, back_date: Union[str, None]) -> Union[str, None]:

        """
        making sure that the functions here read
        correctly the back_date format
        kedro yml does not support "null" as a value
        therefore we use '' to represent None
        """

        if back_date == "":
            return None
        return back_date

    def remove_account_url_from_path(self):

        """
        had problems with fsspec and kedro
        this fixes it
        """

        account_name = self.credentials["account_name"]
        account_url = f"{account_name}.dfs.core.windows.net/"

        if account_url in self.path:
            self.path = self.path.replace(account_url, "")

        return

    def _generate_first_day(self) -> pd.Timestamp:
        today = (
            pd.to_datetime("today")
            if self._back_date is None
            else pd.to_datetime(self._back_date, format="%Y-%m-%d")
        )
        if self.version_config.get("starting_weekday") is None:
            days_difference = 0
        else:
            days_difference: int = (today.weekday() - self.version_config["starting_weekday"]) % 7  # type: ignore
        last_specific_day = today - pd.Timedelta(days=days_difference)
        return last_specific_day

    def file_manager(self, path: str = None):
        if path == None:
            path = self.path
        try:
            return DatasetTypes[self.get_file_extension(path)]
        except:
            raise ValueError(
                f"File extension not supported: {self.get_file_extension(path)}"
            )

    def get_file_extension(self, path: str):
        return path.split(".")[-1]

    def _dtypes_with_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        if isinstance(df, pd.DataFrame):
            return df.astype(self.dtypes)
        return df

    def _save(self, df: pd.DataFrame, path: str) -> None:
        file_manager = self.file_manager(path)
        file_manager.write(
            df=self._dtypes_with_pandas(df),
            path=path,
            **self.save_args,
            storage_options=self._storage_options,
        )
        return

    def _load(self, path: str) -> pd.DataFrame:
        file_manager = self.file_manager(path)
        df = file_manager.read(
            path=path, **self.load_args, storage_options=self._storage_options
        )
        return self._dtypes_with_pandas(df)

    def _describe(self) -> dict[str, Any]:
        return dict(path=self.path, protocol=self.protocol)

    def _get_date_from_pattern(self, path) -> str:
        _, self.date_pattern = match_date_pattern(path)
        matched_value = return_last_match(self.date_pattern, path)
        if matched_value is None:
            raise ValueError(f"Date pattern not found in {path}")
        return matched_value.replace("/", "-")

    def _get_dtypes_from_load_args(self, load_args: dict):
        if "dtypes" in load_args:
            dtypes = load_args.pop("dtypes")
            if not isinstance(dtypes, dict):
                raise ValueError("dtypes must be a dict")
            return dtypes
        return {}


class ReadFile(BaseDataset):  # VendasDataSet

    """
    to read kedro parquets with
    a little bit more of convenience
    """

    def __init__(
        self,
        path: str,
        credentials: dict[str, str] = None,
        load_args: dict[str, Any] = None,
        save_args: dict[str, Any] = None,
    ) -> None:

        """
        initialize the class
        """

        super().__init__(
            path=path,
            load_args=load_args,
            save_args=save_args,
            credentials=credentials,
        )

    def _load(self) -> pd.DataFrame:

        """
        subscribe the load function
        """
        df = super()._load(self.path)
        logging.info(f"Loaded {self.path}")
        return df


class VersionedDataset(BaseDataset):  # VendasVersionedDataset

    """
    this class abstracts the "abstracrversioneddataset" from kedro
    to extend it to the necessities of projetaai
    """

    def __init__(
        self,
        path: str,
        credentials: dict,
        load_args: dict[str, Any] = None,  # type: ignore
        version_config: dict[str, Any] = None,
        back_date=None,  # type: ignore
        save_args: dict[str, Any] = None,
    ) -> None:

        """
        initialize the class
        """
        super().__init__(
            path=path,
            load_args=load_args,
            save_args=save_args,
            credentials=credentials,
            version_config=version_config,
            back_date=back_date,
        )

    def _generate_formatted_path(self) -> str:
        """
        format the dates in the provided path
        """
        self._rises_if_unformatted()
        self.first_day = self._generate_first_day()
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
                    f"{to_format} must be provided in version_config if it's in the path"
                )
            return {f"{to_format}": self.first_day.strftime(fmt)}
        return {}

    def _rises_if_unformatted(self):
        placeholders = re.findall(r"\{(.*?)\}", self.path)
        placeholders = set(placeholders)
        placeholders.add("date_path")
        placeholders.add("date_file")
        if len(placeholders) > 2:
            placeholders.remove("date_path")
            placeholders.remove("date_file")
            raise ValueError(f"placeholders {placeholders} are not allowed in the path")
        return

    def _save(self, data: Union[pd.DataFrame, dict]) -> None:

        """
        overwrite the save function
        """
        formatted_path = self._generate_formatted_path()
        super()._save(data, formatted_path)
        return

    def _load(self) -> pd.DataFrame:
        """
        overwrite the load function
        """
        formatted_path = self._generate_formatted_path()
        df = super()._load(formatted_path)
        return df


class PathReader(BaseDataset):

    """
    abstract the "abstractdataset" from kedro
    to read multiple files in a folder
    """

    def __init__(
        self,
        path: str,
        read_args: dict[str, Any] = None,
        load_args: dict[str, Any] = None,
        credentials: dict = None,
        back_date: str = None,
    ) -> None:

        self.read_args = self.raise_if_read_args_is_none(read_args)

        super().__init__(
            path=path, load_args=load_args, credentials=credentials, back_date=back_date
        )
        self._transform_load_config()

    def raise_if_read_args_is_none(self, read_args):
        if read_args is None:
            raise ValueError(
                """read_args must be provided in yml file \n
                            with the following arguments: \n
                            time_scale, history_length"""
            )
        return read_args

    def _validate_load_config(self) -> str:

        """
        validate necessary arguments
        in load_args
        """

        current_time_scale = self.read_args.get("time_scale", None)
        if current_time_scale is None:
            raise ValueError("time_scale must be provided in yml file")
        return current_time_scale

    def _transform_load_config(self):

        """
        transform the time_scale
        """

        time_scale_map = {"D": "days", "M": "months", "Y": "years"}
        current_time_scale = self._validate_load_config()
        self.read_args["time_scale"] = time_scale_map.get(current_time_scale, "days")
        return

    def _transform_to_timestamp(self, date_str: str) -> pd.Timestamp:
        return pd.to_datetime(date_str, format="%Y-%m-%d")

    def _is_within_date_range(
        self, path: str, first_day: pd.Timestamp, last_day: pd.Timestamp
    ):

        """
        checks if the date is within the date range
        """

        date_str = self._get_date_from_pattern(path)
        date_str = self._transform_to_timestamp(date_str)
        return first_day >= date_str >= last_day

    def _get_paths(self) -> list[str]:
        path_list = self._filesystem.find(self.path)
        if path_list is False:
            raise ValueError(
                f"No files found in the given path please check if it's correct: {self.path}"
            )
        path_list = self._filter(path_list)
        return path_list

    def _filter(self, path_list: list[str]):

        """
        filter the path_list
        with the given date range
        """

        first_day = self._generate_first_day().normalize()
        last_day = self._generate_last_day(first_day)
        path_list = [
            path
            for path in path_list
            if self._is_within_date_range(path, first_day, last_day)
        ]
        if not path_list:
            raise ValueError("No files found in the given date range")
        return path_list

    def _generate_last_day(self, first_day: pd.Timestamp) -> pd.Timestamp:

        """
        generate the last day
        based on the first day
        """

        last_day = first_day - pd.DateOffset(
            **{self.read_args["time_scale"]: self.read_args["history_length"]}
        )
        if self.version_config.get("starting_weekday") is None:
            days_difference = 0
        else:
            days_difference: int = (last_day.weekday() - self.version_config["starting_weekday"]) % 7  # type: ignore
        last_day = last_day - pd.Timedelta(days=days_difference)
        return last_day.normalize()

    def _to_pandas_dataframe(self, path: str) -> pd.DataFrame:

        """
        read the file
        """
        df = super()._load(path=path)
        return df

    def _load(self) -> pd.DataFrame:

        """
        override the load method
        to allow parallelization
        """
        if self.read_args.get("thread_count") is None:
            dfs = map(self._to_pandas_dataframe, self._get_paths())
            dfs = pd.concat(dfs, ignore_index=True)

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
        no save method
        for now its read only
        """

        raise NotImplementedError


class LoadLast(BaseDataset):
    def __init__(
        self,
        path: str,
        load_args: dict = None,
        credentials: dict = None,
        save_args: dict = None,
        version_config: dict = None,
        back_date=None,
    ) -> None:

        super().__init__(
            path, load_args, credentials, save_args, version_config, back_date
        )

    def _get_last_from_path(self) -> str:
        path_list = self._filesystem.find(self.path)
        date_dict = {self._get_date_from_pattern(path): path for path in path_list}
        if self._back_date:
            date = max(filter(self._lower_than_back_date, date_dict.keys()))
        else:
            date = max(date_dict.keys())
        return date_dict.get(date, self._raise_if_none())

    def _raise_if_none(self):
        msg = "" if self._back_date is None else f" given {self._back_date}"
        return ValueError(f"Couldn't fild the most recent file given {self.path}" + msg)

    def _lower_than_back_date(self, date: str) -> bool:
        return date <= self._back_date

    def _load(self) -> pd.DataFrame:
        path = self._get_last_from_path()
        df = super()._load(path)
        return df

    def _save(self):
        return ValueError("Save method not implemented for LoadLast")

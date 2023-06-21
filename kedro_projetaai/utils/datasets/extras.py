# flake8: noqa: E501
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any, Union
import pandas as pd
import re
import logging
import json
from kedro_datasets.pandas.parquet_dataset import ParquetDataSet
from kedro.io import AbstractDataSet, AbstractVersionedDataSet
from kedro.io.core import (
    Version,
    parse_dataset_definition)
from copy import deepcopy

# TODO: default load_config and save_config.
# TODO: passar tudo isso para o projetaai de alguma maneira.
# Save versioned datasets


class BaseDataset:

    """ base class for all datasets in projetaai """
    _df: pd.DataFrame = None # type: ignore

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @df.setter
    def df(self, df: pd.DataFrame) -> None:
        if isinstance(df, pd.DataFrame):
            self._df = self.set_dtypes(df)
        if self.load_args.get('columns'):
            self._df = self._df[self.load_args['columns']]
        return

    @df.getter
    def df(self) -> pd.DataFrame:
        return self._df

    @property
    def _filesystem(self):

        """
        função do filesystem para ser usada na leitura
        do lake e escrita no lake em outras funções.
        """

        from fsspec import filesystem  # pylint: disable=import-outside-toplevel
        return filesystem(self.protocol, **self.credentials)

    def __init__(self, load_args: dict, credentials: dict, save_args: dict=None,  # type: ignore
                  path: str = None, filepath: str = None,  # type: ignore
                  version_config: dict=None, back_date=None) -> None:  # type: ignore

        """
        Inicialização da classe
        """

        self.path = path
        self.version_config = version_config
        self.filepath = filepath
        self.save_args = save_args
        self.load_args = load_args
        self.credentials = credentials
        self._back_date = self.format_back_date(back_date)
        self.default_formating()

    def default_formating(self):

        """
        default formating for the class
        """

        from fsspec.utils import infer_storage_options
        self.check_and_set_path_filepath()
        self.generate_and_check_dtypes_dict()
        self.remove_account_url_from_path()
        self.get_version()
        self.protocol = infer_storage_options(self.path)["protocol"]

    def format_back_date(self, back_date: Union[str, None]) -> Union[str, None]:

        """
        making sure that the functions here read
        correctly the back_date format
        kedro yml does not support "null" as a value
        therefore we use '' to represent None
        """

        if back_date == '':
            return None
        return back_date

    def get_version(self):

        """
        verifica se o dataset é versionado e pega a versão
        """

        if self.version_config:
            self.versioned = self.version_config.pop('versioned', None)
        return

    def remove_account_url_from_path(self):

        """
        had problems with fsspec and kedro
        this fixes it
        """

        account_name = self.credentials['account_name']
        account_url = f"{account_name}.dfs.core.windows.net/"

        if account_url in self.path:
            self.path = self.path.replace(account_url, "")

        return

    def generate_and_check_dtypes_dict(self):

        """
        dtypes is a dict that will be used to cast the dataframe
        """

        self.dtypes = (self.load_args.pop('dtypes')
                       if 'dtypes' in self.load_args.keys() else {})
        if not isinstance(self.dtypes, dict):
            raise ValueError(f"dtypes must be a dict not {type(self.dtypes)}")
        return

    def check_and_set_path_filepath(self):

        """
        defaults filepath and path to path
        just for convenience
        """

        if self.path and self.filepath:
            raise ValueError("path and filepath can't be used together")
        if self.filepath:
            self.path = self.filepath
        return

    def set_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:

        """
        apply dtypes to dataframe
        """

        if self.dtypes:
            return df.astype(self.dtypes)
        else:
            return df

    def _generate_first_day(self):
        today = pd.to_datetime('today') if self._back_date is None else pd.to_datetime(self._back_date, format='%Y-%m-%d')
        if self.load_args.get('starting_weekday'):
            days_difference = (today.weekday() - self.load_args['starting_weekday']) % 7
        else:
            days_difference = 0
        last_specific_day = today - pd.Timedelta(days=days_difference)
        return last_specific_day


class ReadParquet(ParquetDataSet, BaseDataset): #VendasDataSet

    """
    to read kedro parquets with
    a little bit more of convenience
    """

    def __init__(self, filepath: str,
                 credentials: dict[str, str],
                 load_args: dict[str, Any] = {},
                 save_args: dict[str, Any] = None,  # type: ignore
                 version: Version = None) -> None:

        """
        initialize the class
        """

        BaseDataset.__init__(self, filepath=filepath,
                             load_args=load_args,
                             save_args=save_args,
                             credentials=credentials)

        super().__init__(self.path, self.load_args,
                         self.save_args, version,
                         self.credentials)

    def _load(self) -> pd.DataFrame:

        """
        subscribe the load function
        """

        self.df = super()._load()
        logging.info(f"Loaded {self.path}")
        return self.df

class VersionedDataset(AbstractVersionedDataSet, BaseDataset): #VendasVersionedDataset

    """
    this class abstracts the "abstracrversioneddataset" from kedro
    to extend it to the necessities of projetaai
    """

    def __init__(self, filepath: str, credentials: dict,
                 dataset: dict,
                 load_args: dict[str, Any] = {},  # type: ignore
                 version_config: dict[str, Any] = None, back_date=None, # type: ignore
                 save_args: dict[str, Any] = {}) -> None:

        """
        initialize the class
        """

        BaseDataset.__init__(self, filepath=filepath,
                             load_args=load_args,
                             save_args=save_args,
                             credentials=credentials,
                             version_config=version_config,
                             back_date=back_date)

        super().__init__(
            filepath=self.path, # type: ignore
            version=None,  # You can provide a version if needed
        )
        if back_date == '':
            back_date = None
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)



    def get_existing_versions(self):

        """
        get existing versions of the dataset
        """

        path, filename = self.path.split("/")
        name, file_extension = filename.split(".")
        glob_result = self._filesystem.glob(path)
        versions = [i for i in glob_result if i.startswith(name) and i.endswith(file_extension)]
        versions.sort()
        return versions

    def _format_filepath_date(self) -> str:

        """
        format the dates in the provided path
        """

        formatted_filepath = self.path
        date_formats = {}

        if "{date_path}" in formatted_filepath:
            date_formats["date_path"] = self.version_config["date_path"]
        if "{date_file}" in formatted_filepath:
            date_formats["date_file"] = self.version_config["date_file"]


        if date_formats:
            first_day = self._generate_first_day() # type: ignore
            formatted_dates = {key: first_day.strftime(fmt) for key, fmt in date_formats.items()}
            formatted_filepath = formatted_filepath.format(**formatted_dates)
        if "{" in formatted_filepath and "}" in formatted_filepath:
            raise ValueError(f'''Unsupported format string in filepath: {formatted_filepath}, \n
                             Only supported are "date_path" to specify the date in the path and
                             "date_file" to specify the date in the filename.''')

        return formatted_filepath

    # def _format_filepath_version(self) -> str:
    #     versions = self.get_existing_versions()
    #     if self.versioned:

    #     return

    def _save(self, data: Union[pd.DataFrame, dict]) -> None:

        """
        overwrite the save function
        """

        formatted_filepath = self._format_filepath_date()
        bytes_buffer = BytesIO()
        if isinstance(data, pd.DataFrame):
            if formatted_filepath.endswith('parquet'):
                data.to_parquet(bytes_buffer, **self.save_args)
            elif formatted_filepath.endswith('csv'):
                data.to_csv(bytes_buffer, **self.save_args)
            elif formatted_filepath.endswith('xlsx'):
                data.to_excel(bytes_buffer, **self.save_args)
        elif isinstance(data, dict):
            if formatted_filepath.endswith('json'):
                json_dumps = json.dumps(data, **self.save_args)
                bytes_buffer = BytesIO(json_dumps.encode())
        else:
            formatted_filepath_error_message = formatted_filepath.split('.')[-1]
            raise ValueError(f'File extension not supported: {formatted_filepath_error_message}')
        with self._filesystem.open(formatted_filepath, "wb") as file:
            file.write(bytes_buffer.getvalue())


        # Save the data to the formatted_filepath
        # ...

        return

    def _load(self):

        """
        overwrite the load function
        """

        kwargs = {}
        kwargs['filepath'] = self._format_filepath_date()
        kwargs['credentials'] = deepcopy(self.credentials)
        df = self._dataset_type(**kwargs).load() # type: ignore
        if self.path.endswith('json'):
            return df
        if self.load_args.get('columns', None) is not None:
            df = df[self.load_args['columns']]
        logging.info(f"Loaded {self.path}")
        return df

    def _describe(self) -> dict[str, Any]:
        return dict(filepath=self.path, protocol=self.protocol)


class FileReader(AbstractDataSet, BaseDataset):

    """
    abstract the "abstractdataset" from kedro
    to read multiple files in a folder
    """

    def __init__(self, path: str,
                 credentials: dict,
                 dataset: dict,
                 load_args: dict,
                 back_date:str = None) -> None: # type: ignore
        if back_date == '':
            back_date = None  # type: ignore

        BaseDataset.__init__(self, path=path,
                             load_args=load_args,
                             credentials=credentials,
                             back_date=back_date)

        if self.path is None:
            raise ValueError("Must provide `path`")
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)
        self._transform_load_config()

    def _validate_load_config(self) -> str:

        """
        validate necessary arguments
        in load_args
        """

        current_time_scale = self.load_args.get('time_scale', None)
        if current_time_scale is None:
            raise ValueError('time_scale must be provided in yml file')
        return current_time_scale

    def _transform_load_config(self):

        """
        transform the time_scale
        """

        time_scale_map = {
            'D': 'days',
            'M': 'months',
            'Y': 'years'
        }
        current_time_scale = self._validate_load_config()
        self.load_args['time_scale'] = time_scale_map.get(current_time_scale, 'days')
        return

    def _get_date_source(self):

        """
        defaults to path
        for convenience
        """

        if  '.' in self.path.split('/')[-1]:
            return 'filename'
        else:
            return 'path'

    def _get_date_string(self, path: str) -> pd.Timestamp:

        """
        checks the path for a date
        supports the following formats:
        folder/folder/yyyy/mm/dd/file.csv
        or
        folder/file_yyyy-MM-dd.csv
        """

        date_source = self._get_date_source()#self.load_args.get('date_source', 'path')

        if date_source == 'path':
            date_patterns = [
                (r'\d{4}/\d{2}/\d{2}', '%Y/%m/%d'),
                (r'\d{4}/\d{2}', '%Y/%m'),
                (r'\d{4}', '%Y'),
            ]

            for pattern, date_format in date_patterns:
                date_match = re.search(pattern, path)
                if date_match:
                    date_str = date_match.group()
                    date_obj = pd.to_datetime(date_str, format=date_format)
                    return date_obj
        elif date_source == 'filename':
            file_name = path.split('/')[-1]
            date_patterns = [
                (r'\d{8}', '%Y%m%d'),
                (r'\d{6}', '%Y%m'),
                (r'\d{4}', '%Y'),
            ]

            for pattern, date_format in date_patterns:
                date_match = re.search(pattern, file_name)
                if date_match:
                    date_str = date_match.group()
                    date_obj = pd.to_datetime(date_str, format=date_format)
                    return date_obj

        raise ValueError('No date found in path')

    def _is_within_date_range(self, path: str,
                              first_day: pd.Timestamp,
                              last_day: pd.Timestamp):

        """
        checks if the date is within the date range
        """

        date_str = self._get_date_string(path)
        return first_day >= date_str >= last_day

    def _filter(self, path_list: list[str]):

        """
        filter the path_list
        with the given date range
        """

        first_day = self._generate_first_day().normalize()
        last_day = self._generate_last_day(first_day)
        path_list = [path for path in path_list if self._is_within_date_range(path, first_day, last_day)]
        if not path_list:
            raise ValueError('No files found in the given date range')
        return path_list

    def _generate_last_day(self, first_day: pd.Timestamp) -> pd.Timestamp:

        """
        generate the last day
        based on the first day
        """

        last_day = first_day - pd.DateOffset(**{self.load_args['time_scale']: self.load_args['history_length']})
        last_day = last_day - pd.Timedelta(days=last_day.weekday() + 1)
        return last_day.normalize()

    def _to_pandas_dataframe(self, path: str):

        """
        read the file
        """

        # arrumar p deixar mais organizado
        kwargs = {}
        kwargs['filepath'] = f'{self.protocol}://{path}'
        kwargs['credentials'] = deepcopy(self.credentials)
        # essa função está rodando paralelizada
        # não pode colocar o getter aqui
        df = self._dataset_type(**kwargs).load() # type: ignore
        if self.load_args.get('columns'):
            df = df[self.load_args['columns']]
        if self.dtypes:
            df = df.astype(self.dtypes)
        return df

    def _load(self) -> pd.DataFrame:

        """
        override the load method
        to allow parallelization
        """

        path_list = self._filesystem.find(self.path)
        path_list = self._filter(path_list)
        #dfs = pd.concat(map(self._to_pandas_dataframe, path_list))
        if path_list == []:
            raise ValueError('No files found in the given date range')
        with ThreadPoolExecutor(max_workers=self.load_args.get('thread_count', 12)) as executor:
            dfs = pd.concat(executor.map(self._to_pandas_dataframe, path_list),
                            ignore_index=True)
        logging.info(f"Loaded {self.path}")
        return dfs

    def _save(self):

        """
        no save method
        for now its read only
        """

        return NotImplementedError

    def _describe(self):

        """
        default describe method
        """

        return dict(filepath=self.path,
                    protocol=self.protocol)
import logging
from pathlib import Path
import shutil
import requests
from zipfile import ZipFile
from tqdm import tqdm

from operators.base import BaseOperator


RETROSHEET_URL = "https://retrosheet.org/downloads/alldata.zip"


class RetrosheetToStorage(BaseOperator):

    def __init__(self, url: str=RETROSHEET_URL, delete_zipfile: bool=True, 
                 data_dir: str | Path=None, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.delete_zipfile = delete_zipfile
        self.data_dir = Path(data_dir) if data_dir else Path.cwd() / 'data'
        self.target_dir = self.data_dir / 'retrosheet'
        self.zipfile_path = self.target_dir / 'retrosheet_data.zip'

    def execute(self) -> None:
        self.download_data()
        self.extract_data()
        if self.delete_zipfile:
            self.zipfile_path.unlink()

    def download_data(self) -> None:
        if self.target_dir.exists():
            logging.info('Deleting previous version of data...')
            shutil.rmtree(self.target_dir)
        self.target_dir.mkdir(parents=True)
        logging.info('Getting retrosheet data...')
        with requests.get(self.url, stream=True)as response, \
          self.zipfile_path.open('wb') as file:
            response.raise_for_status()
            progress_bar = tqdm(unit="B", unit_scale=True, desc='Downloading')
            for chunk in response.iter_content(chunk_size=None):
                file.write(chunk)
                progress_bar.update(len(chunk))
        logging.info('Retrosheet zip file downloaded.')

    def extract_data(self) -> None:
        logging.info('Extracting retrosheet data...')
        with ZipFile(self.zipfile_path.resolve()) as unzipper:
            for file_info in unzipper.infolist():
                if file_info.is_dir():
                    continue
                file_path = self.target_dir / file_info.filename
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with unzipper.open(file_info) as src, file_path.open('wb') as dest:
                    logging.debug(f'Unzipping file to {str(file_path)}')
                    dest.write(src.read())
                    logging.debug(f'Successfully unzipped file to {str(file_path)}')
        logging.info('Retrosheet data extracted.')


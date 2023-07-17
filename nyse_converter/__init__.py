import os
import glob
import pandas
import uuid
from dask import dataframe as dd
import logging

def main():
    log_file_path = os.environ['log_path']
    logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(levelname)s %(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
    logging.info('Data conversion started')
    SRC_DIR = os.environ['src_dir']
    SRC_FILE_P = os.environ.setdefault('src_file','NYSE*.txt.gz')
    SRC_FILE_FORMAT = sorted(glob.glob(f'{SRC_DIR}/{SRC_FILE_P}'))
    #data/nyse_all/nyse_data/nyse*.txt.gz
    TGT_FILE_FORMAT = [i.replace('nyse_data','nyse_json').replace('txt','json') for i in SRC_FILE_FORMAT]
    data = dd.read_csv(SRC_FILE_FORMAT, names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume'], blocksize=None)
    logging.info('Data Created,it will be written to json')
    data.to_json(TGT_FILE_FORMAT,
                orient='records',
                lines=True,
                compression = 'gzip',
                name_function = lambda _: str(uuid.uuid1())
                )
    logging.info('Data Conversion Complete')
if __name__ == '__main__':
    main()
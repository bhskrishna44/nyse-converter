import os
import glob
import pandas
import uuid
from dask import dataframe as dd

def main():
    SRC_DIR = os.environ['src_dir']
    SRC_FILE_PATTERN = os.environ.setdefault('src_file','NYSE*.txt.gz')
    SRC_FILE_FORMAT = sorted(glob.glob(f'{SRC_DIR}/{SRC_FILE_PATTERN}'))
    TGT_FILE_FORMAT = [i.replace('nyse_data','nyse_json').replace('txt','json') for i in SRC_FILE_FORMAT]
    data = dd.read_csv(SRC_FILE_FORMAT, names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume'], blocksize=None)
    print('Data Created,it will to written to json')
    data.to_json(TGT_FILE_FORMAT,
                orient='records',
                lines=True,
                compression = 'gzip',
                name_function = lambda _: str(uuid.uuid1())
                )
    

                    
if __name__ == '__main__':
    main()
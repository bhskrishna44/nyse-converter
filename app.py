import os
import glob
import pandas
import uuid

def main():
    SRC_DIR = os.environ['src_dir']
    TGT_DIR = os.environ['tgt_dir']
    os.makedirs(f'{TGT_DIR}')
    for i in glob.glob(f'{SRC_DIR}/*'):
        data = pandas.read_csv(i, names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume'])
        data.to_json(f'{TGT_DIR}/part-{str(uuid.uuid1())}.json.gz',
                    orient='records',
                    lines=True
        )
        print(f'number of records in {os.path.split(i)[1]} are {data.shape}')

if __name__ == '__main__':
    main()
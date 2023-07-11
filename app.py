import os
import glob
import pandas
import uuid

def main():
    os.makedirs('data/nyse_all/nyse_json')
    for i in glob.glob('data/nyse_all/nyse_data/*'):
        data = pandas.read_csv(i, names=['ticker', 'trade_date', 'open_price', 'low_price', 'high_price', 'close_price', 'volume'])
        data.to_json(f'data/nyse_all/nyse_json/part-{str(uuid.uuid1())}.json.gz',
                    orient='records',
                    lines=True
        )
        print(f'number of records in {os.path.split(i)[1]} are {data.shape}')

if __name__ == '__main__':
    main()
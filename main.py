import pandas as pd
import requests
import logging
import json
import argparse
from google.cloud import storage
from google.cloud.storage import Blob
from bs4 import BeautifulSoup


logging.getLogger().setLevel(logging.INFO)


class ExcelWrokbookStructure:
    def __init__(self, year, skip_rows, base_url):
        self.year = year
        if year < 2013:
            self.file_name \
                = '4abff3/globalassets/marketdata-excel-files/elspot-prices_' + str(year) + '_monthly_sek.xls'
        elif year < 2021:
            self.file_name \
                = '492034/globalassets/marketdata-excel-files/elspot-prices_' + str(year) + '_monthly_sek.xls'
        else:
            self.file_name = '49224c/globalassets/marketdata-excel-files/elspot-prices_' + str(year) + '_daily_sek.xls'
        self.url = base_url + self.file_name
        self.skip_rows = skip_rows


class StorageItem:
    def __init__(self, dataframe, meta_data):
        self.meta_data = meta_data
        self.df = dataframe


def open_file_url(url, skip_rows=None):
    logging.info("open_file_url(...): url={}".format(url))
    return requests.get(url).content


def open_file(filename):
    f = open(filename, 'r')
    return f.read()


def upload_blob_string(bucket_name, csv_string, destination_blob_name, metadata):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    year = metadata['year']
    blob = Blob(destination_blob_name, bucket)
    if blob.metadata is None:
        blob.metadata = metadata
    else:
        blob.metadata.update(metadata)
    return blob.upload_from_string(
        data=csv_string,
        content_type='text/csv')


def parse_html(html, skip_rows):
    soup = BeautifulSoup(html, 'lxml')  # Parse the HTML as a string
    table = soup.find_all('table')[0]  # Grab the first table
    new_table = pd.DataFrame(columns=range(100), index=range(400))  # I know the size
    row_marker = 0
    for row in table.find_all('tr'):
        column_marker = 0
        columns = row.find_all('td')
        for column in columns:
            new_table.iat[row_marker, column_marker] = column.get_text()
            column_marker += 1
        row_marker += 1

    df = new_table\
        .dropna(axis=1, how='all')\
        .dropna(axis=0, how='all')
    meta_data = {}
    meta_data['file_top_rows'] = df.head(skip_rows)
    df.drop(range(skip_rows), inplace=True)             # Drop na columns (axis=1), Drop na rows (axis=0)
    column_names = df.iloc[0]
    column_names[0] = 'DATE'
    df = df.rename(columns=column_names)
    df = df.drop(skip_rows)                     # Now row 2 is the top row and contains col headers
    return StorageItem(df,meta_data)


def run(request):
    logging.info("Starting NordpoolSpotPriceHistoryExcelLoader")
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help='json with parameters')
    args = parser.parse_args()
    logging.info('run(...): requestr={}'.format(request))
    logging.info('run(...): args={}'.format(args))
    if request == None:
        logging.info('run(...): data=' + args.data)
        input_json = json.loads(str(args.data).replace("'",""))
    else:
        input_json = request.get_json()

    logging.info("request.args: {}".format(input_json))
    bucket_name = input_json['bucket_target']
    file_location = input_json['file_location']
    destination_blob_name = input_json['destination_blob_name']
    logging.info("\nbucket_name: {}\nfile_location: {}\ndestination_blob_name: {}".format(
        bucket_name, file_location, destination_blob_name))
    file_location_2021 = 'https://www.nordpoolgroup.com/'
    workbooks = [ExcelWrokbookStructure(2000, 2, file_location),
                 ExcelWrokbookStructure(2001, 2, file_location),
                 ExcelWrokbookStructure(2002, 2, file_location),
                 ExcelWrokbookStructure(2003, 2, file_location),
                 ExcelWrokbookStructure(2004, 2, file_location),
                 ExcelWrokbookStructure(2005, 2, file_location),
                 ExcelWrokbookStructure(2006, 2, file_location),
                 ExcelWrokbookStructure(2007, 2, file_location),
                 ExcelWrokbookStructure(2008, 2, file_location),
                 ExcelWrokbookStructure(2009, 2, file_location),
                 ExcelWrokbookStructure(2010, 2, file_location),
                 ExcelWrokbookStructure(2011, 2, file_location),
                 ExcelWrokbookStructure(2012, 2, file_location),
                 ExcelWrokbookStructure(2013, 3, file_location),
                 ExcelWrokbookStructure(2014, 2, file_location),
                 ExcelWrokbookStructure(2015, 2, file_location),
                 ExcelWrokbookStructure(2016, 2, file_location),
                 ExcelWrokbookStructure(2017, 2, file_location),
                 ExcelWrokbookStructure(2018, 2, file_location),
                 ExcelWrokbookStructure(2019, 2, file_location),
                 ExcelWrokbookStructure(2020, 2, file_location),
                 ExcelWrokbookStructure(2021, 2, file_location_2021)
                 ]

    for wb in workbooks:
        s = open_file_url(wb.url)
        item = parse_html(s, wb.skip_rows)
        item.meta_data['year'] = wb.year
        upload_blob_string(
            bucket_name,
            item.df.to_csv(index=False, sep=';'),
            destination_blob_name + str(wb.year), item.meta_data)
        logging.info("Uploaded size={} to bucket {} and {}".format(item.df.size, bucket_name, destination_blob_name))


if __name__ == '__main__':
    run(None)
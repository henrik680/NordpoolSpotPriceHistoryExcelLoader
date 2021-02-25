from main import *
import xlrd
import pandas as pd
from bs4 import BeautifulSoup



def hist_table_from_html(html, year):
    logging.debug("Started hist_table_from_html year={}".format(year))
    #soup = BeautifulSoup(html, "html.parser")
    soup = BeautifulSoup(html, "lxml")
    tables = soup.findAll('table')
    for table in tables:
        length = len(table.findAll('tr'))
        logging.debug("length={}".format(length))
        #if length in [55,56]:
        df_list = pd.read_html(str(table))
        if len(df_list) == 1:
            df = df_list[0]
            print(df.head(10))
            # df = df_list[0]\
            #     .dropna(axis=0, how='all')\
            #     .drop(columns=[2,3,4,5])\
            #     .drop(0)\
            #     .assign(Year=year)\
            #     .rename(columns={0: 'Week', 1: 'Spot'})
            #df.iat[0, 2] = 'Year'
            logging.debug(df)
            return df

def test1(file):
    df = pd.read_html(file)[0]
    print(df.head(10))
    print(df.columns[1])
    s = df\
        .rename(columns={df.columns[1]: 'Date'})\
        .to_csv(index=False, sep=';')
    #book = xlrd.open_workbook(file)
    #print("The number of worksheets is {}".format(book.nsheets))
    print(s)


def test2(file,skip_rows):
    logging.info('{}'.format(file))
    f = open(file, 'r')
    s = f.read()
    #hist_table_from_html(s,2020)
    return parser(s,skip_rows)




def func1():
    workbooks = [ExcelWrokbookStructure(2013,3),
         ExcelWrokbookStructure(2015,2),
         ExcelWrokbookStructure(2020,2)]
    df = None
    for wb in workbooks:
        file = wb.file_name
        if df is None:
            df = test2(wb.file_name, wb.skip_rows)
        else:
            df.reset_index()
            #df.append(test2(wb.file_name, wb.skip_rows))
            df_sub = test2(wb.file_name, wb.skip_rows)
            df_sub.reset_index()
            print(df_sub.columns.values)
            df = pd.concat((df,df_sub))
            #df.append(df_sub, axis=0)
    print(df.to_csv(index=False, sep=';'))


def func2():
    wb = ExcelWrokbookStructure(2013,3)
    item = test2(wb.file_name,wb.skip_rows)
    print(item.meta_data)


#run(123)
#test1()
func2()
from dataclasses import dataclass
import os
import pandas as pd
import xlrd
import googlemaps
import json
'''
pandas version 1.x 이상 endcoding='UTF-8' 불필요
ImportError: Missing optional dependency 'xlrd'. 
pip install xlrd 주의!! anaconda install xlrd 하면 에러 발생
TEST
'''

@dataclass
class FileReader:
    # def __init__(self, context, fname, train, test, id, label):
    #     self._context = context  # _ 1개는 default 접근, _ 2개는 private 접근

    # 3.7부터 간소화되서 dataclass 데코 후, key: value 형식으로 써도 됨 (롬복 형식)
    context : str = ''
    fname: str = ''
    train: object = None
    test: object = None
    id : str = ''
    lable : str = ''

    def new_file(self) -> str:
        return os.path.join(self.context,self.fname)

    def csv_to_dframe_utf_8(self) -> object:
        return pd.read_csv(self.new_file(), encoding='utf-8', thousands=',', engine='python')

    def csv_to_dframe_euc_kr(self) -> object:
        return pd.read_csv(self.new_file(), encoding='euc-kr', thousands=',', engine='python')

    def xls_to_dframe(self, header, usecols) -> object:
        print(f'PANDAS VERSION: {pd.__version__}')
        return pd.read_excel(self.new_file(), header = header, usecols = usecols)

    def create_gmaps(self):
        return googlemaps.Client(key='')

    def json_load(self):
        return json.load(open(self.new_file(), encoding='UTF-8'))

class FileChecker:
    def df_null_check(self, df):
        columns_list = []
        for i in df.columns:        # column list 만들기
            columns_list.append(i)

        print(columns_list)

        for i in range(0, len(columns_list)):   # 각 column의 null값(= 비어 있는 값) 조회 & 프린트
            null_count = df[columns_list[i]].isnull().sum(axis=0)
            print(f'{columns_list[i]:25} : null count = {null_count:6}')
        
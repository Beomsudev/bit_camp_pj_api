import os

from com_dayoung_api.utils.file_helper import FileReader
import pandas as pd
from pathlib import Path

"""
title:영화제목
genre:장르
country:제작국가
year:제작년도
company:제작회사
director:감독
actor:배우
date:개봉일
running_time:상영시간
keyword:키워드
plot:줄거리
"""
class MovieService:
    def __init__(self):
        self.fileReader = FileReader()  
        self.path = os.path.abspath("")

    def hook(self):
        data = self.new_model()
        return data

    def new_model(self) -> object:
        path = os.path.abspath("")
        fname = '\data\kmdb_csv_modify4.csv'
        data = pd.read_csv(path + fname, encoding='utf-8')
        
        # print(data)
        # print('***********')
        # data = data.head()
        print(data)

        return data

    # def get_data(self):
    #     reader = self.reader
    #     reader.context = os.path.join(baseurl,'data')
    #     reader.fname = 'kmdb_csv_modify4.csv'
    #     reader.new_file()
    #     data = reader.csv_to_dframe_euc_kr()
    #     data = data.head()
    #     print(data)
    #     return data

if __name__ == "__main__":
    service = MovieService()
    service.hook()

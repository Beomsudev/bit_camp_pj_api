from typing import List
import json
import pandas as pd
import os
import sys
import urllib.request
import csv
import time
from pandas import DataFrame
from pathlib import Path
from com_dayoung_api.utils.file_helper import FileReader

from flask import request, jsonify
from flask_restful import Resource, reqparse

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from com_dayoung_api.ext.db import db, openSession

from com_dayoung_api.resources.reco_movie import RecoMovieDao, RecoMovieDto


class MovieDto(db.Model):
    
    __tablename__ = 'movies'
    __table_args__= {'mysql_collate':'utf8_general_ci'}

    # ,movieid,title,subtitle,description,imageurl,year,rating
    movieid : str = db.Column(db.String(10), primary_key = True, index = True)
    title : str = db.Column(db.String(30))
    subtitle : str = db.Column(db.String(30))
    description : str = db.Column(db.String(200))
    imageurl : str = db.Column(db.String(100))
    year : str = db.Column(db.String(5))
    rating : float = db.Column(db.Float)

    def __init__(self,movieid,title,subtitle,description,imageurl,year,rating):
        self.movieid = movieid
        self.title = title
        self.subtitle = subtitle
        self.description = description
        self.imageurl = imageurl
        self.year = year
        self.rating = rating

    def __repr__(self):
        return f'Movie(movieid=\'{self.movieid}\',\
            title=\'{self.title}\',\
            subtitle=\'{self.subtitle}\',\
            description=\'{self.description}\',\
            imageurl=\'{self.imageurl}\',\
            year=\'{self.year}\',\
            rating=\'{self.rating}\',)'

    def json(self):
        return {
            'movieid' : self.movieid,
            'title' : self.title,
            'subtitle' : self.subtitle,
            'description' : self.description,
            'imageurl' : self.imageurl,
            'year' : self.year,
            'rating' : self.rating
        }

class MovieVo:
    movieid: str = ''
    title: str = ''
    subtitle: str = ''
    description: str = ''
    imageurl: str = ''
    year: str = ''
    rating: float = 0.0

Session = openSession()
session = Session()
class MovieDao(MovieDto):

    @classmethod
    def count(cls):
        return cls.query.count()

    @classmethod
    def find_all(cls):
        print('find_all')
        sql = cls.query
        df = pd.read_sql(sql.statement, sql.session.bind)
        return json.loads(df.to_json(orient='records'))

    @classmethod
    def find_by_title(cls, title):
        print('##### find title #####')
        return session.query(MovieDto).filter(MovieDto.title.like(title)).all()

    @classmethod
    def find_by_id(cls, movieid):
        print('find_by_id')
        return cls.query.filter_by(movieid == movieid)

    # @classmethod
    # def login(cls, movie):
    #     sql = cls.query\
    #         .filter(cls.movieid.like(movie.movieid))\
    #         .filter(cls.password.like(movie.password))
    #     df = pd.read_sql(sql.statement, sql.session.bind)
    #     print('==================================')
    #     print(json.loads(df.to_json(orient='records')))
    #     return json.loads(df.to_json(orient='records'))
            
    # movieid,title,subtitle,description,imageurl,year,rating
    @staticmethod
    def register_movie(movie):
        print('##### new movie data registering #####')
        print(movie)
        newMovie = MovieDao(movieid = movie['movieid'],
                            title = movie['title'],
                            subtitle = movie['subtitle'],
                            description = movie['description'],
                            imageurl = movie['imageurl'],
                            year = movie['year'],
                            rating = movie['rating'])
        session.add(newMovie)
        session.commit()
        print('##### new movie data register complete #####')


    @staticmethod   
    def insert_many():
        print('insert_many')
        service = NaverMovie()
        df = service.hook()
        print(df.head())
        session.bulk_insert_mappings(MovieDto, df.to_dict(orient="records"))
        session.commit()
        session.close()

# update [table] set [field] = '변경값' where = '조건값'
# session.query(테이블명).filter(테이블명.필드명 == 조건 값).update({테이블명.필드명:변경 값})

    @staticmethod
    def modify_movie(movie):
        print('##### movie data modify #####')
        
        Session = openSession()
        session = Session()
        print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        session.query(MovieDto).filter(MovieDto.movieid == movie['movieid']).update({MovieDto.title:movie['title'],
                                                                                    MovieDto.subtitle:movie['subtitle'],
                                                                                    MovieDto.description:movie['description'],
                                                                                    MovieDto.year:movie['imageurl'],
                                                                                    MovieDto.rating:movie['year'],
                                                                                    MovieDto.imageurl:movie['rating']})                                                        
        session.commit()
        print('##### movie data modify complete #####')

    @classmethod
    def delete_movie(cls,movieid):
        print('##### movie data delete #####')
        data = cls.query.get(movieid)
        db.session.delete(data)
        db.session.commit()
        print('##### movie data delete complete #####')

# ==============================================================
# ==============================================================
# ====================     Service  ============================
# ==============================================================
# ==============================================================

'''
10/23 rev1.0
5가지 추가 필요
중복 데이터의 제거 기능
    - 제목, 년도, 감독 의 값이 동일하면 df에서 제거 (csv는 유지? )
null 값의 대체 입력 기능
    - ui 단에서 사진이 짤려서 나와서 사진 크기에 영향 줌
정리하고 주석 달기 
    - 파일 변경 같은 부분(주석 달린 부분) filehelper로 옮기기
merge_df 단에서 네이버 영화 df를 읽어오지 말고 csv로 읽어오기?
    - 항상 api를 검색하면 시간이 오래걸림 csv를 만들고 df로 읽어오게 변환
검색 정확도 높이기
    - 특수 문자, 한자 등 제거 하여 검색 정확도 높이기?
    - 검색 안되는 값이 주로 옜날 영화이니 year컬럼 기준으로 index삭제 하기 (ex 1980 이전은 삭제)
검색 delay 주기
    - 검색시에 데이터가 너무 많고, 집에 인터넷이 느린지 에러가 발생
    - c 언어 처럼 delay를 줄 수 있는지 찾아 보기
bit pj api로 옮기기 (파일명 search ??)
    - bit pj로 옮기고 실행 마무리 하기 (최종 단계) / git api 아이디 비번 삭제!
'''

class NaverMovie:
    def __init__(self):
        self.fileReader = FileReader()  
        self.path = os.path.abspath("")
        self.client_id = "Bgv4VR6y66LHzAHuv9kF"                         # 네이버 api id (git 삭제!!!)
        self.client_secret = "484CDN61ws"                               # 네이버 api secret (git 삭제!!!)

    def hook(self):
        print('*'*10, 'START', '*'*10)
        
        # kmdb_df = self.read_kmdb_csv()                                  # step1 : kmdb_csv 읽어와서 df으로 전환 (return : df)
        # kmdb_movie_title_list = self.get_title_list(kmdb_df)            # step2 : 검색을 위한 제목 list 추출 (return : list)
        # kmdb_movie_year_list = self.get_year_list(kmdb_df)              # step3 : 검색을 위한 년도 list 추출 (return : list)

        # print('*'*30)

        # movie_dict = self.search_naver_movie(kmdb_movie_title_list,\
        #                                         kmdb_movie_year_list)   # step4 : 네이버 영화 api 검색 진행 (return : dict)
        # naver_movie_df = self.dict_to_dataframe(movie_dict)             # step5 : 네이버 영화 검색 dict df 전환 및 csv 저장 (return : df)
        # merge_df = self.merge_df(kmdb_df, naver_movie_df)               # step6 : ui json 생성을 위한 kmdb_csv(df)와 네이버 영화 검색(df) merge (retrun : df)
        # ui_df = self.df_to_ui_json(merge_df)                                    # step7 : merge_df의 column 삭제 및 정렬 후 json파일 저장
        
        path = os.path.abspath("")
        fname = '\\com_dayoung_api\\resources\\saved_data\\ui_df.csv'
        ui_df = pd.read_csv(path + fname, encoding='utf-8')
        print(ui_df)
        return ui_df
        print('*'*10, 'E N D', '*'*10)

    def read_kmdb_csv(self):
        path = os.path.abspath("")
        fname = '\data\kmdb_csv.csv'
        kmdb_df = pd.read_csv(path + fname, encoding='utf-8')
        return kmdb_df

    def get_title_list(self, data):
        kmdb_movie_title_list = []
        for i in data['title']:
            kmdb_movie_title_list.append(i)

        return kmdb_movie_title_list

    def get_year_list(self, data):
        kmdb_movie_year_list = []
        for i in data['year']:
            kmdb_movie_year_list.append(i)

        return kmdb_movie_year_list

    def search_naver_movie(self, kmdb_movie_title_list, kmdb_movie_year_list):
        titlelist = []    # kmdb의 영화 이름이 저장될 list
        yearlist = []     # kmdb의 영화 년도가 저장될 list
        real_dict = {}    # 네이버영화의 json값을 저장할 dict
        movie_index = 0   # kmdb csv의 인덱스
        ui_json_id = 100  # ui_json의 인덱스    
        print('검색 시작!!!!!!!!!!!!!!!!!')

        for kmdb_year in kmdb_movie_year_list:
            yearlist.append(kmdb_year)

        for kmdb_title in kmdb_movie_title_list:    # kmdb title로 naver title 검색
            titlelist.append(kmdb_title)
            
            time.sleep(0.1) # 네트워크가 느린 환경일 경우 검색 지연 적용 / 빠르면 주석 처리

            print('*'*30)           
            print(f'영화 인덱스 : {movie_index}번')
            print(f'검색 제  목 : {kmdb_title}') # 검색 영화 제목
            print(f'검색 연  도 : {yearlist[movie_index]}') # 검색 영화 연도

            encText = urllib.parse.quote(kmdb_title)
            display = "&display=100"
            yearfrom = (f"&yearfrom={yearlist[movie_index]}")
            yearto = (f"&yearto={yearlist[movie_index]}")
            url = "https://openapi.naver.com/v1/search/movie.json?query=" \
                                                                            + encText \
                                                                            + display \
                                                                            + yearfrom \
                                                                            + yearto
            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id",self.client_id)
            request.add_header("X-Naver-Client-Secret",self.client_secret)
            response = urllib.request.urlopen(request)
            rescode = response.getcode()
            
            if(rescode==200):
                response_body = response.read().decode('utf-8')
                jsondata = json.loads(response_body)
                items = jsondata['items']
                temp_dict = {}
                
                if items == []: # kmdb의 영화제목이 네이버 영화에 없는 경우
                    real_data = {'title':'none',
                                'link':'none',
                                'image':'../images/none_image.jpg',
                                'subtitle':'none',
                                'pubDate':'none',
                                'director':'none',
                                'actor':'none',
                                'userRating':'0',
                                'id':ui_json_id}                                    
                    temp_dict[movie_index] = real_data
                    real_dict.update(temp_dict)

                else:           # kmdb의 영화제목이 네이버 영화에 있는 경우
                    i = 0
                    
                    for i in range(len(items)):    # dict에 네이버 영화 정보 추가
                        titles = items[i]['title']
                        titles_drop1 = titles.replace('<b>', '').replace('</b>', '')  # <b>, </b>삭제
                        titles_drop2 = titles_drop1.replace(' ', '')                  # 공백 제거
                        images = items[i]['image']
                        titlelist_drop1 = titlelist[movie_index].replace(' ', '')     # kmdb제목 공백 제거
                        real_data = []
                        
                        '''
                        case0: kmdb의 영화 제목이 네이버에 없는 경우
                        case1: 영화 image 없음, 제목 일  치 (공백 포함)
                        case2: 영화 image 없음, 제목 일  치 (공백 제거)
                        case3: 영화 image 없음, 제목 불일치
                        case4: 영화 image 있음, 제목 일  치 (공백 포함)
                        case5: 영화 image 있음, 제목 일  치 (공백 제거)
                        case6: 영화 image 있음, 제목 불일치
                        '''

                        if images == "":                                                 # 네이버에 영화 image가 없는 경우
                            if titles_drop1 == titlelist[movie_index]:                   # kmdb제목과 naver 제목(drop1) 일치 확인
                                real_data = items[i]                                     # naver 영화 json의 value값 저장
                                real_data['image'] = '../images/none_image.jpg'          # none_image.jpg로 표시
                                userrating_naver = float(real_data['userRating'])/2      # 별점 10점 만점 -> 5점 만점
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                             # ui의 image card 인덱스 값
                                temp_dict[movie_index] = real_data                       # {kmdb의 title값 : naver영화 json value}
                                real_dict.update(temp_dict)
                                print('case1')
                            elif titles_drop2 == titlelist_drop1:                        # kmdb제목과 naver 제목(drop2) 일치 확인
                                real_data = items[i]                                     
                                real_data['image'] = '../images/none_image.jpg'                    
                                userrating_naver = float(real_data['userRating'])/2      
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                             
                                temp_dict[movie_index] = real_data                       
                                real_dict.update(temp_dict)
                                print('case2')
                            else:
                                real_data = items[i]                                     
                                real_data['image'] = '../images/none_image.jpg'                    
                                userrating_naver = float(real_data['userRating'])/2      
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                             
                                temp_dict[movie_index] = real_data                       
                                real_dict.update(temp_dict)
                                print('case3')
                        else:                                                            # 네이버에 영화 image가 없는 경우
                            if titles_drop1 == titlelist[movie_index]:               
                                real_data = items[i]                                    
                                userrating_naver = float(real_data['userRating'])/2  
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                           
                                temp_dict[movie_index] = real_data                       
                                real_dict.update(temp_dict)
                                print('case4')
                            elif titles_drop2 == titlelist_drop1:                 
                                real_data = items[i]                                     
                                userrating_naver = float(real_data['userRating'])/2      
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                             
                                temp_dict[movie_index] = real_data                       
                                real_dict.update(temp_dict)
                                print('case5')
                            else:
                                real_data = items[i]                                     
                                userrating_naver = float(real_data['userRating'])/2      
                                userrating_naver = '%0.1f' % float(userrating_naver)
                                real_data['userRating'] = userrating_naver              
                                real_data['id'] = ui_json_id                             
                                temp_dict[movie_index] = real_data                       
                                real_dict.update(temp_dict)
                                print('case6')        
                                                    
                        # if 3 end
                    i += 1
                    # inner for end
                # if 2 end
                ui_json_id += 100    
            else:
                print("Error Code:" + rescode)
            # if 1 end
            movie_index += 1
        # outter for end
        print('*'*30)
        print('검색 끝!!!!!!!!!!!!!!!!!!')
        # print(real_dict)
        return real_dict

    def dict_to_dataframe(self, movie_dict): # -> filehelper로 옮기기 / movie_dict는 전역값으로 받아오게 하기?
        print('*****to dataframe 진행*****')
        myframe = DataFrame(movie_dict)

        # title, link, image, subtitle, pubDate, director, actor, userRating
        navermovie_index = \
            {'title':'title_naver',
            'link':'link_naver',
            'image':'image_naver',
            'subtitle':'subtitle_naver',
            'pubDate':'pubdate_naver',
            'director':'director_naver',
            'actor':'actor_naver',
            'userRating':'userrating_naver'}               
        myframe = myframe.rename(index=navermovie_index)
        myframe = myframe.T
        # print(myframe)
        # myframe.to_csv('test.csv', encoding='utf-8', index=True)    # -> filehelper로 옮기기
        print('*****to dataframe 완료*****')        
        return myframe

    def merge_df(self, kmdb_df, naver_movie_df):
        print('*****df merge 진행*****')
        merge_df = pd.concat([kmdb_df, naver_movie_df],axis=1)
        # merge_df.to_csv('merge_test.csv', encoding='utf-8')   # -> filehelper로 옮기기
        # print(merge_df)
        print('*****df merge 완료*****')        
        return merge_df

    def df_to_ui_json(self, merge_df):
        print('*****to json 진행*****')

        '''
        ***** 현재 columns *****
        title,
        genre,
        country,
        year,
        company,
        director,
        actor,
        date,
        running_time,
        keyword,
        plot,
        title_naver,
        link_naver,
        image_naver,
        subtitle_naver,
        pubdate_naver,
        director_naver,
        actor_naver,
        userrating_naver
        '''
        '''
        ***** 만들 json sample *****
        {
        "id": 900,
        "title": "Resident Evil",
        "subtitle": "Vendetta",
        "description": "Chris Redfield enlists the help of Leon S. Kennedy and Rebecca Chambers to stop a death merchant, with a vengeance, from spreading a deadly virus in New York.",
        "year": 2014,
        "imageUrl": "../images/담보.jpg",
        "rating": 4.2
        }
        '''

        # 필요 없는 column drop
        drop_df = merge_df.drop \
                    (['country', 
                    'company', 
                    'director', 
                    'actor', 
                    'date', 
                    'running_time',
                    'plot', 
                    'title_naver', 
                    'link_naver',  
                    'subtitle_naver', 
                    'pubdate_naver', 
                    'director_naver', 
                    'actor_naver'], axis=1)
        
        # ui json 형식에 맞게 sorting
        sort_df = drop_df[['id', 'title', 'genre', 'keyword', 'image_naver', 'year', 'userrating_naver']]
        
        ui_json_columns = {
            'id':'movieid',
            'genre':'subtitle',
            'keyword':'description',
            'image_naver':'imageurl',
            'userrating_naver':'rating'
        }
        # ui json과 동일하게 colunm 이름 변경
        rename_df = sort_df.rename(columns=ui_json_columns)
        fill_na_df = rename_df.fillna('none')
        print(fill_na_df)
        path = os.path.abspath("")
        path2 = '\saved_data\\'
        fname = 'ui_df.csv'
        fill_na_df.to_csv(path + path2 + fname, encoding='utf-8', index=True)
        # ui_json = DataFrame.to_json(fill_na_df, orient = 'records', force_ascii=False, indent=8)
        # print(ui_json)
        # save_json = json.loads(ui_json)                                       # --> filehelper로 옮기기
        # with open('movies.json', 'w', encoding='utf-8') as make_json:         # --> 강사님 수정 위해 json 변환 막음
        #     json.dump(save_json, make_json, ensure_ascii=False, indent="\t")  # --> 강사님 수정 위해 json 변환 막음
        print('*****to json 완료*****')
        return fill_na_df

if __name__ == "__main__":
    test = NaverMovie()
    test.hook()

# ==============================================================
# ==============================================================
# =================     Controller  ============================
# ==============================================================
# ==============================================================

# movieid,title,subtitle,description,imageurl,year,rating
# parser = reqparse.RequestParser()
# parser.add_argument('movieid', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('title', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('subtitle', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('description', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('imageurl', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('year', type=str, required=True, help='This field should be a movieid')
# parser.add_argument('rating', type=float, required=True, help='This field should be a movieid')

class Movie(Resource):

    @staticmethod
    def post():
        parser = reqparse.RequestParser()
        parser.add_argument('movieid', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('title', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('subtitle', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('description', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('imageurl', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('year', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('rating', type=float, required=True, help='This field should be a movieid')         
        args = parser.parse_args()
        print(args)
        movies = MovieDto(args['movieid'], \
                        args['title'], \
                        args['subtitle'], \
                        args['description'], \
                        args['imageurl'], \
                        args['year'], \
                        args['rating'])
        print('*********')
        print(f'{args}')
        try:
            MovieDao.register_movie(args)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the movie'}, 500

    @staticmethod
    def get(id: str):
        print('##### get #####')
        print(id)
        try:
            reco_movie = MovieDao.find_by_title(id)
            data = reco_movie.json()
            print(data)
            return data, 200
        except:
            print('fail')
            return {'message':'Title not found'}, 404

    @staticmethod
    def put():
        parser = reqparse.RequestParser()
        parser.add_argument('movieid', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('title', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('subtitle', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('description', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('imageurl', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('year', type=str, required=True, help='This field should be a movieid')
        parser.add_argument('rating', type=float, required=True, help='This field should be a movieid')  
        print('putputputputputputputput')
        args = parser.parse_args()
        print(args)
        movies = MovieDto(args['movieid'], \
                        args['title'], \
                        args['subtitle'], \
                        args['description'], \
                        args['imageurl'], \
                        args['year'], \
                        args['rating'])
        print('*********')
        print(f'{args}')
        
        try:
            print('************!!!!!!!!!!!!!!!!!!!***')
            MovieDao.modify_movie(args)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the movie'}, 500

class MovieDel(Resource):

    @staticmethod
    def post():
        parser = reqparse.RequestParser()
        parser.add_argument('movieid', type=str, required=True, help='This field should be a movieid')     
        args = parser.parse_args()
        print('*********')
        print(f'{args}')
        print('*********')
        movieid = args['movieid']
        print(movieid)

        try:
            MovieDao.delete_movie(movieid)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the movie'}, 500



class Movies(Resource):
    
    def post(self):
        md = MovieDao()
        md.insert_many('movies')

    def get(self):
        print('========== movie10 ==========')
        data = MovieDao.find_all()
        return data, 200

class MovieSearch(Resource):
    def get(self, title):
        print("SEARCH 진입")
        print(f'타이틀 : {title}')
        movie = RecoMovieDao.find_by_title(title)
        # review = {review[i]: review[i + 1] for i in range(0, len(review), 2)}
        # review = json.dump(review)
        movielist = []
        # for review in reviews:
            # reviewdic
        for rev in movie:
            movielist.append(rev.json())
        # print(f'Review type : {type(review[0])}')
        print(f'Review List : {movielist}')
        return movielist[:]

# class Auth(Resource):

#     def post(self):
#         body = request.get_json()
#         movie = MovieDto(**body)
#         MovieDao.save(movie)
#         id = movie.movieid
        
#         return {'id': str(id)}, 200 


# class Access(Resource):

#     def post(self):
#         print('========== movie6 ==========')
#         args = parser.parse_args()
#         movie = MovieVo()
#         movie.movieid = args.movieid
#         # movie.password = args.password
#         print(movie.movieid)
#         # print(movie.password)
#         data = MovieDao.login(movie)
#         return data[0], 200



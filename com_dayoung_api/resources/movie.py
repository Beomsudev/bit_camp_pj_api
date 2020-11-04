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
from com_dayoung_api.utils.file_helper import FileReader, FileChecker

from flask import request, jsonify
from flask_restful import Resource, reqparse

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from com_dayoung_api.ext.db import db, openSession

from com_dayoung_api.resources.reco_movie import RecoMovieDao, RecoMovieDto


class MovieDto(db.Model):

    __tablename__ = 'movies'
    __table_args__ = {'mysql_collate':'utf8_general_ci'}

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
        # df = service.hook()
        # print(df.head())
        # session.bulk_insert_mappings(MovieDto, df.to_dict(orient="records"))
        # session.commit()
        # session.close()

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
# ====================  UI DF 가공  ============================
# ==============================================================
# ==============================================================
class MovieRatingDf:
    def __init__(self):
        self.fileReader = FileReader()  
        self.filechecker = FileChecker()
        self.path = os.path.abspath("")

    def hook(self):
        print('***** 무비 렌즈 UI용 DF가공 시작 *****')

        movie_lens_rating_df = self.read_movie_lens_rating_csv()
        arrange_movie_lens_rating_df = self.arrange_movie_lens_rating_df(movie_lens_rating_df)

        print('***** 무비 렌즈 UI용 DF가공 완료 *****')

        return arrange_movie_lens_rating_df
    def read_movie_lens_rating_csv(self):
        print('***** 무비렌즈 평점 데이터 읽기*****')
        # path = os.path.abspath("")
        # fname = '\com_dayoung_api\\resources\data\movie_lens\ratings_small.csv'
        path = os.path.abspath("")
        fname = '\data\movie_lens\\ratings_small.csv'
        movie_lens_meta_df = pd.read_csv(path + fname, encoding='utf-8')
        print('***** 무비렌즈 평점 데이터 읽기 완료*****')
        return movie_lens_meta_df

    def arrange_movie_lens_rating_df(self, movie_lens_keyword_df):
        print('***** 무비렌즈 레이팅 데이터 가공 *****')
        '''
        [original columns]
        'userId',
        'movieId',
        'rating',
        'timestamp'
        '''

        ##### 필요없는 column 삭제 #####
        drop_df = movie_lens_keyword_df.drop(['timestamp'], axis=1)
        print(drop_df)
        print(drop_df.columns)
        ##### 필요없는 column 삭제 #####

        ##### 데이터 축소 #####
        '''
        userid : 70 번 까지 (10000 row)
        (원본 데이터 : 10만 건)
        '''
        reduction_df = drop_df[(drop_df['userId'] < 71)]
        ##### 데이터 축소 #####

        ##### column 정렬 및 이름 변경 #####
        column_sort_df = reduction_df[['userId', 'movieId', 'rating']]

        mycolumns = {
            'userId':'userid',
            'movieId':'movieid',
            'rating':'rating'
        }

        sort_df = column_sort_df.rename(columns=mycolumns)
        ##### column 정렬 및 이름 변경 #####

        final_movie_lens_rating_df = sort_df
        print('***** 무비렌즈 레이팅 데이터 가공 완료 *****')
        return final_movie_lens_rating_df


if __name__ == "__main__":
    test = MovieRatingDf()
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



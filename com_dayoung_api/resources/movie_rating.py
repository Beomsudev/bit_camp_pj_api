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
from sqlalchemy import func

from com_dayoung_api.ext.db import db, openSession

from com_dayoung_api.resources.reco_movie import RecoMovieDao, RecoMovieDto


class MovieRatingDto(db.Model):

    __tablename__ = 'movie_ratings'
    __table_args__ = {'mysql_collate':'utf8_general_ci'}

    # 'userid', 'movieid', 'rating'
    ratingid : int = db.Column(db.Integer, primary_key = True, index = True)
    userid : int = db.Column(db.Integer)
    movieid : int = db.Column(db.Integer)
    rating : float = db.Column(db.Float)

    # item_id: int = db.Column(db.Integer, db.ForeignKey(ItemDto.item_id))


    def __init__(self,ratingid,userid,movieid,rating):
        self.ratingid = ratingid
        self.userid = userid
        self.movieid = movieid
        self.rating = rating

    def json(self):
        return {
            'ratingid' : self.ratingid,
            'userid' : self.userid,
            'movieid' : self.movieid,
            'rating' : self.rating
        }

class MovieRatingVo:
    ratingid: int = 0
    userid: str = ''
    movieid: str = ''
    rating: float = 0.0


Session = openSession()
session = Session()
class MovieRatingDao(MovieRatingDto):

    @staticmethod
    def bulk():
        print('***** [movie_rating] df 삽입 *****')
        m = MovieRatingDf()
        df = m.hook()
        print(df)
        session.bulk_insert_mappings(MovieRatingDto, df.to_dict(orient='records'))
        session.commit()
        session.close()
        print('***** [movie_rating] df 삽입 완료 *****')

    @classmethod
    def count(cls):
        return session.query(func.count(MovieRatingDto.ratingid)).one()

    @classmethod
    def find_all(cls):
        print('find_all')
        sql = cls.query
        df = pd.read_sql(sql.statement, sql.session.bind)
        return json.loads(df.to_json(orient='records'))

    @staticmethod
    def find_by_id(ratingid):
        print('##### find id #####')
        return session.query(MovieRatingDto).filter(MovieRatingDto.ratingid.like(ratingid)).all()

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
    def register_rating(rating):
        print('##### new rating data registering #####')
        print(rating)
        newRating = MovieRatingDao(ratingid = rating['ratingid'],
                            userid = rating['userid'],
                            movieid = rating['movieid'],
                            rating = rating['rating'])
        session.add(newRating)
        session.commit()
        db.session.close()
        print('##### new rating data register complete #####')

    # update [table] set [field] = '변경값' where = '조건값'
    # session.query(테이블명).filter(테이블명.필드명 == 조건 값).update({테이블명.필드명:변경 값})

    @staticmethod
    def modify_rating(ratingid):
        print('##### rating data modify #####')
        session.query(MovieRatingDto).filter(MovieRatingDto.ratingid == ratingid['ratingid']).update({MovieRatingDto.rating:ratingid['rating']})                                                        
        session.commit()
        session.close()

        print('##### rating data modify complete #####')

    @classmethod
    def delete_rating(cls, ratingid):
        print('##### rating data delete #####')
        data = cls.query.get(ratingid)
        db.session.delete(data)
        session.commit()
        session.close()
        print('##### rating data delete complete #####')

# ==============================================================
# ==============================================================
# =================     Controller  ============================
# ==============================================================
# ==============================================================


class MovieRating(Resource):
    @staticmethod
    def post():
        parser = reqparse.RequestParser()
        parser.add_argument('ratingid', type=int, required=False, help='This field should be a ratingid')
        parser.add_argument('userid', type=int, required=True, help='This field should be a userid')
        parser.add_argument('movieid', type=int, required=True, help='This field should be a movieid')
        parser.add_argument('rating', type=float, required=True, help='This field should be a rating')    
        args = parser.parse_args()
        ratings = MovieRatingDto(args['ratingid'], \
                        args['userid'], \
                        args['movieid'], \
                        args['rating'])
        print('*********')
        print(args)
        try:
            MovieRatingDao.register_rating(args)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the rating'}, 500

    @staticmethod
    def get(id: str):
        try:
            reco_movie = MovieRatingDao.find_by_id(id)
            data = reco_movie.json()
            print(data)
            return data, 200
        except:
            print('fail')
            return {'message':'Title not found'}, 404

    @staticmethod
    def put():
        parser = reqparse.RequestParser()
        parser.add_argument('ratingid', type=int, required=True, help='This field should be a ratingid')
        parser.add_argument('userid', type=int, required=False, help='This field should be a userid')
        parser.add_argument('movieid', type=int, required=False, help='This field should be a movieid')
        parser.add_argument('rating', type=float, required=True, help='This field should be a rating')    
        args = parser.parse_args()
        ratings = MovieRatingDto(args['ratingid'], \
                        args['userid'], \
                        args['movieid'], \
                        args['rating'])
        print('*********')
        print(args)
        try:
            MovieRatingDao.modify_rating(args)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the rating'}, 500

class MovieRatings(Resource):
    
    def post(self):
        md = MovieRatingDao()
        md.bulk('movies')

    def get(self):
        data = MovieRatingDao.find_all()
        return data, 200

class MovieRatingSearch(Resource):
    def get(self, ratingid):
        print("SEARCH 진입")
        print(f'ratingid : {ratingid}')
        movie = MovieRatingDao.find_by_id(ratingid)
        movielist = []
        for rev in movie:
            movielist.append(rev.json())
        print(f'Review List : {movielist}')
        return movielist[:]

class MovieRatingDel(Resource):

    @staticmethod
    def post():
        parser = reqparse.RequestParser()
        parser.add_argument('ratingid', type=int, required=True, help='This field should be a ratingid')
        args = parser.parse_args()
        print('*********')
        print(args)
        ratingid = args['ratingid']

        try:
            MovieRatingDao.delete_rating(ratingid)
            return{'code':0, 'message':'SUCCESS'}, 200
        except:
            return {'message':'An error occured registering the movie'}, 500

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
        path = os.path.abspath("")
        fname = '\com_dayoung_api\\resources\data\movie_lens\\ratings_small.csv'
        # path = os.path.abspath("")
        # fname = '\data\movie_lens\\ratings_small.csv'
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
        ##### 필요없는 column 삭제 #####

        ##### 데이터 축소 #####
        '''
        userid : 70 번 까지 (10000 row)
        (원본 데이터 : 10만 건)
        '''
        reduction_df = drop_df[(drop_df['userId'] < 71)]
        ##### 데이터 축소 #####

        ##### ratingid column 추가 #####
        mylist = []
        for i in range(0, len(reduction_df['movieId'])):
            mylist.append(i)
        ratingid_column = pd.DataFrame(mylist, columns=['ratingid'])
        reduction_df = pd.concat([reduction_df, ratingid_column], axis=1)
        ##### ratingid column 추가 #####


        ##### column 정렬 및 이름 변경 #####
        column_sort_df = reduction_df[['ratingid', 'userId', 'movieId', 'rating']]

        mycolumns = {
            'ratingid':'ratingid',
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


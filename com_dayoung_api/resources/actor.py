# from typing import List
# from flask import request
# from flask_restful import Resource, reqparse
# import json
# from flask import jsonify
# from com_dayoung_api.ext.db import db, openSession
# import pandas as pd
# import json
# import os
# from com_dayoung_api.utils.file_helper import FileReader
# import pandas as pd
# import numpy as np
# from sqlalchemy import func
# from pathlib import Path

# class ActorDto(db.Model):
#     __tablename__ = 'actors'
#     __table_args__={'mysql_collate':'utf8_general_ci'}
#     # columns=['photoUrl', 'age','name','realName','religion','agency', 'spouse', 'children','debutYear','actorid']
#     actorid: str = db.Column(db.String(30), primary_key = True, index = True)
#     photoUrl: str = db.Column(db.String(200))
#     name: str = db.Column(db.String(30))
#     age: str = db.Column(db.String(30))
#     realName: str = db.Column(db.String(30))
#     religion: str = db.Column(db.String(30))
#     agency: str = db.Column(db.String(30))
#     spouse: str = db.Column(db.String(30))
#     children: str = db.Column(db.String(30))
#     debutYear: int = db.Column(db.Integer)

#     def __init__(self, photoUrl, actorid, name, age, realName, spouse, children, debutYear, agency, religion):
#         self.photoUrl = photoUrl
#         self.actorid = actorid
#         self.name = name
#         self.age = age
#         self.realName = realName
#         self.religion = religion
#         self.agency = agency
#         self.spouse = spouse
#         self.children = children
#         self.debutYear = debutYear

#     @property
#     def json(self):
#         return {
#             'photoUrl' : self.photoUrl,
#             'actorid' : self.actorid,
#             'name' : self.name,
#             'age' : self.age,
#             'realName' : self.realName,
#             'spouse' : self.spouse,
#             'children' : self.children,
#             'debutYear' : self.debutYear,
#             'religion' : self.religion,
#             'agency' : self.agency
#         }
# class ActorVo:
#     actorid: str = ''
#     photoUrl: str = ''
#     age: str = ''
#     name: str = ''
#     realName: str = ''
#     religion: str = ''
#     agency: str = ''
#     spouse: str = ''
#     children: str = ''
#     debutYear: int = 0


# class ActorDao(ActorDto):

#     @classmethod
#     def count(cls):
#         return cls.query.count()

#     @classmethod
#     def find_all(cls):
#         sql = cls.query
#         df = pd.read_sql(sql.statement, sql.session.bind)
#         return json.loads(df.to_json(orient='records'))

#     @classmethod
#     def find_by_name(cls, name):
#         return cls.query.filer_by(name == name)

#     @classmethod
#     def find_by_id(cls, actorid):
#         return cls.query.filter_by(actorid == actorid)


#     # @classmethod
#     # def login(cls, actor):
#     #     sql = cls.query\
#     #         .filter(cls.actorid.like(actor.actorid))\
#     #         .filter(cls.password.like(actor.password))
#     #     df = pd.read_sql(sql.statement, sql.session.bind)
#     #     print(json.loads(df.to_json(orient='records')))
#     #     return json.loads(df.to_json(orient='records'))
            

#     @staticmethod
#     def save(actor):
#         db.session.add(actor)
#         db.session.commit()

#     @staticmethod   
#     def insert_many():
#         service = ActorService()
#         Session = openSession()
#         session = Session()
#         df = service.hook()
#         print(df.head())
#         session.bulk_insert_mappings(ActorDto, df.to_dict(orient="records"))
#         session.commit()
#         session.close()

#     @staticmethod
#     def modify_actor(actor):
#         db.session.add(actor)
#         db.session.commit()

#     @classmethod
#     def delete_actor(cls,id):
#         data = cls.query.get(id)
#         db.session.delete(data)
#         db.session.commit()
    
# """   
# # ==============================================================
# # ==============================================================
# # ====================     Service  ============================
# # ==============================================================
# # ==============================================================
# """

# from com_dayoung_api.resources.crawling import Crawling

# class ActorService:
    
#     def __init__(self):
        
#         actors_name = ['전지현', "이병한", "손예진"]
#         actors_name = ['전지현']
#         actors_name =["수지", "이병헌","전지현","손예진","안소희","강동원","하정우","김혜수","현빈" ,"송강호"]
        
#         self.crawl = Crawling(actors_name) # 이병헌 is given as default
#         # print(self.dataFrame)
        
#     def hook(self):
#         self.dataFrame = self.crawl.crawl()
#         print(self.dataFrame)
#         return self.dataFrame

# # ==============================================================
# # ==============================================================
# # =================     Controller  ============================
# # ==============================================================
# # ==============================================================

# parser = reqparse.RequestParser()  # only allow price changes, no name changes allowed
# parser.add_argument('actorid', type=str, required=True,
#                                         help='This field should be a actorid')

# class Actor(Resource):
#     @staticmethod
#     def post():
#         args = parser.parse_args()
#         print(f'Actor {args["id"]} added ')
#         params = json.loads(request.get_data(), encoding='utf-8')
#         if len(params) == 0:

#             return 'No parameter'

#         params_str = ''
#         for key in params.keys():
#             params_str += 'key: {}, value: {}<br>'.format(key, params[key])
#         return {'code':0, 'message': 'SUCCESS'}, 200
#     @staticmethod
#     def get(id):
#         print(f'Actor {id} added ')
#         try:
#             actor = ActorDao.find_by_id(id)
#             if actor:
#                 return actor.json()
#         except Exception as e:
#             return {'message': 'Actor not found'}, 404

#     @staticmethod
#     def update():
#         args = parser.parse_args()
#         print(f'Actor {args["id"]} updated ')
#         return {'code':0, 'message': 'SUCCESS'}, 200

#     @staticmethod
#     def delete():
#         args = parser.parse_args()
#         print(f'Actor {args["id"]} deleted')
#         return {'code' : 0, 'message' : 'SUCCESS'}, 200    

# class Actors(Resource):
    
#     def post(self):
#         ud = ActorDao()
#         ud.insert_many('actors')

#     def get(self):
#         data = ActorDao.find_all()
#         return data, 200

# class Auth(Resource):

#     def post(self):
#         body = request.get_json()
#         actor = ActorDto(**body)
#         ActorDao.save(actor)
#         id = actor.actorid
        
#         return {'id': str(id)}, 200 


# class Access(Resource):
    
#     def post(self):
#         args = parser.parse_args()
#         actor = ActorVo()
#         actor.actorid = args.actorid
#         # actor.password = args.password
#         print(actor.actorid)
#         # print(actor.password)
#         data = ActorDao.login(actor)
#         return data[0], 200
#         # return actor.id
from flask_restful import Resource
from flask import Response, jsonify
from com_dayoung_api.ext.db import db

class ItemDto(db.Model):
    __tablename__='items'
    __table_args__={'mysql_collate':'utf8_general_ci'}

    item_id : int = db.Column(db.Integer, primary_key=True, index=True)
    name : str = db.Column(db.String(30))
    price : str = db.Column(db.String(30))

    # articles = db.relationship('ArticleDto', lazy='dynamic')

    def __init__(self, item_id, name, price):
        self.item_id = item_id
        self.name = name
        self.price = price


    def __repr__(self):
        return f'Item(item_id={self.item_id}, name={self.name}, price={self.price})'

    @property
    def json(self):
        return {'itemId': self.item_id, 'name': self.name, 'price': self.price}

    def save(self):
        db.session.add(self)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()
        
class ItemVo():
    ...

class ItemDao(ItemDto):
    
    @classmethod
    def find_all(cls):
        return cls.query.all()

    @classmethod
    def find_by_name(cls, name):
        return cls.query.filer_by(name == name).all()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id == id).first()

class ItemService(object):
    ...

class Item(Resource):
    ...

class Items(Resource):
    ... 


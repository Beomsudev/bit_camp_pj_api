from flask_restful import Resource, reqparse
from com_dayoung_api.ext.db import db, openSession
from com_dayoung_api.resources.user import UserDto
from com_dayoung_api.resources.movie import MovieDto

class ReviewDto(db.Model):
    __tablename__ = "reviews"
    __table_args__ = {'mysql_collate':'utf8_general_ci'}
    
    id: int = db.Column(db.Integer, primary_key=True, index=True)
    title: str = db.Column(db.String(100))
    content: str = db.Column(db.String(500))
    
    userid: str = db.Column(db.String(30))#, db.ForeignKey(UserDto.userid))
    movie_id: int = db.Column(db.Integer)#, db.ForeignKey(MovieDto.id))
    
    def __init__(self, title, content, userid, movie_id):
        self.title = title
        self.content = content
        self.userid = userid
        self.movie_id = movie_id
        
    def __repr__(self):
                return f'id={self.id}, user_id={self.userid}, movie_id={self.movie_id},\
            title={self.title}, content={self.content}'
            
    @property
    def json(self):
        return {
            'id': self.id,
            'userid' : self.userid,
            'movie_id' : self.movie_id,
            'title' : self.title,
            'content' : self.content
        }
        
    def save(self):
        db.session.add(self)
    
    def delete(self):
        db.session.delete(self)
        db.session.commit()

class ReviewDao():
    
    @classmethod
    def find_all(cls):
        return cls.query.all()

    @classmethod
    def find_by_name(cls, name):
        return cls.query.filer_by(name == name).all()

    @classmethod
    def find_by_id(cls, id):
        return cls.query.filter_by(id == id).first()
        
class Review(Resource):
    def __init__(self):
        parser = reqparse.RequestParser()
        parser.add_argument('id', type=int, required=False, help='This field cannot be left blank')
        parser.add_argument('user_id', type=int, required=False, help='This field cannot be left blank')
        parser.add_argument('movie_id', type=int, required=False, help='This field cannot be left blank')
        parser.add_argument('title', type=str, required=False, help='This field cannot be left blank')
        parser.add_argument('content', type=str, required=False, help='This field cannot be left blank')
        
    def post(self):
        data = self.parser.parse_args()
        review = ReviewDto(data['title'], data['content'], data['user_id'], data['movie_id'])
        try:
            review.save()
        except:
            return {'message': 'An error occurred inserting the review'}, 500
        return review.json(), 201
    
    def get(self, id):
        review = ReviewDao.find_by_id(id)
        if review:
            return review.json()
        return {'message' : 'Review not found'}, 404
    
    def put(self, id):
        data = Review.parser.parse_args()
        review = ReviewDao.find_by_id(id)
        
        review.title = data['title']
        review.content = data['content']
        review.save()
        return Review.json()
    
class Reviews(Resource):
    def get(self):
        return {'reviews' : list(map(lambda review: review.json(), ReviewDao.find_all()))}

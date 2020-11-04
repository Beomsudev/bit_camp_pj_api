from com_dayoung_api.ext.db import db

class ActorDto(db.Model):
    
    __tablename__ = 'actors'
    __table_args__= {'mysql_collate':'utf8_general_ci'}

    id : int = db.Column(db.Integer, primary_key = True, index = True)
    sample1 : str = db.Column(db.String(30))
    sample2 : str = db.Column(db.String(30))



    def __init__(self, id, sample1, genre):
        self.id = id
        self.sample1 = sample1
        self.sample2 = sample2



    def __repr__(self):
        return f'Movie(id=\'{self.id}\',\
            sample1=\'{self.sample1}\',\
            sample2=\'{self.sample2}\',)'

    @property
    def json(self):
        return {
            'id' : self.id,
            'sample1' : self.sample1,
            'sample2' : self.sample2
        }

    def save(self):
        db.session.add(self)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

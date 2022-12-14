from typing import Iterable

from sqlalchemy import (
    Column, ForeignKey, Integer, String, Date, func
)
from sqlalchemy.orm import declarative_base, relationship

DeclarativeBase = declarative_base()


class BaseTable(DeclarativeBase):
    __abstract__ = True

    def as_dict(self, include_relations: Iterable[relationship] = tuple()) -> dict:
        return {
            **{col.name: getattr(self, col.name) for col in self.__table__.columns},
            **{
                relation.key: tuple(entity.as_dict() for entity in getattr(self, relation.key))
                for relation in include_relations
            }
        }


class Question(BaseTable):
    __tablename__ = 'question'
    __mapper_args__ = {'eager_defaults': True}

    id = Column(Integer, primary_key=True)
    question_text = Column(String(200), nullable=False)
    pub_date = Column(Date, server_default=func.now(), nullable=False)

    choices = relationship('Choice', back_populates='question')


class Choice(BaseTable):
    __tablename__ = 'choice'
    __mapper_args__ = {'eager_defaults': True}

    id = Column(Integer, primary_key=True)
    choice_text = Column(String(200), nullable=False)
    votes = Column(Integer, server_default='0', nullable=False)
    question_id = Column(Integer, ForeignKey('question.id', ondelete='CASCADE'))

    question = relationship('Question', back_populates='choices')

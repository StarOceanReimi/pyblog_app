# -*- coding: utf-8 -*-

"""
  Database Model
  
  example code:
  
    class User(Model):
      id = IntField(11, True, True)
      name = StringField(32)
      remark = StringField(128)
      create_time = FloatField(32, default=time.time)
      __primarykey__ = 'id'
      __table__ = 't_user'
  
"""

from orm import *
import time


class User(Model):
  id = IntField(11, True, True)
  name = StringField(32)
  remark = StringField(128)
  create_time = FloatField(32, default=time.time, update=False)
  signiture = TextField()
  __primarykey__ = 'id'
  __table__ = 't_user'
  
class Topic(Model):
  id = IntField(11, True, True)
  title = StringField(255)
  content = LargeTextField()
  lastedit_time = FloatField(32, default=time.time)
  create_time = FloatField(32, default=time.time, update=False)
  click_times = IntField(9, default=0)
  post_by = IntField(11, True)
  visible = BitField(1, default=True)
  __primarykey__ = 'id'
  __table__ = 't_topic'

class Comment(Model):
  id = IntField(11, True, True)
  content = TextField()
  lastedit_time = FloatField(32, default=time.time)
  create_time = FloatField(32, default=time.time, update=False)
  post_by = IntField(11, True)
  topic = IntField(11, True)
  __primarykey__ = 'id'
  __table__ = 't_comment'
  
if '__main__' == __name__:
  mysqldb.create_db()
  User._build_table()
  Topic._build_table()
  Comment._build_table()
  
  user = User(name='Jack', remark='Haha, I am a gay!')
  user.insert()
  
  topic = Topic(title='whether is a gay good or not?!', content='Accually, I dont want talk about gay any more!', \
                post_by=user.id, visible=False)
  
  topic.insert()
  assert Topic.get(1).click_times == 0, 'click times should be automatically set 0'
  ct = topic.create_time
  t_content = topic.content
  topic.content = 'Oh, I am a homosexual guy! Haha!'
  topic.update()
  t = Topic.get(1)
  assert t.create_time == ct
  assert t.content == topic.content
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
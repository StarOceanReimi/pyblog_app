# -*- coding: utf-8 -*-

'''
 Simple Orm
 
  the implemention code is going to be like this:
   
   user = User.get(id)
   user.name = 'NewName'
   user.update()
   
   user = User(name="asdf", birth=time, ..)
   user.save()
   
'''

import re, db, libconfig, sys, collections, threading

mysqldb = db.Db.mysqldb(**libconfig.config['db'])

class Field(object):
  
  __pattern = re.compile(r"([a-zA-Z]+)(?:$|\((\d+)(?=\))\))")
  __t_locals = threading.local()
  
  @classmethod
  def __increate_order(cls):
    if 'counter' not in dir(Field.__t_locals):
      Field.__t_locals.counter = 0
    Field.__t_locals.counter += 1
    return Field.__t_locals.counter
    
  def __init__(self, dbtype, len=0, notnull=False, auto_increment=False):
    self.set_dbtype(dbtype)
    self.set_len(len)
    self._notnull = notnull
    self._autoincr = auto_increment
    self._order = self.__increate_order()
    
  def __repr__(self):
    return '<%s:[%s]>' % (self.__class__.__name__, self._dbtype)

  def set_type(self, type):
    dbtype = self._dbtype and self.__pattern.sub('%s(\\2)' % type, self._dbtype) or None
    self._dbtype = dbtype
    
  def set_len(self, len):
    dbtype = None
    if not len:
      dbtype = self._dbtype and self.__pattern.sub('\\1', self._dbtype) or None
    else:
      dbtype = self._dbtype and self.__pattern.sub('\\1(%s)' % len, self._dbtype) or None
    self._dbtype = dbtype
  
  def set_dbtype(self, dbtype):
    if self.__pattern.match(dbtype):
      self._dbtype = dbtype
    else:
      raise ValueError('"{}" not a valiad dbtype string'.format(dbtype))
  
  def db_def(self):
    DB_DEF = self._dbtype
    if self._notnull:
      DB_DEF += " not null"
    if self._autoincr:
      DB_DEF += " auto_increment"
    return DB_DEF
    
  __str__ = __repr__
  
class StringField(Field):
  def __init__(self, *args):
    super(StringField, self).__init__('varchar(50)', *args)
    
class IntField(Field):
  def __init__(self, *args):
    super(IntField, self).__init__('int(11)', *args)
    
class FloatField(Field):
  def __init__(self, *args):
    super(FloatField, self).__init__('float(32)', *args)
    
class _ModelMetaClass(type):
  def __new__(cls, name, base, attr):
    if name == None:
      return None
    if name == 'Model':
      return type.__new__(cls, name, base, attr)
    
    mappings = dict()
    for k,v in attr.iteritems():
      if isinstance(v, Field):
        mappings[k] = v
    
    for k in mappings:
      attr.pop(k)
    
    attr['__mappings__'] = collections.OrderedDict(sorted(mappings.items(), key=lambda t:t[1]._order))
    
    return super(_ModelMetaClass, cls).__new__(cls, name, base, attr)
    
  def __init__(cls, name, base, attr):
    return super(_ModelMetaClass, cls).__init__(name, base, attr)
      
class Model(dict):
  
  __primarykey__ = None
  __table__ = None
  
  def __init__(self, **kw):
    return super(Model, self).__init__(**kw)
    
  def __getattr__(self, key):
    try:
      return self[key]
    except:
      raise AttributeError('Attribute %s does not exists in %s' % (key, self.__class__.__name__))

  def __setattr__(self, key, value):
    self[key] = value
  
  def insert(self):
    with mysqldb.connect() as conn:
      try:
        conn.insert(self.__table__, **self)
        conn.commit()
        print "%s has inserted to databaes" % self
      except:
        conn.rollback()
        raise
  
  @classmethod
  def _build_table(cls):
    mps = cls.__mappings__
    table = cls.__table__
    pk = cls.__primarykey__
    fields = ','.join(["%s %s" % (k, v.db_def()) for k, v in mps.iteritems()])
    primarykey = "primary key (%s)" % pk
    create_table_script = "create table %s(%s, %s)" % (table, fields, primarykey)
    print create_table_script
    drop_if_exists = "drop table if exists %s" % table
    with mysqldb.connect() as conn:
      conn.update(drop_if_exists)
      conn.update(create_table_script)
      print "table(%s) has successfully created!" % table
        
  __metaclass__ = _ModelMetaClass

class User(Model):
  id = IntField(11, True, True)
  name = StringField(32)
  remark = StringField(128)
  create_time = FloatField(32)
  __primarykey__ = 'id'
  __table__ = 't_user'

if '__main__' == __name__:
  
  mysqldb.create_db()
  
  import time
  user = User(id=1, name="Jacky", remark="Jacky is a gay", create_time=time.time())
  print user
  user.id = 2
  user['remark']='Jack is a homosexual man'
  print user
  user2 = User()
  print user2

  print mysqldb
  User._build_table()
  user.insert()
  
  
  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

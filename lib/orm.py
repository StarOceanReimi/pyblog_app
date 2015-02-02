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

import re, db, libconfig, sys, collections, threading, time, logging

logging.basicConfig(**libconfig.config['log'])
logger = logging.getLogger(__name__)
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
    
  def __init__(self, dbtype, len=0, notnull=False, auto_increment=False, **kw):
    self.set_dbtype(dbtype)
    self.set_len(len)
    self._notnull = notnull
    self._autoincr = auto_increment
    self._order = self.__increate_order()
    default = None
    if kw.has_key('default'): 
      default = self._get_default(kw['default'])
    self._default = default  
    update = True
    if kw.has_key('update'): 
      update = kw['update']
    self._update = update
    
  def _get_default(self, obj):
    if callable(obj):
      return obj()
    return obj
    
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
  def __init__(self, *args, **kw):
    super(StringField, self).__init__('varchar(50)', *args, **kw)
    
class IntField(Field):
  def __init__(self, *args, **kw):
    super(IntField, self).__init__('int(11)', *args, **kw)

class BitField(Field):
  def __init__(self, *args, **kw):
    super(BitField, self).__init__('bit(1)', *args, **kw)
    
class FloatField(Field):
  def __init__(self, *args, **kw):
    super(FloatField, self).__init__('float(32)', *args, **kw)
    
class TextField(Field):
  def __init__(self, *args, **kw):
    super(TextField, self).__init__('text', *args, **kw)

class LargeTextField(Field):
  def __init__(self, *args, **kw):
    super(LargeTextField, self).__init__('longtext', *args, **kw)
    
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
    mp = self.__mappings__
    for k,v in mp.iteritems():
      if v._default is not None:
        self[k] = v._default
    super(Model, self).__init__(**kw)
    
  def __getattr__(self, key):
    try:
      return self[key]
    except:
      raise AttributeError('Attribute %s does not exists in %s' % (key, self.__class__.__name__))

  def __setattr__(self, key, value):
    self[key] = value
  
  @classmethod
  def get(cls, pk):
    with mysqldb.connect() as conn:
      sql = "select * from {0} where {1} = %s".format(cls.__table__, cls.__primarykey__)
      def handler(cur):
        columns = cur.column_names
        row = cur.fetchone()
        return row and zip(columns, row) or None
      ret = conn.select(sql, pk, handler=handler)
      return ret and cls(**dict(ret)) or None
      
  def insert(self):
    with mysqldb.connect() as conn:
      try:
        conn.insert(self.__table__, **self)
        if not self.has_key(self.__primarykey__):
          last_id = conn.select('select last_insert_id();')
          self[self.__primarykey__] = last_id[0][0]
        conn.commit()
      except:
        conn.rollback()
        raise
        
  def update(self):
    mps = self.__mappings__
    params = self.copy()
    for k,v in mps.iteritems():
      if not v._update:
        params.pop(k)
    with mysqldb.connect() as conn:
      try:
        conn.edit(self.__table__, self.__primarykey__, **params)
        conn.commit()
      except:
        conn.rollback()
        raise
        
  def delete(self):
    with mysqldb.connect() as conn:
      try:
        conn.remove(self.__table__, self.__primarykey__, **self)
        conn.commit()
      except:
        conn.rollback()
        raise
  
  @classmethod
  def listall(cls):
    sql = "select * from %s" % cls.__table__
    def handler(cur):
      cols = cur.column_names
      rows = cur.fetchall()
      return [cls(**dict(zip(cols, row))) for row in rows]
      
    with mysqldb.connect() as conn:
      try:
        return conn.select(sql, handler=handler)
        conn.commit()
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
    drop_if_exists = "drop table if exists %s" % table
    with mysqldb.connect() as conn:
      try:
        conn.update(drop_if_exists)
        conn.update(create_table_script)
        logger.info("table(%s) has successfully created!" % table)
      except:
        raise
        
  __metaclass__ = _ModelMetaClass
  
class View(object):
  '''
    example code:
      View.table('t_user').select() -> [{ ... }]
      View.table('t_user').orderby(order(field, asc/desc), [order...]).select() -> [{ ... }]
      View.table('t_user').limit(start, max).orderby(..).select() -> [{ ... }]
      View.table('t_user').filter(stmt(field, op, value)).select() -> [{ ... }]
      View.table(t1).group().filter().select() -> [ [{ ... }], [{ ... }] ]
      View.join_table((t1, alias), (t2, alias), join_type).select() -> [{ ... }]
      View.sql(sql, list/dict, **kw)
  '''
  pass

if '__main__' == __name__:
  from model import *
  mysqldb.create_db()
  User._build_table()
  print User.listall()
  user = User(id=1, name="Jacky", remark="Jacky is a gay")
  user.insert()
  user2 = User(name='Lily', remark="Nice Girl!")
  user2.insert()
  print User.listall()
  if False:
    user = User(id=1, name="Jacky", remark="Jacky is a gay")
    user.id = 2
    user['remark']='Jack is a homosexual man'
    user2 = User(name='Lily', remark="Nice Girl!")
    user.insert()
    user2.insert()
    
    user2.name = 'Lucy'
    user2.update()
    user2.delete()
    
  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

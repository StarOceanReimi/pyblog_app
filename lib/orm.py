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

  def __getattr__(cls, key):
    return FilterClass(cls, key)

class FilterClass(object):
  
  __name = None
  __class = None
  __value = None
  __op = None
  __alias = None
  __valuename = None
  __other_filters = []
  __concatsign = ''

  def __init__(self, cls, name):
    self.__name = name
    self.__class = cls
    self.__alias = cls.__name__
    self.__valuename = "%s_0" % name
    
  def __eq__(self, value):
    self.__value = value
    self.__op = '='
    return self
    
  def __gt__(self, value):
    self.__value = value
    self.__op = '>'
    return self

  def __lt__(self, value):
    self.__value = value
    self.__op = '<'
    return self
    
  def __ne__(self, value):
    self.__value = value
    self.__op = '!='
    return self
    
  def __ge__(self, value):
    self.__value = value
    self.__op = '>='
    return self
    
  def __le__(self, value):
    self.__value = value
    self.__op = '<='
    return self
  
  def like(self, value):
    self.__value = value
    self.__op = 'like'
    return self
  
  def in_(self, value):
    value = isinstance(value, list) and value or list(value)
    self.__value = value
    self.__op = 'in'
    return self
    
  def get_concatsign(self):
    return self.__concatsign
  
  def set_concatsign(self, concatsign):
    self.__concatsign = concatsign
  
  def __and__(self, value):
    return self.__concate__(value, 'and')
    
  def __or__(self, value):
    return self.__concate__(value, 'or')

  def __concate__(self, value, concatesign):
    if not isinstance(value, FilterClass):
      raise TypeError('type must be a FilterClass')
    for p in self:
      if p.get_valuename() == value.get_valuename():
        value.set_valuename(self.__new_valuename())
        break

    value.set_concatsign(concatesign)
    self.__other_filters.append(value)
    return self

  def __new_valuename(self):
    vn = self.__valuename
    return vn[0:-1] + str(len(self.__other_filters)+1)
    
  def __iter__(self):
    return self.__generator__()

  def __generator__(self):
    yield self
    for other in self.__other_filters:
      yield other
    raise StopIteration()

  def get_op(self):
    return self.__op;

  def set_alias(self, alias):
    self.__alias = alias
    
  def get_name(self):
    return self.__name
    
  def get_valuename(self):
    return self.__valuename
    
  def set_valuename(self, name):
    self.__valuename = name
  
  def get_aliasedname(self):
    return "%s.%s" % (self.__alias, self.__name)

  def tofilter(self, tablealias=False):
    filter = None
    property = None
    if tablealias:
      property = self.get_aliasedname()
    else:
      property = self.__name
    
    if self.__op == 'in':
      filterstr = "{0} {1} ({2})".format(property, self.__op, ','.join(['%s' for x in self.__value]))
    elif isinstance(self.__value, FilterClass):
      filterstr = "{0} {1} {2}".format(self.get_aliasedname(), self.__op, self.__value.get_aliasedname())
    else:
      filterstr = "{0} {1} %({2})s".format(property, self.__op, self.get_valuename())
    return (filterstr, self.__value)
                             
    
  def __str__(self):
    return "FilterClass for (%s.%s.%s)" % (self.__class.__module__, self.__class.__name__, self.__name)
    
  __repr__ = __str__
  
      
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
    View is a wrapper class that help check data in 
    database more conveniently. The sample code is list
    below:
    
      Initiate instance:
        view = View(Class*) -> Class should have the attribute __table__
                               or inherit from orm.Model 
        view = View('tablename'*) 
        view = View.sql(sql_statement, *args, **kw)
        view = View.join(['tablename', 'join_key']+) 
      
      Useful methods:
        filter(Class.property=certainvalue & Class.property=something | Class.property=sss) -> conditions and
        
        orderby((Class_property=[asc or desc] ommit means asc)*)
        limit(start, max)
        
        select() -> return a dict
        count() -> return how many rows does the table have
        
  '''
  __tablenames__ = []
  __tablealias__ = []
  __sqlstmt = None
  __sqlargs = []
  __sqlnamedargs = {}
  __ret_cols = None
  __where = None
  
  def __init__(self, *args):
    assert args is not None and len(args) > 0, 'Only can create view by objects and tablenames'
    for x in args:
      if isinstance(x, str):
        self.__tablenames__.append(x)
      else:
        self.__tablenames__.append(x.__table__)
        self.__tablealias__.append(x.__name__)
        
  @classmethod
  def sql(cls, stmt, *args, **kw):
    self.__sqlstmt = stmt
    self.__sqlargs = args
    
  def columns(self, *cols, **kw):
    _cols = None
    if kw.has_key('mapfunc'):
      _cols = map(kw['mapfunc'], cols)
    else:
      _cols = map(lambda c: isinstance(c, FilterClass) and c.get_aliasedname() or c, cols)
      
    __ret_cols = _cols
    return self
  
  def select(self):
    sql = "select {} from {} {}"
    cols = ','.join(self.__ret_cols) if self.__ret_cols is not None else '*'
    tables = None
    if len(self.__tablenames__) > 1:
      tables = ','.join(['%s as %s' % (x, y) for x, y in zip(self.__tablenames__, self.__tablealias__)])
    else:  
      tables = ','.join(self.__tablenames__)
    where = self.__where if self.__where is not None else ''
    sql = sql.format(cols, tables, where)

    def handler(cur):
      cols = cur.column_names
      rows = cur.fetchall()
      return [dict(zip(cols, row)) for row in rows]
      
    ret = []
    with mysqldb.connect() as conn:
      try:
        if len(self.__sqlargs) > 0:
          ret = conn.select(sql, *self.__sqlargs, handler=handler)
        elif len(self.__sqlnamedargs) > 0:
          ret = conn.select(sql, self.__sqlnamedargs, handler=handler)
        else:
          ret = conn.select(sql, handler=handler)
        return ret
      except:
        raise
  
  def filter(self, filters, **kw):
    
    filter_sql = self.__where is None and "where%s" or "and%s"
    
    conditions = []
    tn = len(self.__tablenames__)
    
    for f in filters:
      
      fstr, value = f.tofilter(tn>1)
      conditions.append("%s %s" % (f.get_concatsign(), fstr))

      if f.get_op() == 'in':
        self.__sqlargs += value
      elif value is not None and not isinstance(value, FilterClass):
        self.__sqlnamedargs[f.get_valuename()] = value
        
    self.__where = filter_sql % (' '.join(conditions))
    
    return self
    
  def __str__(self):
    return 'View %s' % self.__tablenames__
    
  __repr__ = __str__
    
if '__main__' == __name__:
  from model import *
  #filters = (User.id >= 1) & (User.id <= 10) & (Topic.id != 50) | (User.name == 'User')

  print View(User).filter(User.id == 1).select()
  #print v._View__sqlnamedargs


  if False:
    
    mysqldb.create_db()
    User._build_table()

    user = User(id=1, name="Jacky", remark="Jacky is a gay")
    user.id = 2
    user['remark']='Jack is a homosexual man'
    user2 = User(name='Lily', remark="Nice Girl!")
    user.insert()
    user2.insert()
    
    user2.name = 'Lucy'
    user2.update()
    user2.delete()
    
  
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

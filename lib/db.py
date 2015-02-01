# -*- coding: utf-8 -*-

'''
Python Simple Db utilities

Codes are going to be like this:
  mysqldb = Db.mysql_db(username, pass, db, **kw)
  with mysqldb.connect() as conn:
    try:
      #update data like insert update delete cmd
      conn.update(sql, *params, **kw)
      #select data
      #returns => [(),(),()]
      conn.select(sql, *params, **kw)
      conn.commit()
    except:
      conn.rollback()
  
'''
import mysql.connector
import libconfig
import logging

logging.basicConfig(**libconfig.config['log'])
logger = logging.getLogger(__name__)
class Db(object):
  @staticmethod
  def mysqldb(**kw):
    return MysqlDb(**kw)
    
class MysqlDb(Db):

  _db_info = None
  
  def __init__(self, **kw):
    _dict = dict(**kw)
    self._db_info = _dict
    
  def connect(self):
    conn = mysql.connector.connect(**self._db_info)
    return MysqlConn(conn)
  
  def create_db(self, database=None, encoding='utf8'):
    _db_name = database;
    if not _db_name:
      if self._db_info.has_key('database'):
        _db_name = self._db_info.pop('database')
      else:
        raise AttributeError('can find database name in dict! you must specifically input a database name to create database')
    conn = mysql.connector.connect(**self._db_info)
    cur = conn.cursor();
    drop_if_exists = "drop database if exists %s;" % _db_name
    cur.execute(drop_if_exists)
    create_script = "create database %s charset = %s" % (_db_name, encoding)
    cur.execute(create_script)
    logger.info('database(%s) has successfully created!', _db_name)
    self._db_info['database'] = _db_name
    
class MysqlConn(object):
  
  _conn = None
  
  def __init__(self, conn):
    self._conn = conn
    self._conn.autocommit = False
    
  def set_autocommit(self, auto):
    self._conn.autocommit = auto
    
  def __enter__(self):
    return self
    
  def __exit__(self, type, value, trackback):
    self._conn.close()

  @staticmethod
  def _buildInsertCmd(tname, **kw):
    sql = "insert into {0}({1}) values({2})"
    key = kw.keys()
    return sql.format(tname, ','.join(key), ','.join(map(lambda x: "%({})s".format(x), key)))
    
  @staticmethod
  def _buildEditCmd(tname, condition=None, **kw):
    sql = "update {0} set {1} {2}"
    key = ['='.join([k, "%({})s".format(k)]) for k in kw.keys()]
    where = ''
    if isinstance(condition, Condition):
      where = "where " + condition.tostr()
    return sql.format(tname, ','.join(key), where)
    
  def insert(self, tname, **kw):
    sql = MysqlConn._buildInsertCmd(tname, **kw)
    with self._cursor() as cur:
      cur.execute(sql, kw)
      logger.debug('sql: %s , args=%s', sql, str(kw))
      
  def edit(self, tname, pk, **kw):
    sql = MysqlConn._buildEditCmd(tname, c(pk), **kw)
    with self._cursor() as cur:
      cur.execute(sql, kw)
      logger.debug('sql: %s , args=%s', sql, str(kw))
      
  def remove(self, tname, pk, **kw):
    sql = "delete from {0} where {1} = %({1})s".format(tname, pk)
    with self._cursor() as cur:
      cur.execute(sql, kw)
      logger.debug('sql: %s , args=%s', sql, str(kw))
      
  def update(self, stmt, *args):
    with self._cursor() as cur:
      cur.execute(stmt, args)
      logger.debug('sql: %s , args=%s', stmt, str(args))
    
  def select(self, stmt, *args, **kw):
    with self._cursor() as cur:
      cur.execute(stmt, args)
      result_handler = kw.has_key('handler') and kw['handler'] or None
      if result_handler and callable(result_handler):
        ret = result_handler(cur)
        return ret
      else:
        ret = cur.fetchall()
        return ret
  
  def commit(self):
    self._conn.commit()
    
  def rollback(self):
    self._conn.rollback()
  
  def cursor(self, **kw):
    return self._conn.cursor(**kw)
  
  def _cursor(self, **kw):
    return self.__cursor(self._conn.cursor(**kw))
    
  class __cursor(object):
    def __init__(self, cur):
      self.cursor = cur
    def __enter__(self):
      return self.cursor
    def __exit__(self, type, value, tback):
      self.cursor.close()

def c(k, va=None, op='='):
  return Condition(k, va, op)

class Condition(object):
  def __init__(self, key, va=None, operator='='):
    self._str = "%s %s %s" % (key, operator, "%({})s".format(va if va else key))
  
  def _and(self, condition):
    self._str += ' and ' + condition._str
    return self
    
  def _or(self, condition):
    self._str += ' or ' + condition._str
    return self
    
  def tostr(self):
    return self._str;


if __name__ == '__main__':
    
  print MysqlConn._buildEditCmd('t_user', c('id'), user='Ashely', id=1)
  
  if False:
    mysqldb = Db.mysqldb(**libconfig.config['db'])
    try:
      conn = mysqldb.connect()
    except mysql.connector.errors.ProgrammingError, pe:
      error_class = pe.__class__
      print '%s.%s: %s' % (error_class.__module__, error_class.__name__, pe.__str__())
      mysqldb.create_db()
  





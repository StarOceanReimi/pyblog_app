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

class Db(object):
  @staticmethod
  def mysqldb(un, pa, db, **kw):
    return MysqlDb(un, pa, db, **kw)
    
  
class MysqlDb(Db):

  _db_info = None
  
  def __init__(self, un, pa, db, **kw):
    _dict = dict(user=un, password=pa, database=db)
    _dict.update(kw)
    self._db_info = _dict
    
  def connect(self):
    conn = mysql.connector.connect(**self._db_info)
    return MysqlConn(conn)
 
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
  
  def insert(self, tname, **kw):
    sql = MysqlConn._buildInsertCmd(tname, **kw)
    with self._cursor() as cur:
      cur.execute(sql, kw)
  
  def update(self, stmt, *args):
    with self._cursor() as cur:
      cur.execute(stmt, args)
    
  def select(self, stmt, *args, **kw):
    with self._cursor() as cur:
      cur.execute(stmt, args)
      result_handler = None
      for x in kw.values():
        if callable(x):
          result_handler = x
          break
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
    

if __name__ == '__main__':
  
  mysqldb = Db.mysqldb('root', 'root', 'test', use_unicode=True)
  conn = mysqldb.connect()
  





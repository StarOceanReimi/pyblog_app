import os
cwd = os.getcwd()
lib = os.path.join(cwd, 'lib')
import sys
sys.path.append(lib)

import db

#create a table
def test1():
  mysqldb = db.Db.mysqldb('root', 'root', 'test')
  with mysqldb.connect() as conn:
    try:
      cur = conn.cursor()
      iter = cur.execute('''
        drop table if exists t_product;
        create table t_product (
          id int auto_increment primary key, 
          name varchar(50) not null,
          remark varchar(50)
          );''', multi=True)
      for ret in iter:
        print 'Running... ' + ret.statement
      
      print 'table was created successfully.'
    except:
      raise

from wsgiref.simple_server import make_server
#http test
def app(enviorn, start_response):

  response = '\n'.join(['%s=%s' % (k, v) for k,v in enviorn.iteritems()])

  status = '200 ok'

  headers = [('content-type', 'text/plain'), ('content-length', str(len(response)))]

  start_response(status, headers)

  return response;

port = 8051  
httpd = make_server('localhost', port, app)
print 'Start Http on port %s' % port
httpd.serve_forever()
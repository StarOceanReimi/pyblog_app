import os
cwd = os.getcwd()
lib = os.path.join(cwd, 'lib')
import sys
sys.path.append(lib)

import db

#create a table
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

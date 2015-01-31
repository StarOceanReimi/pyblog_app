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

import re

class Field(object):
  
  _name = None
  _dbtype = None
  
  __pattern = re.compile(r"([a-zA-Z]+)(?:$|\((\d+)(?=\))\))")
  
  def __init__(self, name, dbtype):
    self._name = name
    self._dbtype = dbtype
    
  def __repr__(self):
    return '<%s:[%s, %s]>' % (self.__class__.__name__, self._name, self._dbtype)
    
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
    
    
  __str__ = __repr__
  
class StringField(Field):
  def __init__(self, name):
    super(StringField, self).__init__(name, 'varchar(20)')

class IntField(Field):
  def __init__(self, name):
    super(IntField, self).__init__(name, 'int(11)')
    
class FloatField(Field):
  def __init__(self, name):
    super(IntField, self).__init__(name, 'float(32)')
    
if '__main__' == __name__:
  fs = StringField('name')
  print fs
  fs.set_len(50)
  print fs
  fs.set_type('char')
  print fs
  fs.set_dbtype('char(200)')
  print fs
    

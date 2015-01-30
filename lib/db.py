# -*- coding: utf-8 -*-

'''
Python Simple Db utilities
'''

def test(who=None):
  print 'Hello', who and who or "World!"
  
if "__main__" == __name__:
  import sys
  args = sys.argv
  who = len(args) > 1 and args[1] or None
  test(who)
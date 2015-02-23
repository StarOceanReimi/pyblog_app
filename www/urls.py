# -*- coding: utf-8 -*-
from web import *


@view('index.html')
@get('/')
def index():
  return dict(name='Q')
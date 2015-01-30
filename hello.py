import os
cwd = os.getcwd()
lib = os.path.join(cwd, 'lib')
import sys
sys.path.append(lib)

import db
db.test("Q")


import sys
import os

basename = os.path.basename(os.path.abspath('.'))
if basename == 'py':
	libPath = os.path.abspath('../lib/')
	utilPath = os.path.abspath('../utils/')
	quantificationPath = os.path.abspath('../quantification/')
else:
	libPath = os.path.abspath('./lib/')
	utilPath = os.path.abspath('./utils/')
	quantificationPath = os.path.abspath('./quantification/')

if not libPath in sys.path:
	sys.path.append(libPath)
	
if not utilPath in sys.path:
	sys.path.append(utilPath)

if not quantificationPath in sys.path:
	sys.path.append(quantificationPath)
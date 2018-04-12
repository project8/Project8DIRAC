#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-externals-versions
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "bfe41d7 (2014-06-16 09:58:26 +0200) FedericoStagni <fstagni@cern.ch>"

from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC import exit as DIRACExit
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()
diracAdmin.getExternalPackageVersions()
DIRACExit( 0 )


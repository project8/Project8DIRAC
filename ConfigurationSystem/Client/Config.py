# $HeadURL$
"""
  Instantiate the global Configuration Object
  gConfig is used everywhere within DIRAC to access Configuration data
"""
__RCSID__ = "eb3c0e6 (2010-11-29 08:23:34 +0000) Ricardo Graciani <graciani@ecm.ub.es>"
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

gConfig = ConfigurationClient()
def getConfig():
  return gConfig

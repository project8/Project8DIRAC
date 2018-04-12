__RCSID__ = "8a308fc (2016-10-28 13:16:00 +0200) Federico Stagni <federico.stagni@cern.ch>"

from DIRAC                                              import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath

gBaseLocalSiteSection = "/LocalSite"

def gridEnv():
  """
    Return location of gridenv file to get a UI environment
  """
  return gConfig.getValue( cfgPath( gBaseLocalSiteSection, 'GridEnv' ), '' )

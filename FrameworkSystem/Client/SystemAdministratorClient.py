""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service. It has also methods to update the Configuration
    Service with the DIRAC components options
"""

__RCSID__ = "146b4ee (2016-06-22 15:18:34 +0200) Federico Stagni <federico.stagni@cern.ch>"

from DIRAC.Core.Base.Client import Client

SYSADMIN_PORT = 9162

class SystemAdministratorClient( Client ):

  def __init__( self, host, port = None, **kwargs ):
    """ Constructor function. Takes a mandatory host parameter
    """
    Client.__init__( self, **kwargs )
    if not port:
      port = SYSADMIN_PORT
    self.setServer( 'dips://%s:%s/Framework/SystemAdministrator' % ( host, port ) )

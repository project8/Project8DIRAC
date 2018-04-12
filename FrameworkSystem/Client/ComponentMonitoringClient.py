"""
Class for making requests to a ComponentMonitoring Service
"""

__RCSID__ = "df71646 (2015-03-04 11:11:45 +0100) Sbalbp <sbalbp@gmail.com>"

from DIRAC.Core.Base.Client import Client

class ComponentMonitoringClient( Client ):

  def __init__( self, **kwargs ):
    """
    Constructor function
    """

    Client.__init__( self, **kwargs )
    self.setServer( 'Framework/ComponentMonitoring' )

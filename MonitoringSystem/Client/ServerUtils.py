"""
This module is used to create an appropriate object which can be used to insert records to the Monitoring system.
It always try to insert the records directly. In case of failure the monitoring client is used...
"""

__RCSID__ = "5c62788 (2016-10-24 10:26:34 +0200) Zoltan Mathe <zoltan.mathe@cern.ch>"

def getDBOrClient( DB, serverName ):
  """ Tries to instantiate the DB object and returns it if we manage to connect to the DB, otherwise returns a Client of the server
  """
  from DIRAC import gLogger
  from DIRAC.Core.DISET.RPCClient                            import RPCClient
  try:
    database = DB()
    if database._connected:
      return database
  except:
    pass

  gLogger.info( 'Can not connect to DB will use %s' % serverName )
  return RPCClient( serverName )

def getMonitoringDB():
  serverName = 'Monitoring/Monitoring'
  MonitoringDB = None
  try:
    from DIRAC.MonitoringSystem.DB.MonitoringDB import MonitoringDB
  except:
    pass
  return getDBOrClient( MonitoringDB, serverName )

monitoringDB = getMonitoringDB()

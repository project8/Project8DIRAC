"""
Do the initial configuration of a DIRAC component
"""
#
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions
from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
#
from DIRAC import gConfig
from DIRAC import exit as DIRACexit

__RCSID__ = "fdb9598 (2016-04-22 12:10:28 +0200) Federico Stagni <federico.stagni@cern.ch>"

gComponentInstaller.exitOnError = True
#
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ComponentType System Component|System/Component' % Script.scriptName,
                                     'Arguments:',
                                     '  ComponentType:  Name of the ComponentType (ie: agent)',
                                     '  System:  Name of the DIRAC system (ie: WorkloadManagement)',
                                     '  component:   Name of the DIRAC component (ie: JobCleaningAgent)'] ) )
Script.parseCommandLine()
args = Script.getPositionalArgs()

componentType = args[0]

if len( args ) == 2:
  system, component = args[1].split( '/' )
else:
  system = args[1]
  component = args[2]

result = gComponentInstaller.addDefaultOptionsToCS( gConfig, componentType, system, component,
                                                    getCSExtensions(),
                                                    specialOptions = {},
                                                    overwrite = False )
if not result['OK']:
  print "ERROR:", result['Message']
else:
  DIRACexit()

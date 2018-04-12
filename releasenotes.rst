
================
Version v6r17p11
================

Core
====

Bugfix
:::::::::::

 - ElasticSearchDB - set a very high number (10K) for the size of the ElasticSearch result

Monitoring
==========

Bugfix
:::::::::::

 - MonitoringDB - et a very high number (10K) for the size of the ElasticSearch result

WMS
===

Bugfix
:::::::::::

 - pilotCommands - get the pilot environment from the contents of the bashrc script

DMS
===

Bugfix
:::::::::::

 - RemoveReplica - fix for the problem that if an error was set it was never reset
 - SE metadata usage in several components: ConsistencyInspector, DataIntwgrityClient, FTSRequest, dirac-dms-replica-metadata, StageMonitorAgent, StageRequestAgent, StorageManagerClient, DownloadInputData, InputDataByProtocol

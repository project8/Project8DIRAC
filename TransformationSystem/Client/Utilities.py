#!/bin/env python

__RCSID__ = "$Id:$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities as \
        DIRACPluginUtilities
from DIRAC.TransformationSystem.Client.Utilities import getFileGroups
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from collections import Counter
import os

class PluginUtilities(DIRACPluginUtilities):

    def __init__(
            self,
            plugin='Standard',
            transClient=None,
            dataManager=None,
            fc=None,
            debug=False,
            transInThread=None,
            transID=None):
        super(PluginUtilities, self).__init__(
                plugin,
                transClient=transClient,
                dataManager=dataManager,
                fc=fc,
                debug=debug,
                transInThread=transInThread,
                transID=transID)
        self.opsHelper = Operations()

    def getRunID(self, lfn):
        res = fc.getFileUserMetadata(lfn)
        if not res['OK']:
            return res
        return res['Value']['run_id']

#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os, sys

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Interfaces.API.Job import Job

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient \
        import TransformationClient

import subprocess
from datetime import datetime

PATH_TO_SANDBOX = ('/project8/dirac/ts_prod/katydid_%s/termite_%s/scripts')

class P8Transformation(Transformation):
    def __init__(self, transID=0, transClient=None,
            software_tag=None, config_tag=None):
        super(P8Transformation, self).__init__(transID, transClient)

        if software_tag == None or config_tag == None:
            raise Exception('Must provide software and config tags')
        self.software_tag = software_tag
        self.config_tag = config_tag
        self.path_to_sandbox = (PATH_TO_SANDBOX
                % (self.software_tag, self.config_tag))
        self.dirac = Dirac()
        self.fc = FileCatalogClient()
        ops_dict = Operations().getOptionsDict('Transformations/')
        if not ops_dict['OK']:
            raise Exception(ops_dict['Message'])
        ops_dict = ops_dict['Value']
        self.PROD_DEST_DATA_SE = ops_dict.get(
                'ProdDestDataSE', 'PNNL-PIC-SRM-SE')
        self.PROD_DEST_MONITORING_SE = ops_dict.get(
                'ProdDestMonitoringSE', '')

    def _uploadToolsFile(self):
        # Get the tools file from the repository
        tmp_dir = os.path.join(
                os.getcwd(),
                'p8_%s' % datetime.now().strftime('%Y-%m-%d_%H%M%S'))
        print('repo_dir: %s\n' % tmp_dir)
        cmd = 'git clone https://github.com/project8/Project8DIRAC.git %s'
        subprocess.check_call(cmd % tmp_dir, shell=True)
        os.chdir(tmp_dir) # temporary due to branch change
        cmd = 'git checkout origin/transformation-work -b transformation-work'
        subprocess.check_call(cmd, shell=True)
        # TODO: git checkout tags/<tag> to get specific tools tag?

        # Upload the tools file
        tools_file = os.path.join(self.path_to_sandbox, 'p8dirac_wms_tools.py')
        print('tools_file: %s\n' % tools_file)
        res = self.dirac.removeFile(tools_file)
        if not res['OK']:
            return res
        res = self.dirac.addFile(
                tools_file,
                os.path.join(
                    tmp_dir,
                    'TransformationSystem/Utilities/p8dirac_wms_tools.py'),
                self.PROD_DEST_DATA_SE)

        # Remove the local tools file
        os.chdir(os.path.dirname(tmp_dir))
        subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        if not res['OK']:
            return res
        return S_OK(tools_file)

    def _isUploaded(self, lfn):
        # Verify that a file is in the file catalog
        res = self.fc.getFileSize(lfn) # better way to check?
        if not res['OK']:
            return res
        if not res['Value']['Successful'].get(lfn):
            return S_ERROR('%s is not uploaded' % lfn)
        return S_OK()


    def buildProcessTransformation(self):
        cwd = os.getcwd()

        # Try to get the configuration file and if that doesn't work, then
        # quit out.
        cfg_file = os.path.join(
                self.path_to_sandbox, 'Katydid_ROACH_Config.yaml')
        print('cfg_file: %s\n' % cfg_file)
        res = self._isUploaded(cfg_file)
        if not res['OK']:
            return res

        # Create Katydid script
        script = (
                '#!/bin/bash\n'
                'CFG_FILE=$1\n'
                'unset DIRAC\n'
                'P8_INPUT_FILE=`python -c \'import p8dirac_wms_tools as tools; '
                'print(tools.getJobFileName())\'`\n'
                'source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh\n'
                'Katydid -c ${CFGFILE} -e ${P8_INPUT_FILE}\n'
                'ls -l\n'
                'source /cvmfs/hep.pnnl.gov/project8/dirac_client_current/bashrc\n'
                'python -c "import p8dirac_wms_tools as tools; '
                'print(tools.uploadJobOutputROOT(\'%s\', \'%s\'))"'
                % (self.software_tag, self.software_tag, self.config_tag))
        script_name = os.path.join(cwd, 'katydid_%s.sh' % self.software_tag)
        f = open(script_name, 'w+')
        f.write(script)
        f.close()

        # Upload Katydid script
        katydid_file = os.path.join(
                self.path_to_sandbox,'katydid_%s.sh' % self.software_tag)
        print('katydid_file: %s\n' % katydid_file)
        print('katydid contents:\n%s\n' % script)
        res = self.dirac.removeFile(katydid_file) # First remove it
        if not res['OK']:
            return res
        res = self.dirac.addFile(
                katydid_file, script_name, self.PROD_DEST_DATA_SE)
        # Regardless of if the file was uploaded successfully, remove the temp file
        subprocess.check_call('rm %s' % script_name, shell=True)
        # Now check if the file was uploaded successfully
        if not res['OK']:
          return res
       
        # Upload the tools file
        res = self._uploadToolsFile()
        if not res['OK']:
            return res
        tools_file = res['Value']

        # Create a job and use the files we just uploaded as the input sandbox

        self.j = Job()
        self.j.setInputSandbox(
                ['LFN:%s' % cfg_file,
                 'LFN:%s' % katydid_file,
                 'LFN:%s' % tools_file])
        self.j.setExecutable(
                './' + os.path.basename(katydid_file),
                arguments=os.path.basename(cfg_file))
        self.j.setCPUTime(1000)
        self.j.setOutputSandbox(['env.txt', '*log', '*info*', '*xml*', '*json'])
        self.j.setName(
                'katydid_%s-termite_%s' % (self.software_tag, self.config_tag))

        # Set other parameters of this transformation
        self.setTransformationName(
                'test-katydid_%s-termite_%s'
                % (self.software_tag, self.config_tag))
        self.setTransformationGroup('KatydidMetadataProcess')
        self.setType('DataReprocessing')
        self.setDescription(
                'Transformation used to automatically process RAW data')
        self.setLongDescription(
                'Transformation use to autmatically process RAW data. '
                'Triggered using the metadata fields.')
        self.setMaxNumberOfTasks(0)
        self.setBody(self.j.workflow.toXML())
        self.setPlugin('P8FilterForRAWData')
        self.setGroupSize(1)
        self.addTransformation()
        self.setStatus('Active')
        self.setAgentType('Automatic')
        tid = self.getTransformationID()
        if not tid['OK']:
          return tid
        self.transClient.createTransformationInputDataQuery(
                tid['Value'],
                {'DataLevel': 'RAW', 'DataType': 'Data', 'run_id': '6935'}) # run_id is temporary for testing
        return S_OK(tid['Value'])

    def buildMergeTransformation(self):
        cwd = os.getcwd()

        # Create p8dirac_postprocessing.sh script
        script = (
                '#!/bin/bash\n'
                'echo "hostname:" `/bin/hostname`\n'
                'echo " "; echo " "\n'
                'unset DIRAC\n'
                'echo "Sourcing Dirac"\n'
                'source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh\n'
                'echo " "; echo " "\n'
                'echo "Creating json details file"\n'
                'JSON_DUMP=`python -c \'import p8dirac_wms_tools as tools; '
                'print(tools.createDetails())\'`\n'
                'echo "Executing python p8dirac_postprocessing.py"\n'
                'python p8dirac_postprocessing.py $JSON_DUMP\n'
                'echo "Notifying slack of completed analysis"\n'
                'python slack_post.py $JSON_DUMP\n'
                'rm $JSON_DUMP\n' # remove the temp file
                % self.software_tag)
        script_name = os.path.join(cwd, 'p8dirac_postprocessing.sh')
        f = open(script_name, 'w+')
        f.write(script)
        f.close()

        # Upload p8dirac_postprocessing.sh script
        postproc_file = os.path.join(
                self.path_to_sandbox, 'p8dirac_postprocessing.sh')
        print('post_processing file: %s\n' % postproc_file)
        print('post_processing contents:\n%s\n' % script)
        res = self.dirac.removeFile(postproc_file) # First remove it
        if not res['OK']:
            return res
        res = self.dirac.addFile(
                postproc_file, script_name, self.PROD_DEST_DATA_SE)
        # Regardless of if the file was uploaded successfully, remove the temp
        subprocess.check_call('rm %s' % script_name, shell=True)
        # Now check if file was uploaded successfully
        if not res['OK']:
           return res
        
        # Upload the tools file
        res = self._uploadToolsFile()
        if not res['OK']:
            return res
        tools_file = res['Value']

        # Make sure the other ladybug files are uploaded
        post_procpy = os.path.join(self.path_to_sandbox, 'postprocessing.py')
        slack_postpy = os.path.join(self.path_to_sandbox, 'slack_post.py')
        safe_accesspy = os.path.join(
                self.path_to_sandbox, 'p8dirac_safe_access.py')
        p8dirac_postprocpy = os.path.join(
                self.path_to_sandbox, 'p8dirac_postprocessing.py')

        res = self._isUploaded(post_procpy)
        if not res['OK']:
            return res
        res = self._isUploaded(slack_postpy)
        if not res['OK']:
            return res
        res = self._isUploaded(safe_accesspy)
        if not res['OK']:
            return res
        res = self._isUploaded(p8dirac_postprocpy)
        if not res['OK']:
            return res

        # NOTE: Everything below is untested.
        # Create the job (adapted from p8dirac_jobsubmitter.py)
        self.j = Job()
        self.j.setCPUTime(3000)
        # no arguments now
        self.j.setExecutable('./%s' % os.path.basename(postproc_file))
        self.j.setName('postprocessing-katydid_%s-termite_%s'
                % (self.software_tag, self.config_tag))
        self.j.setDestination('DIRAC.PNNL.us') # FIXME!!!
        self.j.setLogLevel('debug')
        self.j.setInputSandbox(
                ['LFN:%s' % postproc_file,
                 'LFN:%s' % tools_file,
                 'LFN:%s' % post_procpy,
                 'LFN:%s' % slack_postpy,
                 'LFN:%s' % safe_accesspy,
                 'LFN:%s' % p8dirac_postprocpy])
        self.j.setOutputSandbox(['std.err', 'std.out'])

        # Set other parameters of this transformation
        # TODO: VERIFY THESE ARE CORRECT
        self.setTransformationName(
                'test-katydid_%s-termite_%s-merge'
                % (self.software_tag, self.config_tag))
        self.setTransformationGroup('KatydidMetadataProcess') # is this right?
        self.setType('DataReprocessing') # is this right?
        self.setDescription(
                'Transformation used to automatically merge processed data')
        self.setLongDescription(
                'Transformation used to automatically merge processed data. '
                'Triggered using metadata fields.')
        self.setMaxNumberOfTasks(0)
        self.setBody(self.j.workflow.toXML())
        self.setPlugin('P8Merge') # TODO
        self.setGroupSize(1) # is this right?
        self.addTransformation()
        self.setStatus('Active')
        self.setAgentType('Automatic')
        tid = self.getTransformationID()
        if not tid['OK']:
            return tid
        self.transClient.createTransformationInputDataQuery(
                tid['Value'],
                {'DataLevel': 'Processed',
                 'DataType': 'Data',
                 'SoftwareVersion': 'katydid_%s' % self.software_tag,
                 'ConfigVersion': 'termite_%s' % self.config_tag,
                 'DataExt': 'root',
                 'DataFlavor': 'Event'})
        return S_OK(tid['Value'])

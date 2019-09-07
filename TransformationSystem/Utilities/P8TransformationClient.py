#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os, sys
import pdb
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

PATH_TO_SANDBOX = ('/project8/dirac/ts_processed/katydid_%s/termite_%s/scripts')

class P8Transformation(Transformation):
    def __init__(self, transID=0, transClient=None,
            software_tag=None, config_tag=None):
        super(P8Transformation, self).__init__(transID, transClient)

        if software_tag == None or config_tag == None:
            raise Exception('Must provide software and config tags')
        self.software_tag = software_tag
        self.config_tag = config_tag

    def buildTransformation(self):
        pdb.set_trace()
        path_to_sandbox = PATH_TO_SANDBOX % (self.software_tag, self.config_tag)
        # Use DIRAC Operation config
        ops_dict = Operations().getOptionsDict('Transformations/')
        if not ops_dict['OK']:
            return ops_dict
        ops_dict = ops_dict['Value']
        PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
        PROD_DEST_MONITORING_SE = ops_dict.get('ProdDestMonitoringSE', '')

        try:
            dirac = Dirac()
        except Exception:
            return S_ERROR('Failed to initialize Dirac object')
        cwd = os.getcwd()
        # Try to get the configuration file and if that doesn't work, then
        # quit out.
        cfg_file = os.path.join(path_to_sandbox, 'Katydid_ROACH_Config.yaml')
        print('cfg_file: %s\n' % cfg_file)
        res = dirac.getFile(cfg_file)
        if not res['OK']:
            return res
        if cfg_file in res['Value']['Failed']:
            return S_ERROR('Config file must be uploaded to %s. %s'
                    % (cfg_file, res['Value']['Failed'][cfg_file]))

        # The config file exists, so we can proceed.
        # Remove it, since getFile actually retrieves it, and we don't need it here.
        subprocess.check_call('rm ./Katydid_ROACH_Config.yaml', shell=True)
        # Create Katydid script
        script = (
                '#!/bin/bash\n'
                'CFG_FILE=$1\n'
                'P8_INPUT_FILE=`python -c \'import p8dirac_wms_tools as tools; '
                'print(tools.getJobFileName())\'`\n'
                '(source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh; ' 
                'Katydid -c ${CFG_FILE} -e ${P8_INPUT_FILE} )\n'
                'ls -l\n'
                '(source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh; '
                'python -c "import livetime as livetimetools; '
                'print(livetimetools.main())")\n'
                'python -c "import p8dirac_wms_tools as tools; '
                'print(tools.uploadJobOutputROOT(\'%s\', \'%s\'))"'
                % (self.software_tag, self.software_tag, self.software_tag, self.config_tag))
        script_name = os.path.join(cwd, 'katydid_%s.sh' % self.software_tag)
        f = open(script_name, 'w+')
        f.write(script)
        f.close()
        # Upload Katydid script
        katydid_file = os.path.join(
                path_to_sandbox,'katydid_%s.sh' % self.software_tag)
        print('katydid_file: %s\n' % katydid_file)
        print('katydid contents:\n%s\n' % script)
        res = dirac.removeFile(katydid_file) # First remove it
        if not res['OK']:
            return res
        res = dirac.addFile(katydid_file, script_name, PROD_DEST_DATA_SE)
        # Regardless of if the file was uploaded successfully, remove the temp file
        subprocess.check_call('rm %s' % script_name, shell=True)
        # Now check if the file was uploaded successfully
        if not res['OK']:
          return res

        # Upload live time file
        livetime_file = os.path.join(path_to_sandbox, 'livetime.py')
        print('livetime_file: %s\n' % livetime_file)
        res = dirac.removeFile(livetime_file)
        if not res['OK']:
            return res
        res = dirac.addFile(
                livetime_file,
                os.path.join(
                    cwd,
                    'livetime.py'),
                PROD_DEST_DATA_SE)

        if not res['OK']:
          return res

        # Get the tools file from the repo
        tmp_dir = os.path.join(
            cwd, 'p8_%s' % datetime.now().strftime('%Y-%m-%d_%H%M%S'))
        print('repo_dir: %s\n' % tmp_dir)
        subprocess.check_call(
                'git clone https://github.com/project8/Project8DIRAC.git %s'
                % tmp_dir, shell=True)
        os.chdir(tmp_dir) # temporary due to branch change
        subprocess.check_call(
                'git checkout origin/transformation-work -b transformation-work',
                shell=True)

        # TODO: do a git checkout tags/<tag> to get the specific tools tag

        # Upload tools file
        tools_file = os.path.join(path_to_sandbox, 'p8dirac_wms_tools.py')
        print('tools_file: %s\n' % tools_file)
        res = dirac.removeFile(tools_file)
        if not res['OK']:
            return res
        res = dirac.addFile(
                tools_file,
                os.path.join(
                    tmp_dir,
                    'TransformationSystem/Utilities/p8dirac_wms_tools.py'),
                PROD_DEST_DATA_SE)

        # Remove the tools file regardless of if the file was uploaded successfully
        os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        if not res['OK']:
          return res

        # Create a job and use the files we just uploaded as the input sandbox
        self.j = Job()
        self.j.setInputSandbox(
                ['LFN:%s' % cfg_file,
                 'LFN:%s' % katydid_file,
                 'LFN:%s' % tools_file,
                 'LFN:%s' % livetime_file])
        self.j.setExecutable(
                './' + os.path.basename(katydid_file),
                arguments=os.path.basename(cfg_file))
        self.j.setCPUTime(10000)
        self.j.setOutputSandbox(['env.txt', '*log', '*info*', '*xml*', '*json'])
        self.j.setName(
                'katydid_%s-termite_%s' % (self.software_tag, self.config_tag))

        # Set other parameters of this transformation
        self.setTransformationName(
                'runs37000-39000-katydid_%s-termite_%s_egg_v2'
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
                ## tester
                #{'DataLevel': 'RAW', 'DataType': 'Data', 'run_id': '7501'})
                # > 8603 (Tritium data starts at 8670)
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">":'8602'}})
                # 7501-7762
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">":'7500', "<":'7763'}})
                # 7965-8074
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">":'7964', "<":'8075'}})
                # 7416-7435 (Elise pressure study)
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">":'7415', "<":'7436'}})
                # 8603-8669
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">":'8602', "<":'8670'}})
                # Elise request: 7849-7902
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'7849', "<=":'7902'}})
                # Elise request: 7180-7207
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'7180', "<=":'7207'}})
                # Elise request: 7043-7371
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'7043', "<=":'7371'}})
                # Elise request: 8075-8599 
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'8075', "<=":'8599'}})
                # Elise request: 7803-7839 
                #{'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'11115'}})
                {'DataLevel': 'RAW', 'DataType': 'Data','run_id': {">=":'37000', "<=":'39000'}})
        # above run_id is temporarily there for testing
        return S_OK(tid['Value'])

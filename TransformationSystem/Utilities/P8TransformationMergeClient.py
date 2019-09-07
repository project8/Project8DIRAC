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
import pdb
import subprocess
from datetime import datetime

PATH_TO_SANDBOX = ('/project8/dirac/ts_processed/katydid_%s/termite_%s/scripts')

class P8Transformation(Transformation):
    def __init__(self, transID=0, transClient=None,
            software_tag=None, config_tag=None, run_start=None, run_end=None, version=None):
        super(P8Transformation, self).__init__(transID, transClient)

        if software_tag == None or config_tag == None:
            raise Exception('Must provide software and config tags')
        self.software_tag = software_tag
        self.config_tag = config_tag
        self.run_start = run_start
        self.run_end = run_end
        self.version = version


    def buildTransformation(self):
        path_to_sandbox = PATH_TO_SANDBOX % (self.software_tag, self.config_tag)
        # Use DIRAC Operation config
        ops_dict = Operations().getOptionsDict('Transformations/')
        if not ops_dict['OK']:
            return ops_dict
        ops_dict = ops_dict['Value']
        PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
        #PROD_DEST_MONITORING_SE = ops_dict.get('ProdDestMonitoringSE', '')

        try:
            dirac = Dirac()
        except Exception:
            return S_ERROR('Failed to initialize Dirac object')

        cwd = os.getcwd()
        #pdb.set_trace()
        # Create root merge script
        script = (
                '#!/bin/bash\n'
                'python -c "import p8dirac_merge_tools as tools; print(tools.uploadJobOutputROOT())"\n'
		)
        script_name = os.path.join(cwd, 'merge_using_katydid_%s.sh' % self.software_tag)
        #print(script_name)
        f = open(script_name, 'w+')
        f.write(script)
        f.close()
        
        # Upload Katydid script
        katydid_file = os.path.join(
                path_to_sandbox,'merge_using_katydid_%s.sh' % self.software_tag)
        print('katydid_file: %s\n' % katydid_file)
        print('katydid contents:\n%s\n' % script)
        '''
        res = dirac.removeFile(katydid_file) # First remove it
        if not res['OK']:
            return res
        res = dirac.addFile(katydid_file, script_name, PROD_DEST_DATA_SE)
        # Regardless of if the file was uploaded successfully, remove the temp file
        subprocess.check_call('rm %s' % script_name, shell=True)
        # Now check if the file was uploaded successfully
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
                'git checkout origin/transformation-work-development -b transformation-work-development',
                shell=True)
        '''
        # TODO: do a git checkout tags/<tag> to get the specific tools tag
        # Upload tools file
        tools_file = os.path.join(path_to_sandbox, 'p8dirac_merge_tools.py')
        '''
        print('tools_file: %s\n' % tools_file)
        #pdb.set_trace()
        
        res = dirac.removeFile(tools_file)
        if not res['OK']:
            print('tools file could not be removed.')
            return res
        print(os.path.join(cwd, 'p8dirac_merge_tools.py'))
        res = dirac.addFile(
                tools_file,
                os.path.join(cwd, 'p8dirac_merge_tools.py'),
                PROD_DEST_DATA_SE)

        if not res['OK']:
            print('tools file not uploaded')
            return res
        
        # Remove the tools file regardless of if the file was uploaded successfully
        os.chdir(os.path.dirname(tmp_dir))
        subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        if not res['OK']:
          return res
        '''
        # Create a job and use the files we just uploaded as the input sandbox
        #pdb.set_trace()
        self.j = Job()
        self.j.setInputSandbox(
                ['LFN:%s' % katydid_file,
                 'LFN:%s' % tools_file])
        self.j.setExecutable(
                './' + os.path.basename(katydid_file))
        self.j.setCPUTime(10000)
        self.j.setOutputSandbox(['env.txt', '*log', '*info*', '*xml*', '*json'])
        self.j.setName(
                'merged_katydid_%s-termite_%s' % (self.software_tag, self.config_tag))

        # Set other parameters of this transformation
        #self.setTransformationName(
        #        'run%s-%s-merge-attempt4-katydid_%s-termite_%s_%s'
        #        % (self.run_start, self.run_end, self.software_tag, self.config_tag, self.version))
        self.setTransformationName(
                'run%s-%s-merge-katydid_%s-termite_%s_%s'
                % (self.run_start, self.run_end, self.software_tag, self.config_tag, self.version))
        self.setTransformationGroup('KatydidMetadataMerge_v1')
        self.setType('DataReprocessing')
        self.setDescription(
                'Transformation used to automatically merge processed RAW data')
        self.setLongDescription(
                'Transformation use to automatically merge processed RAW data. '
                'Triggered using the metadata fields.')
        self.setMaxNumberOfTasks(0)
        self.setBody(self.j.workflow.toXML())
        self.setPlugin('P8Merge')
        self.setGroupSize(1)
        self.addTransformation()
        self.setStatus('Active')
        self.setAgentType('Automatic')
        tid = self.getTransformationID()
        if not tid['OK']:
          return tid
        # Create metadata for input query
        inputDataQuery = {'DataType': 'data', 'DataLevel': 'processed', 'DataExt': 'root', 'DataFlavor': 'event'}
        inputDataQuery.update({'SoftwareVersion': 'katydid_%s' % self.software_tag})
        inputDataQuery.update({'ConfigVersion': 'termite_%s' % self.config_tag })
        #Test 
        #inputDataQuery.update({'run_id': '8603'})
        #  > 8603 (Tritium data starts at 8670)
        #inputDataQuery.update({'run_id': {">":'8603'}})
        # 7501-7762
        #inputDataQuery.update({'run_id': {">":'7500', "<":'7763'}})
        # 7965-8074
        #inputDataQuery.update({'run_id': {">":'7964', "<":'8075'}})
        # 7416-7435
        #inputDataQuery.update({'run_id': {">":'7420', "<":'7436'}})
        # 7849-7902
        #inputDataQuery.update({'run_id': {">":'7848', "<":'7903'}})
        # 7180-7207
        #inputDataQuery.update({'run_id': {">":'7179', "<":'7208'}})        
        # 7031-7037
        #inputDataQuery.update({'run_id': {">":'7030', "<":'7038'}})        
        # 7043-7371
        #inputDataQuery.update({'run_id': {">":'7042', "<":'7372'}})
        # 8075-8599
        #inputDataQuery.update({'run_id': {">":'8074', "<":'8600'}})
        # 7803-7839 
        #[7427, 7429, 7430, 7432, 7433, 7435, 8384, 8098]
        #inputDataQuery.update({'run_id': {">":'10010', "<":'10084'}})
        #pdb.set_trace()
        #inputDataQuery.update({'run_id': {">":'8602', "<":'8604'}})
        #inputDataQuery.update({'run_id': {">":'9850', "<":'9852'}})
        #inputDataQuery.update({'run_id': {">":'9851', "<":'9860'}})
        #inputDataQuery.update({'run_id': {">":'9859', "<":'9901'}})
        #inputDataQuery.update({'run_id': {">":'9900', "<":'10000'}})
        #inputDataQuery.update({'run_id': {">":'9999', "<":'10011'}})
        #inputDataQuery.update({'run_id': {">":'7426', "<":'7428'}})
        #inputDataQuery.update({'run_id': {">":'7428', "<":'7436'}})
        #inputDataQuery.update({'run_id': {">":'8383', "<":'8385'}})
        #inputDataQuery.update({'run_id': {">":'8079'}})
        #inputDataQuery.update({'run_id': {">":'8000', "<":'9001'}})
        self.transClient.createTransformationInputDataQuery(
                tid['Value'], {'DataLevel': 'processed', 'DataType': 'Data','run_id': {">=":self.run_start, "<=":self.run_end}, 'DataExt': 'root', 'DataFlavor': 'event', 'SoftwareVersion': 'katydid_%s' % self.software_tag, 'ConfigVersion': 'termite_%s' % self.config_tag        })
        #self.transClient.createTransformationInputDataQuery(
        #        tid['Value'], {'DataLevel': 'processed', 'DataType': 'Data','run_id': {">=":self.run_start}, 'DataExt': 'root', 'DataFlavor': 'event', 'SoftwareVersion': 'katydid_%s' % self.software_tag, 'ConfigVersion': 'termite_%s' % self.config_tag        })
        #self.transClient.createTransformationInputDataQuery(
        #        tid['Value'],inputDataQuery)
        # above run_id is temporarily there for testing
        return S_OK(tid['Value'])

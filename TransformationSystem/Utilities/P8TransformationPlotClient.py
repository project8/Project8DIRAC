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
        # Create root merge script 
        script = (
                '#!/bin/bash\n'
                'INPUT_LFN=`python -c "import p8dirac_plot_tools as tools; print(tools.getPlotJobLFNs())"`\n'
                'python -c "import p8dirac_plot_tools as tools; print(tools.check_lfn_health())"\n'
                '(source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh; '
                'python -c "import p8_plot_initiate as tools; print(tools.initiate_plots())" ${INPUT_LFN})\n'
                'ls -l\n'
                'python -c "import p8dirac_plot_tools as tools; print(tools.uploadJobOutputROOT())"\n'
                % (self.software_tag)) 
        script_name = os.path.join(cwd, 'plot_%s.sh' % self.software_tag)
        f = open(script_name, 'w+')
        f.write(script)
        f.close()
        # Upload Plot script
        plot_file = os.path.join(
                path_to_sandbox,'plot_%s.sh' % self.software_tag)
        print('plot_file: %s\n' % plot_file)
        print('plot contents:\n%s\n' % script)
        res = dirac.removeFile(plot_file) # First remove it
        if not res['OK']:
            return res
        res = dirac.addFile(plot_file, script_name, PROD_DEST_DATA_SE)
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

        # TODO: do a git checkout tags/<tag> to get the specific tools tag

        # Upload tools file
        tools_file = os.path.join(path_to_sandbox, 'p8dirac_plot_tools.py')
        print('tools_file: %s\n' % tools_file)
        res = dirac.removeFile(tools_file)
        #print(res)
        if not res['OK']:
            print('Could not remove tools file.')
            return res
        res = dirac.addFile(
                tools_file,
                os.path.join(cwd, 'p8dirac_plot_tools.py'),
                PROD_DEST_DATA_SE)
        #print(res)
        # Remove the tools file regardless of if the file was uploaded successfully
        #os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        if not res['OK']:
            print('Could not add tools file.')
            return res

        # Upload plots file
        #plot_files_to_upload = ['p8_plot_initiate.py', 'postprocessing.py', 'postprocessor.py']
        #for file in plot_files_to_upload:
        plot_tools_file_initiate = os.path.join(path_to_sandbox, 'p8_plot_initiate.py')
        print('plot_tools_file: %s\n' % plot_tools_file_initiate)
        res = dirac.removeFile(plot_tools_file_initiate)
        #print(res)
        if not res['OK']:
	    print('Could not remove tools file.')
	    return res
        res = dirac.addFile(
	        plot_tools_file_initiate,
	        os.path.join(cwd, 'p8_plot_initiate.py'),
	        PROD_DEST_DATA_SE)
        #print(res)
        if not res['OK']:
            print('Could not add tools file.')
	    return res

        plot_tools_file_processing = os.path.join(path_to_sandbox, 'postprocessing.py')
        print('plot_tools_file: %s\n' % plot_tools_file_processing)
        res = dirac.removeFile(plot_tools_file_processing)
        #print(res)
        if not res['OK']:
            print('Could not remove tools file.')
            return res
        res = dirac.addFile(
                plot_tools_file_processing,
                os.path.join(cwd, 'postprocessing.py'),
                PROD_DEST_DATA_SE)
        #print(res)
        if not res['OK']:
            print('Could not add tools file.')
            return res

        plot_tools_file = os.path.join(path_to_sandbox, 'postprocessor.py')
        print('plot_tools_file: %s\n' % plot_tools_file)
        res = dirac.removeFile(plot_tools_file)
        #print(res)
        if not res['OK']:
            print('Could not remove tools file.')
            return res
        res = dirac.addFile(
                plot_tools_file,
                os.path.join(cwd, 'postprocessor.py'),
                PROD_DEST_DATA_SE)
        #print(res)
        if not res['OK']:
            print('Could not add tools file.')
            return res

	# Remove the tools file regardless of if the file was uploaded successfully
	os.chdir(os.path.dirname(tmp_dir))
        subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        print('Creating the job')
        # Create a job and use the files we just uploaded as the input sandbox

        self.j = Job()
        self.j.setInputSandbox(
                ['LFN:%s' % plot_file,
                 'LFN:%s' % tools_file,
                 'LFN:%s' % plot_tools_file,
                 'LFN:%s' % plot_tools_file_processing,
                 'LFN:%s' % plot_tools_file_initiate])
        self.j.setExecutable(
                './' + os.path.basename(plot_file))
        self.j.setCPUTime(10000)
        self.j.setOutputSandbox(['env.txt', '*log', '*info*', '*xml*', '*json'])
        self.j.setName(
                'plot_katydid_%s-termite_%s' % (self.software_tag, self.config_tag))
        #38
        # Set other parameters of this transformation
        self.setTransformationName(
                'run18686plus-plot-katydid_%s-termite_%s_v3'
                % (self.software_tag, self.config_tag))
        self.setTransformationGroup('KatydidMetadataPlot')
        self.setType('DataReprocessing')
        self.setDescription(
                'Transformation used to automatically plot processed RAW data')
        self.setLongDescription(
                'Transformation use to automatically plot processed RAW data. '
                'Triggered using the metadata fields.')
        self.setMaxNumberOfTasks(0)
        self.setBody(self.j.workflow.toXML())
        self.setPlugin('Standard')
        self.setGroupSize(1)
        self.addTransformation()
        self.setStatus('Active')
        self.setAgentType('Automatic')
        tid = self.getTransformationID()
        if not tid['OK']:
          return tid
        # Create metadata for input query
        inputDataQuery = {'DataType': 'data', 'DataLevel': 'processed', 'DataExt': 'root', 'DataFlavor': 'merged'}
        inputDataQuery.update({'SoftwareVersion': 'katydid_%s' % self.software_tag})
        inputDataQuery.update({'ConfigVersion': 'termite_%s' % self.config_tag })
        #Test 
        #inputDataQuery.update({'run_id': {">":'10008', "<":'10010'}})
        #  > 8603 (Tritium data starts at 8670)
        #inputDataQuery.update({'run_id': {">":'8602'}})
        # 7501-7762
        #inputDataQuery.update({'run_id': {">":'7500', "<":'7763'}})
        # 7965-8074
        #inputDataQuery.update({'run_id': {">":'7964', "<":'8075'}})
        inputDataQuery.update({'run_id': {">=":'18686', "<":'18686000'}})
        self.transClient.createTransformationInputDataQuery(
                tid['Value'],inputDataQuery)
        # above run_id is temporarily there for testing
        return S_OK(tid['Value'])


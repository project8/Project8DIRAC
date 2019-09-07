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

PATH_TO_SANDBOX = ('/project8/dirac/calib/rf_bkgd/scripts')

class P8Transformation(Transformation):
    def __init__(self, transID=0, transClient=None):
        super(P8Transformation, self).__init__(transID, transClient)


    def buildTransformation(self):
        path_to_sandbox = PATH_TO_SANDBOX
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
        # Create rf_bkgd plot script 
        script = (
                '#!/bin/bash\n'
                'python -c "import p8dirac_rfbkgdplot_tools as tools; print(tools.getPlotJobLFNs())"\n'
                'ls -l\n'
                '(source /cvmfs/hep.pnnl.gov/project8/katydid/v2.16.0/setup.sh; '
                'python -c "import p8_rfbkgdplot_tools as tools; print(tools.execute())")\n'
                'ls -l\n'
                'python -c "import p8dirac_rfbkgdplot_tools as tools; print(tools.uploadJobOutputROOT())"\n') 
        script_name = os.path.join(cwd, 'plot_rfbkgd.sh')
        f = open(script_name, 'w+')
        f.write(script)
        f.close()
        # Upload Plot script
        plot_file = os.path.join(
                path_to_sandbox,'plot_rfbkgd.sh')
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
        tools_file = os.path.join(path_to_sandbox, 'p8dirac_rfbkgdplot_tools.py')
        print('tools_file: %s\n' % tools_file)
        res = dirac.removeFile(tools_file)
        #print(res)
        if not res['OK']:
            print('Could not remove tools file.')
            return res
        res = dirac.addFile(
                tools_file,
                os.path.join(cwd, 'p8dirac_rfbkgdplot_tools.py'),
                PROD_DEST_DATA_SE)
        #print(res)
        # Remove the tools file regardless of if the file was uploaded successfully
        #os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        if not res['OK']:
            print('Could not add tools file.')
            return res

        # Upload plots file
        plot_tools_file = os.path.join(path_to_sandbox, 'p8_rfbkgdplot_tools.py')
        print('plot_tools_file: %s\n' % plot_tools_file)
        res = dirac.removeFile(plot_tools_file)
        #print(res)
        if not res['OK']:
	    print('Could not remove tools file.')
	    return res
        res = dirac.addFile(
	        plot_tools_file,
	        os.path.join(cwd, 'p8_rfbkgdplot_tools.py'),
	        PROD_DEST_DATA_SE)
        #print(res)
        if not res['OK']:
            print('Could not add tools file.')
	    return res


	# Remove the tools file regardless of if the file was uploaded successfully
	#os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

	# Add upload file script
        upload_file = os.path.join(
                path_to_sandbox,'upload_rfbkgd.py')
        print('upload_file: %s\n' % upload_file)
        #print('plot contents:\n%s\n' % script)
        res = dirac.removeFile(upload_file) # First remove it
        if not res['OK']:
            return res
        res = dirac.addFile(upload_file, os.path.join(cwd, 'upload_rfbkgd.py'), PROD_DEST_DATA_SE)
        # Now check if the file was uploaded successfully
        if not res['OK']:
          return res
        # Remove the tools file regardless of if the file was uploaded successfully
        #os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

        # Add p8dirac_safe_access script
        safe_access_file = os.path.join(
                path_to_sandbox,'p8dirac_safe_access.py')
        print('safe_access_file: %s\n' % safe_access_file)
        #print('plot contents:\n%s\n' % script)
        res = dirac.removeFile(safe_access_file) # First remove it
        if not res['OK']:
            return res
        res = dirac.addFile(safe_access_file, os.path.join(cwd, 'p8dirac_safe_access.py'), PROD_DEST_DATA_SE)
        # Now check if the file was uploaded successfully
        if not res['OK']:
          return res
        #os.chdir(os.path.dirname(tmp_dir))
        #subprocess.check_call('rm -rf %s' % tmp_dir, shell=True)

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
                 'LFN:%s' % upload_file,
                 'LFN:%s' % safe_access_file])
        self.j.setExecutable(
                './' + os.path.basename(plot_file))
        self.j.setCPUTime(10000)
        self.j.setOutputSandbox(['env.txt', '*log', '*info*', '*xml*', '*json'])
        self.j.setName(
                'plot_calib_rfbkgd')
        #38
        # Set other parameters of this transformation
        self.setTransformationName(
                'plot-calib_rf_bkgd_v1'
                )
        self.setTransformationGroup('CalibMetadataEsrPlot')
        self.setType('DataReprocessing')
        self.setDescription(
                'Transformation used to automatically plot calib rf_bkgd data')
        self.setLongDescription(
                'Transformation use to automatically plot calib rf_bkgd data. '
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
        inputDataQuery = {'DataType': 'calib', 'DataLevel': 'raw', 'DataFlavor': 'rf_bkgd'}
        #Test 
        #inputDataQuery.update({'run_id': {">":'10008', "<":'10010'}})
        #  > 8603 (Tritium data starts at 8670)
        #inputDataQuery.update({'run_id': {">":'8602'}})
        # 7501-7762
        #inputDataQuery.update({'run_id': {">":'7500', "<":'7763'}})
        # 7965-8074
        #inputDataQuery.update({'run_id': {">":'7964', "<":'8075'}})
        #inputDataQuery.update({'run_id': {">":'10300'}})
        self.transClient.createTransformationInputDataQuery(
                tid['Value'],inputDataQuery)
        # above run_id is temporarily there for testing
	print(tid)
        return S_OK(tid['Value'])


#!/usr/bin/env python
'''
p8-dirac-wms-submit-job
Author: MG
Date: 04/16/2018
Details: A Utility script for submitting jobs using a convenient YAML config file.
'''
from p8_base_script import BaseScript

# dirac requires that we set this
__RCSID__ = '$Id$'

class SubmitJob(BaseScript):
    '''
    '''
    switches = [
                ('f:', 'filename', 'YAML/JSON config file to use', None),
                ('l', 'local', 'Run job locally', False),
               ]
    def main(self):

        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Interfaces.API.Dirac import Dirac
        from DIRAC.Interfaces.API.Job import Job
        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
        import os

        if self.filename.endswith(".json"):
            import json as reader
        elif self.filename.endswith(".yaml"): 
            import yaml as reader
        else:
            gLogger.error("Dont support file format.{}".format(self.filename))
            DIRAC_exit(1)
        with open(str(self.filename), 'r') as stream:
            try:
                info = reader.load(stream)
            except ValueError as exc:
                gLogger.error(exc)
                DIRAC_exit(1)
        
        # Put the files into an archive for easy upload
        if 'input_files' in info.keys() and len(info['input_files'])>0:
            gLogger.info("Found list of files to input in Sandbox: {}".format(info['input_files']))
            import tarfile
            tar_sourcefolder_fname = info['job_name']+'_archive.tar'
            if os.path.exists(tar_sourcefolder_fname):
                gLogger.info('removing existing archive {}'.format(tar_sourcefolder_fname))
                os.remove(tar_sourcefolder_fname)

            gLogger.info('creating archive {}'.format(tar_sourcefolder_fname))
            with tarfile.open(tar_sourcefolder_fname, "w:gz") as tar:
                for item in info['input_files']:
                    gLogger.info("adding {} as {}".format(item["file"],item["alias"]))
                    tar.add(item["file"], arcname=item["alias"] or item["file"])
        # Creates a script for tar'ing the archive
        tar_archive_script = info['job_name']+'_tar_archive.sh'
        with open(tar_archive_script, 'w') as stream:
            stream.write("tar -xvf {}\n".format(tar_sourcefolder_fname))

        # Preparing main analysis into a bash script. Includes:
        # - sourc'ing of the desired environment (latest Katydid or latest morpho or latest mermithid)
        # - executing what the user wants (from config file)
        list_cmd_main_analysis = []
        if 'analysis_env' not in info.keys():
            gLogger.warning("setting default analysis env: mermithid")
            analysis_env = "mermithid"
        else:
            analysis_env = info['analysis_env']
        if 'version_env' not in info.keys():
            gLogger.warning("setting default env version: current")
        else:
            env_version = info['version_env']
        setup_file = "/cvmfs/hep.pnnl.gov/project8/{}/{}/setup.sh".format(analysis_env,env_version)
        list_cmd_main_analysis.append("echo 'source "+setup_file+"'")
        list_cmd_main_analysis.append("source {}".format(setup_file))
        for cmd in info["analysis_cmd"]:
            list_cmd_main_analysis.append(str(cmd))
        main_analysis_script = info['job_name']+'_main_analysis_script.sh'
        with open(main_analysis_script, 'w') as stream:
            for line in list_cmd_main_analysis:
                stream.write(str(line)+"\n")

        # Collect the input data requested for this analysis
        inputdata_list = []
        fcc = FileCatalogClient()
        for a_dict in info['input_data']:
            if "runID" in a_dict.keys() and "analysis" in a_dict.keys():
                if isinstance(a_dict['runID'],list):
                    for runID in a_dict['runID']:
                        inputdata_list.extend(self._getListLFN(a_dict['analysis'],runID,a_dict.get("endswith")))
                else:
                    inputdata_list.extend(self._getListLFN(a_dict['analysis'],a_dict['runID'],a_dict.get("endswith")))
            elif "file" in a_dict.keys():
                status = fcc.exists(a_dict['file'])
                if not status['Value']['Successful'][a_dict['file']]:
                    gLogger.error("file {} does not exist".format(a_dict['file']))
                    DIRAC_exit(1)
                inputdata_list.append(a_dict['file'])
            else:
                gLogger.error("could not parse {}".format(a_dict))
        lfnpath_list = []
        for item in inputdata_list:
            gLogger.info("Job_submitter: new item to upload {}".format(item))
            lfnpath_list.append('LFN:{}'.format(item))
        gLogger.info('Job submitter: {}'.format(info['job_name']))
        j = Job()
        j.setName(info['job_name'])
        j.setCPUTime(info['cpu_time'])
        j.setDestination(info['destination'])
        j.setLogLevel(info['log_level']) 
        j.setInputSandbox([tar_sourcefolder_fname, tar_archive_script, main_analysis_script])
        # Step 1: untar the input files
        j.setExecutable(tar_archive_script)
        # Step 2: Do the main analysis
        j.setExecutable(main_analysis_script)
        # Step 3: Do the post analysis things
        if 'job_mode' not in info.keys():
            mode = "wms"
        else:
            mode = info['job_mode']
        # CLI replaces definition in config file.
        if self.local:
            gLogger.info("Local job!")
            mode = "local"
        if mode == "local":
            gLogger.always("Warning: local job -> no in/output data")
        else:
            j.setInputData(lfnpath_list)
            j.setOutputSandbox(['std.err', 'std.out'].extend(info['output_files']))
            j.setOutputData(info['output_files'],info['output_SE'])

        # submit the job
        dirac = Dirac()

        result = dirac.submit(j,mode=mode)
        gLogger.always("Results: \n{}".format(j._toJDL()))

        gLogger.info("Cleaning up!")
        os.remove(tar_sourcefolder_fname)
        os.remove(main_analysis_script)
        os.remove(tar_archive_script)


    def _returnPath(self, analysis, runID):
        path = ""
        if analysis.startswith("katydid"):
            path+="/project8/dirac/proc"
            if runID>2772:
                path+= "/%03dyyyxxx/%06dxxx/%09d" % (int(runID/1e6),int(runID/1e3),int(runID))
            else:
                path+="/%09d" % (int(runID))
            path+="/{}".format(analysis)
        elif analysis=="calib":
            path+="/project8/dirac/calib"
        return path
        
    def _getListLFN(self,analysis,runID,endswith=None):
        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
        fcc = FileCatalogClient()
        path=self._returnPath(analysis,runID)
        lfnlist = sorted([lfn for lfn in
            fcc.findFilesByMetadata(
                {'run_id': {'=': runID}}, path=path)['Value']])
        if endswith is not None:
            lfnlist = [lfn for lfn in lfnlist if lfn.endswith(endswith)]
        if len(lfnlist)==0:
            gLogger.error("Couldn't find files with {} in {} ending with {}".format(runID,analysis,endswith))
            DIRAC_exit(1)
        return lfnlist

# make it able to be run from a shell
if __name__ == "__main__":
    script = SubmitJob()
    script()

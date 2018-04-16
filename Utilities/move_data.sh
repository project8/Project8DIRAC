#! /bin/bash

echo "hostname:" `/bin/hostname`
echo " "; echo " "

# DIRAC has the default value /opt/dirac/pro which does not exist in the
# cluster environment.  You MUST unset DIRAC before running setup_katydid.sh
# (it only sets the correct value if the current value is empty).
unset DIRAC

source /cvmfs/hep.pnnl.gov/project8/katydid-2.7.0/setup_katydid.sh

echo " "; echo " "

echo " "; echo " "
echo "pre-job directory list:"
ls -l
echo " "; echo " "

echo "command: python move_data_fc.py $@"
echo " "; echo " "
python move_data_fc.py $@

echo " "; echo " "
echo "post-job directory list:"
ls -l



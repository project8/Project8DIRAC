import os
cwd = os.getcwd()
# Create rfbkgd plot script
script = (
	'#!/bin/bash\n'
	'python -c "import p8dirac_rfbkgdplot_tools as tools; print(tools.getPlotJobLFNs())"\n'
	'ls -l\n'
	'(source /cvmfs/hep.pnnl.gov/project8/katydid/%s/setup.sh; '
	'python -c "import p8_rfbkgdplot_tools as tools; print(tools.execute())")\n'
	'ls -l\n'
	'python -c "import p8dirac_rfbkgdplot_tools as tools; print(tools.uploadJobOutputROOT())"\n')
script_name = os.path.join(cwd, 'plot_rfbkgd.sh')
f = open(script_name, 'w+')
f.write(script)
f.close()


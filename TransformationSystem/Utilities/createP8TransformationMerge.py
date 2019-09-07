from P8TransformationMergeClient import P8Transformation

#T = P8Transformation(software_tag='v2.14.0', config_tag='v1.2.0') # or whatever the tags should be
## Replace run_end with a large number
start_no = 38000
inc_by = 49
stop_at = 38500
for i in range(1,10000):
	T = P8Transformation(software_tag='v2.17.1', config_tag='v2.2.1', run_start=str(start_no), run_end=str(start_no + inc_by), version='v2') # or whatever the tags should be
	res = T.buildTransformation()
	print('Loop %s: %s - %s' %(str(i), str(start_no), str(start_no + inc_by)))
	start_no = start_no + inc_by + 1
	if start_no >= stop_at:
		break

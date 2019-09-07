from P8TransformationClient import P8Transformation

#T = P8Transformation(software_tag='v2.14.0', config_tag='v1.2.0', run_start='8600', run_end='8601', version='v1') # or whatever the tags should be
#T = P8Transformation(software_tag='v2.14.0', config_tag='v1.2.0')#, run_start='8600', run_end='8601', version='v1') # or whatever the tags should be
T = P8Transformation(software_tag='v2.17.1', config_tag='v2.2.1')
res = T.buildTransformation()

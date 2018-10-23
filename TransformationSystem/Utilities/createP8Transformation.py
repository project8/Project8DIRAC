from P8TransformationClient import P8Transformation

T = P8Transformation(software_tag='v2.14.0', config_tag='v1.2.0') # or whatever the tags should be
res = T.buildTransformation()

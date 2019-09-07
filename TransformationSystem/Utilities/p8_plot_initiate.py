import postprocessing
import json
import os
def initiate_plots():
    tree_name = 'multiTrackEvents'
    with open('plot_config.txt') as f:
        json_data = json.load(f)
    input_file = str(json_data['input_file'])
    input_file = os.path.basename(input_file)
    status = int(json_data['status'])
    #print('The input file into the plots code is: %s.' %input_file)
    #print('The status of file health check is: %s.' %str(status))
    if not status > 0:
        print('File is not good')
        sys.exit(-9)

    postprocessing.data_quality(input_file, tree_name, True)

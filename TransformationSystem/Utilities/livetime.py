import h5py

#import p8dirac_wms_tools as tools

def main():
    #eggFN = tools.getJobFileName()
    #eggFN = "rid000007427_21.egg"
    import os, os.path
    dir = os.getcwd()
    for root, dirs, files in os.walk(dir):
        for f in files:
            fullpath = os.path.join(root, f)
            if os.path.splitext(fullpath)[1] == '.egg':
                print('Egg file for livetime is ' + f)
                eggFN = f
    print("Getting Livetime values...")
    print("eggFN is " + eggFN)
    f = h5py.File( eggFN, "r" )

    acquisition_attrs = {}
    acquisitions = f["streams"]["stream0"]["acquisitions"]
    overallStartTime = 100000.0
    overallEndTime = 0.0

    for k in acquisitions.keys():
        acquisition_attrs[k] = {}
    
        for a in acquisitions[k].attrs.keys():
            acquisition_attrs[k][a] = int(acquisitions[k].attrs[a])

        if overallStartTime > acquisition_attrs[k]['first_record_time']*1e-9:
    	    overallStartTime = acquisition_attrs[k]['first_record_time']*1e-9;
        if overallEndTime < acquisition_attrs[k]['first_record_time']*1e-9 + 4.096e-5*acquisition_attrs[k]['n_records']:
    	    overallEndTime = acquisition_attrs[k]['first_record_time']*1e-9 + 4.096e-5*acquisition_attrs[k]['n_records'];

    liveTime = overallEndTime - overallStartTime
    print( "Time in run: " + str( overallStartTime ) + " -- " + str( overallEndTime ) )
    print( "Calculated live time over all acquistions: " + str( liveTime ) )

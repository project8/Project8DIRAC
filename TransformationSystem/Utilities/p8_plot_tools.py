#!/usr/bin/env python

import os
import sys
import json
import collections
import subprocess
import ROOT
import datetime
'''
rightMargin = 0.1
leftMargin = 0.09
botMargin = 0.11
topMargin = 0.06
legWidth=0.2
legHeight=0.3

style = ROOT.TStyle(ROOT.gStyle)
style.SetOptStat("nem")
style.SetStatY(1.-topMargin)
style.SetStatX(1.-rightMargin)
style.SetStatW(0.2)
style.SetStatH(0.2)
style.SetLabelOffset(0,'xy')
style.SetLabelSize(0.05,'xy')
style.SetTitleOffset(0.9,'y')
style.SetTitleSize(0.05,'y')
style.SetLabelSize(0.05,'y')
style.SetLabelOffset(0,'y')
style.SetTitleSize(0.05,'x')
style.SetLabelSize(0.05,'x')
style.SetTitleOffset(1.02,'x')
style.SetPalette(53) # Dark Body radiator colormap
style.SetPadRightMargin(rightMargin)
style.SetPadTopMargin(topMargin)
style.SetPadBottomMargin(botMargin)
style.SetPadLeftMargin(leftMargin)
style.cd()
'''

def quality_plots(output_dir=None):
    tree_name = 'multiTrackEvents'
    input_file = sys.argv[-1]
    with open('plot_config.txt') as f:
        json_data = json.load(f)
    input_file = str(json_data['input_file'])
    input_file = os.path.basename(input_file)
    status = int(json_data['status'])
    print('The input file into the plots code is: %s.' %input_file)
    #print('The status of file health check is: %s.' %str(status))
    #print(int(sys.argv[-2]))
    if not status > 0:
        print('File is not good')
        sys.exit(-9) 
    rightMargin = 0.1
    leftMargin = 0.09
    botMargin = 0.11
    topMargin = 0.06
    legWidth=0.2
    legHeight=0.3

    style = ROOT.TStyle(ROOT.gStyle)
    style.SetOptStat("nem")
    style.SetStatY(1.-topMargin)
    style.SetStatX(1.-rightMargin)
    style.SetStatW(0.2)
    style.SetStatH(0.2)
    style.SetLabelOffset(0,'xy')
    style.SetLabelSize(0.05,'xy')
    style.SetTitleOffset(0.9,'y')
    style.SetTitleSize(0.05,'y')
    style.SetLabelSize(0.05,'y')
    style.SetLabelOffset(0,'y')
    style.SetTitleSize(0.05,'x')
    style.SetLabelSize(0.05,'x')
    style.SetTitleOffset(1.02,'x')
    style.SetPalette(53) # Dark Body radiator colormap
    style.SetPadRightMargin(rightMargin)
    style.SetPadTopMargin(topMargin)
    style.SetPadBottomMargin(botMargin)
    style.SetPadLeftMargin(leftMargin)
    style.cd()
    '''
    Create quality plots of the relevant quantities contained into the tree of a root file.
    The plots are saved as pdf in the output_dir.
    Some cosmetic elements require the name of the files to be as events_000001097_katydid_v2.7.0_concat.root
    If changed, modify #1
    '''
    # variable to plot
    plotted_var = [
        'Event.fStartTimeInAcq',
        'Event.fTimeLength',
        'Event.fStartFrequency',
        'Event.fFirstTrackSlope',
        'Event.fFirstTrackTimeLength',
        'Event.fTracks.fSlope',
        'Event.fTotalEventSequences'
    ]
    # title for the x axis
    title_plotted_var = [
        'StartTimeInAcq [s]',
        'TimeLength [s]',
        'StartFrequency [Hz]',
        'FirstTrackSlope [Hz/s]',
        'FirstTrackTimeLength [s]',
        'Tracks Slope [Hz/s]',
        'Number of tracks per event'
    ]
    # enable/disable log y scale 
    scale_plotted_var = [
        0,
        1,
        0,
        0,
        1,
        0,
        1
    ]

    #more complex plots (in case of failure)
    more_complex_plot = [
        'JumpSize',
        'JumpSizeBetweenEvents',
        'JumpLength',
        'JumpLengthClose',
        'TrackEventLengths',
        'NumberTracksInEvent'
    ]
    more_complex_plot_title = [
        "Jump size [MHz]",
        "Jump size [MHz]",
        "Jump length between events [s]",
        "Jump length between events [s]",
        "Time [s]",
        "Number of tracks per event"
    ]

    # Transform list into single element
    if isinstance(input_file,list):
        input_file = input_file[0]
    input_filename = os.path.basename(os.path.splitext(input_file)[0])

    list_pfn_plots = []

    # Timestamp box
    timestampBox = ROOT.TPaveText(0.001,
                       0.001,
                       0.45,
                       0.06,
                       "BRNDC")
    timestampBox.SetTextColor(1)
    timestampBox.SetFillColor(0)
    timestampBox.SetTextAlign(12)
    timestampBox.SetTextSize(0.04)
    timestampBox.SetBorderSize(1)
    timestampBox.AddText(datetime.datetime.now().strftime("%a, %d. %b %Y %I:%M:%S%p UTC"))
    
    # empty tree box
    emptyTreeBox = ROOT.TPaveText(0.31,
                       0.41,
                       0.6,
                       0.6)
                    #    "BRNDC")
    emptyTreeBox.SetTextColor(4)
    emptyTreeBox.SetFillColor(0)
    emptyTreeBox.SetTextAlign(22)
    emptyTreeBox.SetTextAngle(45)
    emptyTreeBox.SetTextSize(0.04)
    emptyTreeBox.SetBorderSize(0)
    emptyTreeBox.AddText("Tree {}".format(tree_name))
    emptyTreeBox.AddText("does not exist")

    # Cosmetic #1
    title = input_file.replace("_"," ")
    title = title.replace("events ","Run #")
    #title = title.replace("katydid","Katydid")
    title = title.replace("merged","")
    title = title.replace(".root","")

    print('postprocessing: Opening root file {}'.format(input_filename))
    file = ROOT.TFile.Open(str(input_file),'r')
    print('postprocessing: Opening tree {}'.format(tree_name))

    # Loop that will generate empty plots if the tree does not exist
    can =  ROOT.TCanvas("data_quality","data_quality",600,400)
    if not file.GetListOfKeys().Contains(str(tree_name)):
        print('postprocessing: tree name given ({}) is not in root file: will create empty plots!'.format(tree_name))
        htemp = ROOT.TH1F("htemp","Temporary histo",100,0,1)
        print('postprocessing: Generating basic plots')
        for i,var in enumerate(plotted_var):
            htemp.SetTitle(title)
            htemp.GetXaxis().SetTitle(title_plotted_var[i])
            htemp.Draw()
            timestampBox.Draw()
            emptyTreeBox.Draw()
            filename = str(input_filename)+'_'+str(var).replace('.','_')+'.pdf'
            can.SetLogy(scale_plotted_var[i])
            if output_dir is None:
                can.SaveAs(filename)
                list_pfn_plots.append(filename)
            else:
                can.SaveAs(os.path.join(output_dir,filename))
                list_pfn_plots.append(os.path.join(output_dir,filename))
            can.SetLogy(0)
        print('postprocessing: Generating more complex plots')
        for i,var in enumerate(more_complex_plot):
            htemp.SetTitle(title)
            htemp.GetXaxis().SetTitle(more_complex_plot_title[i])
            htemp.Draw()
            timestampBox.Draw()
            emptyTreeBox.Draw()
            filename = str(input_filename)+'_'+str(var).replace('.','_')+'.pdf'
            # can.SetLogy(scale_plotted_var[i])
            if output_dir is None:
                can.SaveAs(filename)
                list_pfn_plots.append(filename)
            else:
                can.SaveAs(os.path.join(output_dir,filename))
                list_pfn_plots.append(os.path.join(output_dir,filename))
            can.SetLogy(0)
        return list_pfn_plots
    
    # Proper generation of the plots if the tree exists
    tree = file.Get(str(tree_name))
    print('postprocessing: Generating basic plots')
    for i,var in enumerate(plotted_var):
        print('postprocessing: Generating {} histogram'.format(str(var)))
        tree.Draw(str(var))
        htemp = ROOT.gPad.GetPrimitive("htemp")
        htemp.SetTitle(title)
        htemp.GetXaxis().SetTitle(title_plotted_var[i])
        htemp.Draw()
        timestampBox.Draw()

        filename = str(input_filename)+'_'+str(var).replace('.','_')+'.pdf'
        can.SetLogy(scale_plotted_var[i])
        if output_dir is None:
            can.SaveAs(filename)
            list_pfn_plots.append(filename)
        else:
            can.SaveAs(os.path.join(output_dir,filename))
            list_pfn_plots.append(os.path.join(output_dir,filename))
        can.SetLogy(0)

    print('postprocessing: Generating more complex plots')

    print('postprocessing: Generating {} histogram'.format("jump size"))
    jumpSize = []
    # variable for the jump length and cut jump size
    startTimeTrack = []
    endTimeTrack = []
    startFreqTrack = []
    endFreqTrack = []
    jumpLength = []
    jumpLengthClose = []
    jumpSizeBetweenEvents = []
    jumpSizeBetweenEventsCut = []
    for iEntry in range(tree.GetEntries()):
        tree.GetEntry(iEntry)
        startTimeTrack.append(tree.GetLeaf("fStartTimeInRunC").GetValue())
        endTimeTrack.append(tree.GetLeaf("fEndTimeInRunC").GetValue())
        startFreqTrack.append(tree.GetLeaf("fStartFrequency").GetValue())
        endFreqTrack.append(tree.GetLeaf("fEndFrequency").GetValue())
        for i in range(1,tree.GetLeaf("fTracks.fStartFrequency").GetLen()):
            jumpSize.append(-tree.GetLeaf("fTracks.fStartFrequency").GetValue(i)+tree.GetLeaf("fTracks.fEndFrequency").GetValue(i-1))
    for iValue in range(len(startTimeTrack)-1):
        jumpLength.append(+endTimeTrack[iValue]-startTimeTrack[iValue+1])
        jumpSizeBetweenEvents.append(-startFreqTrack[iValue+1]+endFreqTrack[iValue])
        if -endTimeTrack[iValue]+startTimeTrack[iValue+1]>-0.2 and -endTimeTrack[iValue]+startTimeTrack[iValue+1] <0.2:
            jumpLengthClose.append(endTimeTrack[iValue]-startTimeTrack[iValue+1])
            jumpSizeBetweenEventsCut.append(-startFreqTrack[iValue+1]+endFreqTrack[iValue])
    #return list_pfn_plots
    print('Exiting Plot Code')
    sys.exit(0)

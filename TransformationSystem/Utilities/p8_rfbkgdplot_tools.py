#! /usr/bin/env python

# =============================================================================
# Program: 	RF background plotting
# Version: 	Oct 2016
# Author:	M. Guigue
# =============================================================================

import xml.etree.ElementTree as ET

import ROOT as root
import numpy as np
import sys, getopt
import json

def setROOTStyle(rightMargin = 0.02,
                 leftMargin  = 0.09,
                 botMargin   = 0.11,
                 topMargin   = 0.06):

    style = root.TStyle(root.gStyle)
    style.SetOptStat(0)
    style.SetOptFit(0)
    style.SetStatY(1.-topMargin)
    style.SetStatX(1.-rightMargin)
    style.SetStatW(0.2)
    style.SetStatH(0.2)
    style.SetLabelSize(     0.05,'xy')
    style.SetTitleOffset(   1.1 ,'y')
    style.SetTitleSize(     0.05,'xy')
    style.SetPadRightMargin(rightMargin)
    style.SetPadTopMargin(topMargin)
    style.SetPadBottomMargin(botMargin)
    style.SetPadLeftMargin(leftMargin)
    style.cd()


def main(inputfile):
    #inputfile = ""
    outputfile = ""
    '''
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print('python python_file.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('python python_file.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    if inputfile is "":
        print("Provide an input filename using -i.")
        sys.exit(2)
    else:
        print("Input filename is {}.".format(inputfile))
    '''
    if outputfile is "":
        print("No output filename provider via -o. Using input file basename.")
        ##%% BAV - strip off the existing .dpt extension before forming the name
        outputfile = inputfile.replace('.dpt', '')
    print("Output filename is {}.pdf".format(outputfile))

    rightMargin = 0.03
    leftMargin  = 0.105
    botMargin   = 0.11
    topMargin   = 0.06
    setROOTStyle( rightMargin,leftMargin, botMargin, topMargin)

    x=[]
    data =[]
    print('Reading data file {}'.format(inputfile))
    xmltree = ET.parse(inputfile)
    xmlroot = xmltree.getroot()
    xml_waveform = xmlroot.find('Internal').find('Composite').find('Items').find('Composite').find('Items').find('Waveform')
    
    xmin = np.double(xml_waveform.find('XStart').text)/1e6
    xmax = np.double(xml_waveform.find('XStop').text)/1e6
    nbpoints = np.int(xml_waveform.find('Count').text)
    for j in range(0,nbpoints):
        x.append(xmin + (xmax-xmin)/(nbpoints-1)*j)
    #data = np.double(xml_obj['RSAPersist']['Internal']['Composite']['Items']['Composite']['Items']['Waveform']['y'])
    data_txt = []
    for element in xml_waveform.iter('y'):
        data_txt.append(element.text)
    data = np.double(data_txt)


    print("Creating the graph.")
    can = root.TCanvas("can","can",200,10,1200,800)
    plotRange=[xmin,xmax]
    graph = root.TGraph(len(x))
    for i in range(len(x)):
        graph.SetPoint(i,x[i],data[i])

    print("Making the graph look nice.")
    graph.SetTitle(inputfile)
    graph.GetXaxis().SetLimits(plotRange[0], plotRange[1])
    #graph.SetMinimum(-66)
    #graph.SetMaximum(-59)
    ##%% BAV - Compute the ranges dynamically (there is no static range appropriate for all of our data)
    graph.SetMinimum(np.floor(np.amin(data)))
    graph.SetMaximum(np.ceil(np.amax(data)))
    graph.Draw('AP')
    graph.SetMarkerColor(2)
    graph.SetMarkerStyle(20) # comment if the points look too big
    graph.SetLineColor(2)
    graph.SetLineWidth(2)
    graph.GetXaxis().SetTitle("Frequency [MHz]")
    graph.GetYaxis().SetTitle("Power [dB]")

    print("Saving the graph.")
    can.SaveAs("{}.pdf".format(outputfile))
    #can.SaveAs("{}.png".format(outputfile))


def execute():
    with open('plot_config.txt') as json_file:
        data = json.load(json_file)
    print(data['input_file'])
    main(data['input_file'])

import ROOT
import datetime
import os
import json
from array import array
from numpy import mean, std

class plotter:

    def __init__(self, inputFile=None, treeName=None, outputDir=None):
        self.tree=None
        self.rootFile=None

        self.rightMargin = 0.06
        self.leftMargin = 0.09
        self.botMargin = 0.11
        self.topMargin = 0.06

        self.SetStyle()

        self.__histogramVariables = {}
        self.outputDir = outputDir

        self.timestampBox = ROOT.TPaveText(0.001, 0.001, 0.36, 0.06, "BRNDC")
        self.timestampBox.SetTextColor(1)
        self.timestampBox.SetFillColor(0)
        self.timestampBox.SetTextAlign(12)
        self.timestampBox.SetTextSize(0.035)
        self.timestampBox.SetBorderSize(1)
        self.timestampBox.AddText(datetime.datetime.now().strftime("%a, %d. %b %Y %I:%M:%S%p UTC"))

        # ROOT.TColor.InvertPalette()
        if inputFile is not None:
            if isinstance(inputFile,list):
                inputFile= inputFile[0]
                self.title = os.path.splitext(inputFile)[0]
                self.title = self.title.replace("_"," ")
                self.title = self.title.replace("events ","Run #")
                self.title = self.title.replace("katydid","Katydid")
                self.title = self.title.replace("concat","")
            self.inputFilename = os.path.basename(os.path.splitext(inputFile)[0])
        else:
            self.inputFilename = "postproc"
            self.title = "postproc"

        if treeName is not None:
            self.rootFile = ROOT.TFile.Open(inputFile)
            self.tree = self.rootFile.Get(treeName)

        if outputDir is not None:
            self.outputDir = outputDir
        else:
            self.outputDir = os.getcwd()


    def __del__(self):
        if self.tree is not None:
            self.rootFile.Close()

    def SetStyle(self, statsBoxStyle="em"):
        style = ROOT.TStyle(ROOT.gStyle)
        style.SetOptStat(statsBoxStyle)
        style.SetStatY(1.-self.topMargin)
        style.SetStatX(1.-self.rightMargin)
        style.SetStatW(0.2)
        style.SetStatH(0.2)

        style.SetLabelOffset(0,'xy')
        style.SetLabelSize(0.035,'xy')

        style.SetTitleOffset(1.04,'y')
        style.SetTitleSize(0.035,'y')
        style.SetLabelSize(0.035,'y')
        style.SetLabelOffset(0,'y')

        style.SetTitleSize(0.035,'x')
        style.SetLabelSize(0.035,'x')
        style.SetTitleOffset(1.00,'x')

        style.SetPadRightMargin(self.rightMargin)
        style.SetPadTopMargin(self.topMargin)
        style.SetPadBottomMargin(self.botMargin)
        style.SetPadLeftMargin(self.leftMargin)
        style.SetPalette(112) # Viridis colormap

        ROOT.TGaxis.SetExponentOffset(0.0, -0.055, "x")

        style.cd()

    def SetVariable(self,variableName, variableValue):
        self.__histogramVariables[variableName] = variableValue

    def GetVariable(self, variableName):
        if variableName not in self.__histogramVariables and self.tree is not None:
            self.AddLeaf(variableName)
        return self.__histogramVariables[variableName]

    def GetStructuredArray(self, leafName):
        leafArray = []
        nEntries = self.tree.GetEntries()
        for i in range(0, nEntries):
            self.tree.GetEntry(i)
            tmpArray = []
            for j in range(0,self.tree.GetLeaf(leafName).GetLen()):
                tmpArray.append(self.tree.GetLeaf(leafName).GetValue(j))
            leafArray.append(tmpArray)
        return leafArray

    def AddLeaf(self,leafName):
        leafArray = []
        nEntries = self.tree.GetEntries()
        for i in range(0, nEntries):
            self.tree.GetEntry(i)
            for j in range(0,self.tree.GetLeaf(leafName).GetLen()):
                leafArray.append(self.tree.GetLeaf(leafName).GetValue(j))
        self.SetVariable(leafName,leafArray)


    def MakeHist1D(self,varName, nBins=100, xRange=None):
        varArray = self.GetVariable(varName)
        if not xRange:
            xRange = self.AutoRange(varArray)

        hNew = ROOT.TH1D(varName, varName, nBins, xRange[0], xRange[1])
        for varValue in varArray:
            if varValue >=xRange[0] and varValue<= xRange[1]:
                hNew.Fill(varValue)

        return hNew


    def DrawHistogram1D(self, varName, xTitle = "", nBins = 100, xRange = None, logy = False, fit = None, statsBoxStyle ="em"):

        self.rightMargin = 0.06
        self.SetStyle(statsBoxStyle)

        can =  ROOT.TCanvas("data_quality","data_quality",600,400)
        can.SetLogy(bool(logy))
        filename = ""
        hNew = []

        if not isinstance(varName, list):
            hNew = self.MakeHist1D(varName, nBins, xRange)
            hNew.SetTitle(self.title)
            hNew.GetXaxis().SetTitle(xTitle)
            hNew.GetXaxis().CenterTitle()
            hNew.Draw()
            filename = self.inputFilename + '_' + varName.replace('.','_') + '.pdf'
        else:
            hNew = []
            filename = self.inputFilename + '_'
            leg = ROOT.TLegend(1-self.rightMargin-0.2,1-self.topMargin-0.15,1-self.rightMargin,1-self.topMargin)

            for i in range(len(varName)):
                var = varName[i]
                hNew.append(self.MakeHist1D(var, nBins, xRange))
                hNew[-1].SetTitle(self.title)
                hNew[-1].GetXaxis().CenterTitle()
                hNew[-1].GetXaxis().SetTitle(xTitle)
                hNew[-1].SetLineColor(i+1)

                if i==0:
                    hNew[-1].Draw("")
                else:
                    filename += "+"
                    hNew[-1].Draw("same")

                filename += var.replace('.','_')
                leg.AddEntry(hNew[-1],var,'l')


            filename += ".pdf"

            if fit:
                resultsFitBox = ROOT.TPaveText(1-self.rightMargin-0.2, 1-self.topMargin-0.3, 1-self.rightMargin, 1-self.topMargin-0.15, "BRNDC")
                for fitFunc in fit:
                    fitFunc.Draw("sameL")
                    leg.AddEntry(fitFunc,fitFunc.GetName(),'l')
                    resultsFitBox = self.AppendFitBox(resultsFitBox,fitFunc)
                resultsFitBox.Draw()

            leg.Draw()

        self.timestampBox.Draw()
        self.WriteToPDF(can, filename)

    def AppendFitBox(self,resultsFitBox, fitFunc):
        for parameterIndex in range(fitFunc.GetNpar()):
            resultsFitBox.AddText(fitFunc.GetParName(parameterIndex) + ' = {}#pm{}'.format(round(fitFunc.GetParameter(parameterIndex),2),round(fitFunc.GetParError(parameterIndex),2)))
            resultsFitBox.SetFillColor(0)
            resultsFitBox.SetTextAlign(12)
            resultsFitBox.SetBorderSize(1)
            (resultsFitBox.GetListOfLines().Last()).SetTextColor(fitFunc.GetLineColor());

        return resultsFitBox


    def AutoRange(self,variableList):
        if not variableList:
            return [0,1]
        elif len(variableList) == 1:
            return [variableList[0] - 0.5, variableList[0] + 0.5]
        else:
            return [min(variableList), max(variableList) + 1e-14 * abs(max(variableList))]


    def MakeHist2D(self,varNameX, varNameY, nBinsX = 100, nBinsY = 100, xRange = None, yRange = None):
        varArrayX = self.GetVariable(varNameX)
        varArrayY = self.GetVariable(varNameY)

        if not xRange:
            xRange = self.AutoRange(varArrayX)
        if not yRange:
            yRange = self.AutoRange(varArrayY)

        hNew = ROOT.TH2D("htemp", "htemp", nBinsX, xRange[0], xRange[1], nBinsY, yRange[0], yRange[1])
        for i in range(len(varArrayX)):
            hNew.Fill(varArrayX[i], varArrayY[i])

        return hNew

    def DrawHistogram2D(self, varNameX, varNameY, xTitle = "", yTitle = "", nBinsX = 100, nBinsY = 100, xRange = None, yRange = None, logx=False, logy = False, logz=True):
        self.rightMargin = 0.1
        self.SetStyle()

        hNew = self.MakeHist2D(varNameX, varNameY, nBinsX, nBinsY, xRange, yRange)
        can =  ROOT.TCanvas("data_quality","data_quality",600,400)

        can.SetLogx(bool(logx))
        can.SetLogy(bool(logy))
        can.SetLogz(bool(logz))

        #Format
        hNew.SetTitle(self.title)
        hNew.GetXaxis().SetTitle(xTitle)
        hNew.GetYaxis().SetTitle(yTitle)
        hNew.GetXaxis().CenterTitle()
        hNew.GetYaxis().CenterTitle()
        hNew.SetMinimum(1.0)
        hNew.Draw("colz")
        self.timestampBox.Draw()

        filename = self.inputFilename + '_' + varNameX.replace('.','_') + '_' + varNameY.replace('.','_') + '.pdf'
        self.WriteToPDF(can, filename)

    def DrawGraph(self,varNameX,varNameY,xTitle="",yTitle=""):
        can =  ROOT.TCanvas("data_quality","data_quality",600,400)
        if not isinstance(varNameX, list):
            varArrayX = array('f',self.GetVariable(varNameX))
            varArrayY = array('f',self.GetVariable(varNameY))
            nEntries = len(varArrayX)
            g = ROOT.TGraph(nEntries,varArrayX,varArrayY)
            g.Draw("AC")
            g.SetTitle(self.title)
            g.GetXaxis().SetTitle(xTitle)
            g.GetYaxis().SetTitle(yTitle)
            g.GetXaxis().CenterTitle()
            filename = self.inputFilename + '_' +varNameY.replace('.','_') + '.pdf'
        else:
            all_graphs = []
            leg = ROOT.TLegend(1-self.rightMargin-0.2,1-self.topMargin-0.15,1-self.rightMargin,1-self.topMargin)
            for varX, varY in zip(varNameX,varNameY):
                varArrayX = array('f',self.GetVariable(varX))
                varArrayY = array('f',self.GetVariable(varY))
                nEntries = len(varArrayX)
                all_graphs.append(ROOT.TGraph(nEntries, varArrayX, varArrayY))
                leg.AddEntry(all_graphs[-1],varY,'l')

            maxVal = max([ max(self.GetVariable(varYList)) for varYList in varNameY])
            minVal = min([ min(self.GetVariable(varYList)) for varYList in varNameY])

            all_graphs[0].Draw("AC")
            all_graphs[0].SetTitle(self.title)
            all_graphs[0].GetXaxis().SetTitle(xTitle)
            all_graphs[0].GetYaxis().SetTitle(yTitle)
            all_graphs[0].GetXaxis().CenterTitle()
            all_graphs[0].GetYaxis().CenterTitle()
            all_graphs[0].SetMaximum(maxVal + 0.02*abs(maxVal))
            all_graphs[0].SetMinimum(minVal - 0.02*abs(maxVal))
            leg.Draw()
            filename = self.inputFilename

            for i in range(1,len(all_graphs)):
                all_graphs[i].SetLineColor(i+1)
                all_graphs[i].Draw("C")

            for y in varNameY:
                filename+='_'+y.replace('.','_')
            filename+='.pdf'

            self.timestampBox.Draw()
        self.WriteToPDF(can, filename)

    def WriteToPDF(self, canvas, filename):
        if self.outputDir is None:
            canvas.SaveAs(filename)
            #list_pfn_plots.append(filename)
        else:
            canvas.SaveAs(os.path.join(self.outputDir,filename))
            #list_pfn_plots.append(os.path.join(self.outputDir,filename))

    def notify_slack(self):
        eventStartFreqs = self.GetVariable("fStartFrequency")
        trackTimeLengths = self.GetVariable("fTracks.fTimeLength")
        trackSlopes = self.GetVariable("fTracks.fSlope")
        tracksPerEv = mean(self.GetVariable("fTotalEventSequences"))
        nEvents = len(eventStartFreqs)
        tLength = mean(trackTimeLengths)
        startFreq = [mean(eventStartFreqs), std(eventStartFreqs)]
        tSlope = [mean(trackSlopes), std(trackSlopes)]
        message = "*Finished processing: " + str(self.inputFilename) + "*\n"
        message += "Number of Events: " +str(nEvents) + "\n"
        message += "Average Track Length [ms]: " + str("{:.3f}".format(tLength*1e3)) + "\n"
        message += "Start Frequencies [MHz]: " + str("{:.3f} ± {:.3f}".format(startFreq[0]/1e6, startFreq[1]/1e6)) + "\n"
        message += "Track Slopes [MHz/s]: " + str("{:.3f} ± {:.3f}".format(tSlope[0]/1e6, tSlope[1]/1e6)) + "\n"
        message += "Tracks per Event: " + str("{:.3f}".format(tracksPerEv))
        messageDictionary = {"text" : message}

        #Converts single quotes in dictionary to double quotes
        messageDictionary = json.dumps(messageDictionary)

        #Write to dirac_notices
        #webhook = "https://hooks.slack.com/services/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" #### #slack_test
        webhook = "https://hooks.slack.com/services/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" #### #dirac_notices

        commandString = "curl -X POST -H 'Content-type: application/json' --data '" + str(messageDictionary)+ "' " + webhook
        #Send the message
        os.system(commandString)
        print("Slack notification\n{}".format(message))

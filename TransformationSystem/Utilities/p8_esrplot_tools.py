#! /usr/bin/env python
# =============================================================================
# Program: 	ESR analysis script
# Version: 	March 2017
# Author:	W. Pettus
# =============================================================================

import os, sys
import json
import numpy as np
from ROOT import gROOT, gStyle, TCanvas, TF1, TFile, TGraphErrors, TMultiGraph

# Analysis globals
sweep_range = 10.0 # voltage range for "SWEEP_OUT" from ardbeg
f_cyclotron = 1.758820024e11 # rad s^-1 T^-1
g_factor = 2.0026 # g-factor for BDPA (from literature)
f_rescale = 1e-6 # frequency looks better in MHz
field_factor = 4.*np.pi / (g_factor*f_cyclotron*f_rescale) # frequency to field conversion factor

####def analyze_run(filepath, shape='gaussian', n_fits=2):
def analyze_run(input_filename, shape='gaussian', n_fits=2):
    '''
    Perform analysis on ESR raw data file:
    - Load JSON saved data
    - Convert raw outputs to processed units
    - Calculate crossing frequency via Wiener optimal filter
    - Fit trace with ROOT Minuit
    - Save output to JSON and ROOT
    '''
    # Argument checks
    ####if not os.path.isdir(filepath):
    ####    raise IOError("analyze_run takes path argument, invalid directory path")
    if not os.path.exists(input_filename):
        raise IOError("analyze_run: input_filename argument {} does not exist".format(input_filename))
    if shape not in ('gaussian','lorentzian'):
        raise ValueError("analyze_run unknown shape '{}', only trained for 'gaussian' or 'lorentzian'".format(shape))
    try:
        n_fits = int(n_fits)
    except ValueError:
        raise ValueError("analyze_run invalid n_fits '{}', requires integer".format(n_fits))

    # IO wrangling
    ####if filepath.count('raw') != 1:
    ####    raise IOError("Uncontrolled path; expect single instance of 'raw'.")
    ####if filepath.endswith('/'):
    ####    filepath = filepath.rstrip('/')
    ####tstamp = filepath.split('/')[-1]
    ####infile = open(filepath+'/{}-esr.json'.format(tstamp))
    infile = open(input_filename)
    raw = json.load(infile)
    ####outpath = filepath.replace('raw', 'proc')
    ####os.makedirs(outpath)
    ####outbase = outpath + '/' + tstamp
    outbase = input_filename.rstrip('-esr.json')

    # Load sweeper frequency range from header
    freq = [float(raw['header']['sweeper']['hf_start_freq']),
            float(raw['header']['sweeper']['hf_stop_freq'])]

    result = {}
    traces = {}
    # Load data arrays
    for c in range(1,6):
        key = 'coil{}'.format(c)
        if key in raw:
            data, fspan = format_input(raw[key]['data'], freq)
            filtered = wiener_filter(data, shape)
            fitted = root_fit(data, n_fits, fspan, shape)
            result.update( { key : { 'filt' : filtered['result']*field_factor,
                                     'filt_e' : filtered['error']*field_factor,
                                     'fit' : fitted.pop('result')*field_factor,
                                     'fit_e' : fitted.pop('error')*field_factor } } )
            traces.update( { key : fitted } )

    save_data(outbase, result, traces)


def format_input(raw, freq):
    '''
    Method to format single coil data for analysis:
    - Scale sweeper out into MHz frequency
    - Scale up x/y voltages into nice units (*1e6)
    - Sort by frequency (but calculate span for fit error)
    '''
    proc = {}
    for key in raw.keys():
        proc[key] = np.array(raw[key].split(';'), dtype=float)
    proc['freq'] = (freq[0] + (freq[1]-freq[0])*proc.pop('adc')/sweep_range) * f_rescale
    fspan = proc['freq'][-1] - proc['freq'][0] + (freq[1]-freq[0])*f_rescale
    combined = np.column_stack((proc['freq'], proc['x']*1e6, proc['y']*1e6))
    combined = combined.ravel().view([('f','float'), ('x','float'), ('y','float')])
    combined.sort()

    return combined, fspan


def save_data(basename, result, traces):
    '''
    Dump results into 'proc' JSON file
    Generate ROOT plots and save to ROOT and graphic format
    '''
    jsonfile = open(basename+'-result.json', 'w')
    json.dump(obj=result, fp=jsonfile, indent=4)
    jsonfile.close()

    root_setup()
    rootfile = TFile(basename+'-plots.root', 'recreate')
    field_plot(result, rootfile, basename)
    esr_trace_plots(traces, rootfile, basename)
    rootfile.Close()


def root_setup():
    '''
    Method to configure ROOT canvas formatting.
    '''
    gROOT.Reset()
    gROOT.SetBatch()
    gStyle.SetOptStat     (0)
    gStyle.SetOptFit      (0)
    gStyle.SetTitleSize   (0.045,'xy')
    gStyle.SetTitleOffset (0.8,  'xy')
    gStyle.SetPadTickY    (1)
    gStyle.SetPadTickX    (1)


def wiener_filter(data, shape):
    '''
    Apply Wiener filter to data to crossing frequency
    '''
    freq = data['f']
    volt = data['x'] + 1j*data['y']
    print("doing filter on target: {}".format(shape))
    width = (freq[np.argmin(volt)] - freq[np.argmax(volt)]) / 2.
    x1 = (freq - freq[0])
    x2 = (freq - freq[-1])
    if shape == 'gaussian':
        deriv1 = -x1 * np.exp(-x1**2 / 2. / width**2) * np.exp(0.5) / width
        deriv2 = -x2 * np.exp(-x2**2 / 2. / width**2) * np.exp(0.5) / width
    elif shape == 'lorentzian':
        deriv1 = -x1 / (x1**2 + (width * 3.**0.5)**2)**2 * 16. * width**3
        deriv2 = -x2 / (x2**2 + (width * 3.**0.5)**2)**2 * 16. * width**3
    else:
        raise ValueError("Unknown shape {}, only trained for 'gaussian' or 'lorentzian'".format(shape))
    target_signal = np.concatenate((deriv1[:int(len(deriv1)/2)], deriv2[int(len(deriv2)/2):]))
    if sum(target_signal) == 0:
        raise ValueError("target signal identically 0, wiener_filter calculation broken")
    data_fft = np.fft.fft(volt)
    data_fft[0] = 0
    target_fft = np.fft.fft(target_signal)
    filtered = np.abs( np.fft.ifft(data_fft * target_fft) )

    # Lightly-tuned data-quality checks:
    fom1 = ( np.max(filtered) - np.mean(filtered) ) / np.std(filtered)
    #fom2 = abs(fit_field - b_field) / (fit_field_e**2 + b_field_e**2)**0.5
    if fom1 < 2.5:
        res_freq = 0
        res_freq_e = 0
        print("Rejecting Wiener filter result with figure-of-merit = {}".format(fom1))
    else:
        index = np.argmax( np.abs(filtered) )
        res_freq = freq[index]
        res_freq_e = max(freq[index]-freq[index-1],
                         freq[index+1]-freq[index])

    return { 'result': res_freq,
             'error': res_freq_e,
             'target': target_signal,
             'filtered': filtered }


def root_fit(data, fits, span, shape):
    '''
    Use ROOT fitting with Minuit to determine crossing frequency
    '''
    # Calculate quick seed values
    p1 = np.argmin(data['x'])
    p2 = np.argmax(data['x'])
    s = (data['f'][p2] - data['f'][p1]) / 2.
    b = (data['f'][p2] + data['f'][p1]) / 2.
    a = (data['x'][p2] - data['x'][p1]) / 2.
    if shape == 'gaussian':
        fit = TF1('fit','((x-[1])*gaus(0)+[3])*(x>0)\
                   +(-[4]*(x+[1])*exp(-(x+[1])**2/2./[2]**2)+[5])*(x<0)')
        a *= np.exp(0.5) / s
    elif shape == 'lorentzian':
        fit = TF1('fit','([0]*(x-[1])/(3*[2]**2+(x-[1])**2)**2+[3])*(x>0)\
                       +(-[4]*(x+[1])/(3*[2]**2+(x+[1])**2)**2+[5])*(x<0)')
        a *= 16 * s**3
    else:
        raise ValueError("Unknown shape {}, only trained for 'gaussian' or 'lorentzian'".format(shape))
    fit.SetParameters(a,b,s,np.mean(data['x']),a/5,np.mean(data['y']))
    fit.SetLineColor(4)

    f2 = np.concatenate((-data['f'][::-1], data['f']))
    xy = np.concatenate((data['y'][::-1], data['x']))
    fe = np.array(len(data['f']) * [span / (len(data['f']) - 1) / 6.], dtype=float)
    f2e = np.concatenate((fe, fe))
    if b < (data['f'][0] + data['f'][-1]) / 2.:
        xe = np.array(len(data['x']) * [np.std(data['x'][-50:])])
        ye = np.array(len(data['y']) * [np.std(data['y'][-50:])])
    else:
        xe = np.array(len(data['x']) * [np.std(data['x'][:50])])
        ye = np.array(len(data['y']) * [np.std(data['y'][:50])])
    xye = np.concatenate((ye, xe))

    scale = 1
    for ct in range(fits):
        xe = xe * scale
        ye = ye * scale
        xye = xye * scale
        plot1 = TGraphErrors(len(f2), f2, xy, f2e, xye)
        plot1.Fit('fit','ME')
        scale = (fit.GetChisquare() / fit.GetNDF())**0.5
        print("Chi-Square : {} / {}; rescale error by {}".format(fit.GetChisquare(), fit.GetNDF(), scale))
        if scale > 0.95 and scale < 1.05:
            print("Acceptable error reached after fit #{}, aborting iterative scale and fit".format(ct+1))
            break

    plot1.SetName('xy_f')
    plot2 = TGraphErrors(len(data['f']), data['f']*1., data['y']*1., fe, ye)
    plot2.SetName('y_f')
    plot2.SetLineColor(2)
    if shape == 'gaussian':
        fit2 = TF1('fit2','((x-[1])*gaus(0)+[3])*(x>0)',data['f'][0],data['f'][-1])
    elif shape == 'lorentzian':
        fit2 = TF1('fit2','([0]*(x-[1])/(3*[2]**2+(x-[1])**2)**2+[3])*(x>0)',data['f'][0],data['f'][-1])
    fit2.SetParameters(fit.GetParameter(4), fit.GetParameter(1), fit.GetParameter(2), fit.GetParameter(5))
    fit2.SetLineColor(3)

    ymax = max(np.max(data['x']), np.max(data['y']))
    ymin = min(np.min(data['x']), np.min(data['y']))
    yrng = ymax - ymin
    ymax += yrng/8.
    ymin -= yrng/8.
    plot2.GetYaxis().SetRangeUser(ymin, ymax)

    res_freq = fit.GetParameter(1)
    res_freq_e = fit.GetParError(1)
    if res_freq < data['f'][0] or res_freq > data['f'][-1]:
        print("Rejecting fit result with out-of-range resonant frequency = {} MHz".format(res_freq))
        res_freq = 0
        res_freq_e = 0
    # FIXME: better check of fit failure?

    return { 'graph_xy' : plot1,
             'graph_y' : plot2,
             'fit' : fit,
             'fit_y': fit2,
             'result' : res_freq,
             'error' : res_freq_e }


def esr_trace_plots(traces, rootfile, basename):
    '''
    Generate and save output plots of ESR traces.
    '''
    for coil in sorted(traces):
        num = coil.strip('coil')
        can = TCanvas('can{}'.format(num), '{}'.format(coil))
        traces[coil]['graph_y'].SetTitle('Coil {};Frequency (MHz);Amplitude (arb)'.format(num))
        traces[coil]['graph_y'].Draw('AL')
        traces[coil]['fit_y'].Draw('same')
        traces[coil]['graph_xy'].Draw('L')
        can.Write()
        can.SaveAs(basename+'-{}.pdf'.format(coil))


def field_plot(result, rootfile, basename):
    '''
    Generate and save output plot of B-field values for all coils.
    '''
    filt = { coil : result[coil] for coil in result if (result[coil]['filt']!=0) }
    fit = { coil : result[coil] for coil in result if (result[coil]['fit']!=0) }
    if len(filt)==0 and len(fit)==0:
        print("No valid ESR measurements, skipping field_plot")
        return

    x1 = np.array([coil.strip('coil') for coil in sorted(filt)], dtype=float)
    x1e = np.zeros(len(x1), dtype=float)
    x2 = np.array([coil.strip('coil') for coil in sorted(fit)], dtype=float)
    x2e = np.zeros(len(x2), dtype=float)
    y1 = np.array([filt[coil]['filt'] for coil in sorted(filt)], dtype=float)
    y1e = np.array([filt[coil]['filt_e'] for coil in sorted(filt)], dtype=float)
    y2 = np.array([fit[coil]['fit'] for coil in sorted(fit)], dtype=float)
    y2e = np.array([fit[coil]['fit_e'] for coil in sorted(fit)], dtype=float)

    can = TCanvas('can0', 'field')
    mg = TMultiGraph()
    if len(x1) != 0:
        g_filt = TGraphErrors(len(x1), x1, y1, x1e, y1e)
        mg.Add(g_filt)
    if len(x2) != 0:
        g_fit = TGraphErrors(len(x2), x2, y2, x2e, y2e)
        g_fit.SetLineColor(2)
        mg.Add(g_fit)
    mg.SetTitle('Field Map;ESR Coil Position;Field (T)')
    mg.Draw('AL')
    can.Write()
    can.SaveAs(basename+'-fieldmap.pdf')


def execute():
    with open('plot_config.txt') as json_file:
        data = json.load(json_file)
    print(data['input_file'])
    analyze_run(data['input_file'])

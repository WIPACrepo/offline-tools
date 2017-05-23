

from __future__ import division, print_function
from os import uname

from os.path import expandvars
import glob, sys, tables, icecube, pylab, scipy.stats, matplotlib, time, scipy.optimize, math,pickle
from I3Tray import*
from icecube import icetray, dataclasses, dataio, hdfwriter, tableio, phys_services,  photonics_service, lilliput
#from icecube import simclasses,WaveCalibrator, wavedeform, gulliver, gulliver_modules, paraboloid, MuonInjector, MuonGun
#from icecube import simprod, TopologicalSplitter
import matplotlib as mpl
matplotlib.use('Agg')
from matplotlib.colors import LogNorm
import numpy as np
import matplotlib.pyplot as plt

#from icecube import weighting
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import rc,gridspec
import matplotlib.cbook as cbook
from scipy.optimize import fsolve
#from icecube.weighting import fluxes, WeightCalculator
import scipy.optimize as optimization 
load("libmue")
load("bayesian-priors")

font = {'family': 'serif',
                    'serif':  'Computer Modern Roman',
                    'weight': 200,
                    'size':   30}

matplotlib.rc('font', **font)
class MyColors():
    def __init__(self):
        self.colors = ['#000000','#1C1C1C','#424242','#6E6E6E','#A4A4A4','#D8D8D8','#E6E6E6','#F2F2F2','#FAFAFA','#FFFFFF',
                       '#4B82B8',
                        '#B8474D',
                        '#95BB58',
                        '#234B7C',
                        '#8060A9',
                        '#53A2CB',
                        '#FC943B']

    def __getitem__(self, item):
        return self.colors[int(item % 7)]

plt.rc('text', usetex=True)

mycolorsI = ['#000000','#1C1C1C','#424242','#6E6E6E','#A4A4A4','#D8D8D8','#E6E6E6','#F2F2F2','#FAFAFA','#FFFFFF',
            '#4B82B8','#B8474D','#95BB58','#234B7C','#8060A9','#53A2CB','#FC943B']

mycolorsII = ['#000000','#1C1C1C','#424242','#6E6E6E','#A4A4A4','#D8D8D8','#E6E6E6','#F2F2F2','#FAFAFA','#FFFFFF',
            '#00a0c6','#a9deec','#006077','#64a5b5','#f69331','#53A2CB','#FC943B']

mycolorsIII = ['#c70039','#ff5733','#ff8d1a','#ffc300','#eddd53','#add45c','#57c785',
               '#00baad','#2a7b9b','#3d3d6b','#511849','#900c3f','#900c3f'] #0,6 or 0,4,8

colors2 = ['#525564','#74828F', '#96C0CE', '#BEB9B5', '#C25B56', '#FEF6EB']

def make_cmap(colors, position=None, bit=False):
    import matplotlib as mpl
    import numpy as np
    bit_rgb = np.linspace(0,1,256)
    if position == None:
        position = np.linspace(0,1,len(colors))
    else:
        if len(position) != len(colors):
            sys.exit("position length must be the same as colors")
        elif position[0] != 0 or position[-1] != 1:
            sys.exit("position must start with 0 and end with 1")
    if bit:
        for i in range(len(colors)):
            colors[i] = (bit_rgb[colors[i][0]],
                         bit_rgb[colors[i][1]],
                         bit_rgb[colors[i][2]])
    cdict = {'red':[], 'green':[], 'blue':[]}
    for pos, color in zip(position, colors):
        cdict['red'].append((pos, color[0], color[0]))
        cdict['green'].append((pos, color[1], color[1]))
        cdict['blue'].append((pos, color[2], color[2]))

    cmap = mpl.colors.LinearSegmentedColormap('my_colormap',cdict,256)
    return cmap

print(uname())

class chargePlot():
    def __init__(self, x1,x2,livetime,
                 x1_label = 'NuFSGen2011',x2_label = 'NuFSGen2012',title ='2015',
                 nbins = 100, alpha = 0.9,xlabel = r'E$^{proxy}_{\mu}$ [GeV]',loc = 1, run_id = 'unknown'):
        
        fig = pylab.figure(figsize=(10,5)) 
        ax = fig.add_subplot(111)
        
        xmin = 0
        xmax = 200
        ymin = 0
        ymax = 50
        alpha = 1

        x1l = x1_label
        x1_color = mycolorsIII[2]
        n1,binEdges = np.histogram(x1,bins = nbins)
        bincenters = 0.5*(binEdges[1:]+ binEdges[:-1])
        pylab.hist(x1,weights = np.ones(len(x1))*1./livetime,label=x1l,bins=nbins,histtype='step', linewidth=2, range=[xmin, xmax],color=x1_color)

        x2l = x2_label
        x2_color = mycolorsIII[8]
        n1,binEdges = np.histogram(x2,bins = nbins)
        bincenters = 0.5*(binEdges[1:]+ binEdges[:-1])
        pylab.hist(x2,weights = np.ones(len(x2))*1./livetime,label=x2l,bins=nbins,histtype='step', linewidth=2, range=[xmin, xmax],color=x2_color)

        plt.title(title,color="black")
        pylab.yscale('linear', nonposy='clip')
        pylab.xscale('linear', nonposx='clip')
        pylab.grid(b=True, which='major', color='black', alpha = 0.3,linestyle='-')
        pylab.grid(b=True, which='minor', color='black',alpha = .1,  linestyle='-')
        pylab.ylabel('Rate [Hz]')
        pylab.xlabel(r'Total Charge [p.e.]')
        pylab.axis([xmin, xmax, ymin,ymax])
        leg = pylab.legend(fontsize=18,loc=1,fancybox = True)
        #pylab.show()
        pylab.savefig('/home/joertlin/public_html/tmp/pass2/Run_{run_id}.svg'.format(run_id = run_id))


storage = '/data/user/joertlin/tmp/charges'

# Config
run_id = 118359
season = 2011
livetime = 28801.1250762939
# Config end

charges_p1 = []
counter = 0
for f in glob.glob(os.path.join(storage, 'Level2_*{run_id}*.i3.bz2'.format(run_id = run_id))):
    print('Read {0}'.format(f))
    infile = dataio.I3File(f)

    while(infile.more()):
        frame = infile.pop_physics()
        if frame is not None:
            charges_p1.append(frame['TotalCharge_split'].value)

print(len(charges_p1))

charges_p2 = []
counter = 0
for f in glob.glob(os.path.join(storage, 'Level2pass2_*{run_id}*.i3.bz2'.format(run_id = run_id))):
    print('Read {0}'.format(f))
    infile = dataio.I3File(f)
    while(infile.more()):
        frame = infile.pop_physics()
        if frame is not None:
            charges_p2.append(frame['TotalCharge_split'].value)

print(len(charges_p2))
print('done...')
c = chargePlot(x1 = charges_p1 , x2 = charges_p2, livetime = livetime, title = 'Level2 Run {run_id}'.format(run_id = run_id),
               x1_label = 'IC86.{season} Pass1'.format(season = season), x2_label = 'IC86.{season} Pass2'.format(season = season), run_id = run_id)


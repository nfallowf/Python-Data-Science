import matplotlib.pyplot as plt
from matplotlib import style
import numpy as np
from scipy import stats

style.use('ggplot')  

# Plot closing price
def plotData(data):
    data.plot()
    plt.show()
    plt.close()
    
# Plot closing price with Rolling Average
def plotRA(data, RA):
    data.plot()
    RA.plot()
    plt.show()
    plt.close()

# Plot closing price with volume traded
def plotVolume(data):
    ax1 = plt.subplot2grid((6,1), (0,0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6,1), (5,0), rowspan=1, colspan=1, sharex=ax1)
    ax1.plot(data.index, data['Close'])
    ax2.bar(data.index, data['Volume'])
    plt.show()
    plt.close()
    

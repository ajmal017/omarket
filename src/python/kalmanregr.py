from pandas.io.data import DataReader
secs = ['EWA', 'EWC']
data = DataReader(secs, 'yahoo', '2010-1-1', '2014-8-1')['Adj Close']
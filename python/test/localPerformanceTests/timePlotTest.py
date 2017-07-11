import cdms2, datetime, matplotlib
import matplotlib.pyplot as plt
opendap_url = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-R_historical_r2i1p1/ta_Amon_GISS-E2-R_historical_r2i1p1_197601-200012.nc"
varName = "ta"
dset = cdms2.open( opendap_url )
tas = dset[varName]
timeSeries = tas[:,8,45,100]
list_of_datetimes = [datetime.datetime(x.year, x.month, x.day, x.hour, x.minute, int(x.second)) for x in tas.getTime().asComponentTime()]
dates = matplotlib.dates.date2num(list_of_datetimes)
plt.plot_date(dates, timeSeries.data )
plt.gcf().autofmt_xdate()
plt.show()
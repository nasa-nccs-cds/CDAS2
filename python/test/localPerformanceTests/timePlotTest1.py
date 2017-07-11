import cdms2
import plotly.offline as py
import plotly.graph_objs as go
import pandas as pd

opendap_url = "http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-R_historical_r2i1p1/ta_Amon_GISS-E2-R_historical_r2i1p1_197601-200012.nc"
varName = "ta"

dset = cdms2.open( opendap_url )
tas = dset[varName]
timeSeries = tas[:,8,45,100]
# Convert to Celsius
timeSeries -= 273.15

datetimes = pd.to_datetime(timeSeries.getTime().asdatetime())

data = [go.Scatter(x=datetimes, y=timeSeries)]

print(py.plot(data, output_type='file', filename='testTimeSeries.html', auto_open=False))

# sns.tsplot(data=timeSeries, time=Series(datetimes))
# plt.show()

# timeAxis = tas.getTime()
# data = timeSeries.data
# list_of_datetimes = [datetime.datetime(x.year, x.month, x.day, x.hour, x.minute, int(x.second)) for x in timeAxis.asComponentTime()]
# dates = matplotlib.dates.date2num(list_of_datetimes)
#
# plt.plot_date(dates, data)
# plt.gcf().autofmt_xdate()
# plt.show()

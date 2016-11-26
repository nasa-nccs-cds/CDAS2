import cdms2

gridFilePath = '/Users/thomas/.cdas/cache/collections/NCML/giss_r1i1p1.nc'
gridfile = cdms2.open( gridFilePath )
dimensions = ['time', 'lat', 'lon']
axes = [ gridfile.axes.get(dim) for dim in dimensions ]
shape = [1, 90, 144]
origin = [0, 0, 0]
subAxes = []

for index in range( len(axes) ):
    start = origin[index]
    length = shape[index]
    axis = axes[index]
    print " >> subset Axis: start: {0}, length: {1}, axis: {2}".format( str(start), str(length), axis.listall() )
    subAxes.append( axis.subAxis( start, start + length ) )
    print " >> Added Axis: index: {0}".format( str(index) )


print subAxes[0].listall()

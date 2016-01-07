# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 19:05:40 2015

@author: danielparks
"""

#pylint:disable-msg=E1101
#pylint:disable-msg=C0103,C0111

import numpy as np
import googlemaps
import datetime as dt
import sqlite3
import json
import time

API_KEY = None
DB_PATH = None

class Database(object):
    """ Take the information from Google Maps and stuff it into a database """

    fields = ('time', 'latitude_from', 'longitude_from', 'latitude_to',
              'longitude_to', 'transit_response', 'driving_response')
    dtypes = ('real', 'real', 'real', 'real', 'real', 'text', 'text')

    def __init__(self, path, buffer_size=100):
        # turn on the database. if the path doesnt exist, that means the table
        # needs to be created

        # create commands
        tmp_f = ', '.join(['%s %s'%(f, dt) for f, dt in zip(self.fields, self.dtypes)])
        create_cmd = "create table google_responses (%s)"%tmp_f
        self.insert_cmd = "insert into google_responses values (%s)"%(', '.join(['?']*len(self.fields)))

        from os.path import isfile
        create = not isfile(path)

        self.conn = sqlite3.connect(path)
        self.curs = self.conn.cursor()

        # create table if necessary
        if create:
            self.curs.execute(create_cmd)

        self.buffer_size = buffer_size
        self.data_buffer = []

    def record(self, data):

        # check the data. should be: datetime, float, float, float, float, list, list
        for idx, dtype in enumerate((dt.datetime, float, float, float, float, list, list)):
            if not isinstance(data[idx], dtype):
                print('failed!')
                return None

        # convert directions to json for storage
        data = list(data)
        for idx, func in zip((0, -1, -2), (dt.datetime.timestamp, json.dumps, json.dumps)):
            data[idx] = func(data[idx])

        # append data to data_buffer
        self.data_buffer.append(tuple(data))

        # if the data_buffer has reached a certain length, dump the data into
        # the database and clear the buffer. sqlite3 is optimized for transactions
        # so it makes sense to wait a while before making a write in order to
        # amortize the cost of disk access
        if len(self.data_buffer) >= self.buffer_size:
            self.curs.executemany(self.insert_cmd, self.data_buffer)
            self.conn.commit()
            self.data_buffer = []

class GoogleDirectionGetter(object):
    """ Talk to Google Maps API """

    def __init__(self, key=None):
        self.cxn = googlemaps.Client(key=key)

    def get(self, origin, finish):
        # get transit and driving distances and times for moving from point
        # origin to point finish.
        print('(%.4f, %.4f):(%.4f, %.4f)'%(origin+finish))

        n = dt.datetime.now()+dt.timedelta(0, 0, 0, 0, 5) # five minutes into the future
        transit = self.cxn.directions(origin, finish, mode='transit', departure_time=n)
        driving = self.cxn.directions(origin, finish, mode='driving', departure_time=n)

        return n, origin[0], origin[1], finish[0], finish[1], transit, driving

class PolygonSampler(object):
    """ Implements ray-tracing method to determine if a randomly drawn
    (x, y) point is inside a polygon defined by a set of vertices. """

    def __init__(self):
        self.x1 = None
        self.x2 = None
        self.y1 = None
        self.y2 = None
        self.loaded = False

        self.min_x = None
        self.max_x = None
        self.min_y = None
        self.max_y = None

    def load_points(self, xvals, yvals):

        assert isinstance(xvals, np.ndarray)
        assert isinstance(yvals, np.ndarray)
        assert xvals.shape == yvals.shape
        assert xvals.ndim == 1

        # save for later
        self.xvals = xvals
        self.yvals = yvals
        self.rolled_x = np.ascontiguousarray(np.roll(xvals, 1))
        self.rolled_y = np.ascontiguousarray(np.roll(yvals, 1))

        # min/max
        self.min_x = xvals.min()
        self.max_x = xvals.max()
        self.min_y = yvals.min()
        self.max_y = yvals.max()

        self.loaded = True

    def points_from_kml(self, coords_string):
        # input the coordinates string from the google earth kml
        # workflow:
        # 1. trace the region of interest using the polygon tool in google earth
        # 2. save the polygon as a KML file
        # 3. open the file and find the string of coordinates
        # 4. send the coordinates as a string to this function

        x_vals = []
        y_vals = []
        for coord in coords_string.split(',0'):
            if len(coord) > 0:
                coord2 = coord.split(',')
                x_vals.append(float(coord2[0]))
                y_vals.append(float(coord2[1]))

        self.load_points(np.array(y_vals[:-1]), np.array(x_vals[:-1]))

    def test_point(self, xval, yval):
        # implement ray-casting algorithm for in/out determination
        # research faster algorithms using numba etc.
        # but: this is not slow, and making it faster wouldnt matter much
        # dhp laptop with ~60 points gives ~75us max per test.
        # repeated trials give faster results because something is cached

        bool1 = (self.yvals < yval) & (self.rolled_y >= yval)
        bool2 = (self.rolled_y < yval) & (self.yvals >= yval)
        bool3 = (self.xvals <= xval) | (self.rolled_x <= xval)
        bool4 = self.xvals+(yval-self.yvals)/(self.rolled_y-self.yvals)*(self.rolled_x-self.xvals) < xval
        return ((bool1 | bool2) & bool3 & bool4).sum()%2

    def sample(self):

        def _get(a, b):
            return (b-a)*np.random.random_sample()+a

        assert self.loaded

        # repeatedly sample at random until a point is  found inside the polygon.
        # the returned tuple can be put into google maps
        while True:
            point_x = _get(self.min_x, self.max_x)
            point_y = _get(self.min_y, self.max_y)
            if self.test_point(point_x, point_y):
                return point_x, point_y
                
def parse_directions(resp):

    t = resp[0]['legs'][0]['duration']['value']

    # total distances by mode. distances in meters.
    # total times by model. times in seconds
    distances = {}
    times = {}
    for step in resp[0]['legs'][0]['steps']:
        m = step['travel_mode']
        distances[m] = distances.get(m, 0)+step['distance']['value']
        times[m] = times.get(m, 0)+step['duration']['value']

    return t, distances, times                


# this is the outline of san francisco done by hand in google earth
# and then expored to a .kml file
x = '-122.4948912015918,37.68037723229899,0 -122.4805506215812,37.68622226072776,0 -122.4557732911681,37.69343450074676,0 -122.4419508077932,37.6949623484931,0 -122.4286291255407,37.69643741544487,0 -122.4164506011284,37.68622901413689,0 -122.4028891264216,37.68515860592969,0 -122.3916333321391,37.68936429992507,0 -122.394145810898,37.70792724501131,0 -122.3867643746725,37.71096726617559,0 -122.3841907917646,37.70907902212533,0 -122.3798896168044,37.71083808912604,0 -122.3767312153647,37.71591218196993,0 -122.3829141559536,37.72108906152666,0 -122.377393066373,37.72443555477281,0 -122.375364928958,37.72075881660353,0 -122.3654580386897,37.71686873628337,0 -122.3619574822368,37.72009613219431,0 -122.358260223896,37.72667500071492,0 -122.3596254667212,37.72988708279716,0 -122.3745937785922,37.7332764093118,0 -122.3747268248764,37.73711086493225,0 -122.3677773130189,37.73985410383761,0 -122.3762534782357,37.74884523139018,0 -122.3774653240321,37.75165384241259,0 -122.3832907858756,37.75348080122858,0 -122.3815036229569,37.75537619774887,0 -122.3816495715224,37.75965576597044,0 -122.3872619095997,37.76321115334789,0 -122.3855475205623,37.76875022571834,0 -122.3867895507054,37.77208580382067,0 -122.3874245106858,37.77837546689884,0 -122.3869625140816,37.78748727543376,0 -122.388501696234,37.7904728077707,0 -122.3997816993457,37.80255526011727,0 -122.4046311011295,37.80677438996746,0 -122.4119158147208,37.80883506524336,0 -122.420631058821,37.80818696198431,0 -122.4246069482805,37.80605373042566,0 -122.4281597958584,37.80826057265295,0 -122.4335616778315,37.80543422778879,0 -122.4433952886292,37.80764798703868,0 -122.4626134799103,37.80450582500688,0 -122.46782742431,37.80640074119365,0 -122.4735978218506,37.80864926431968,0 -122.4782160297504,37.80746699704775,0 -122.4787833986568,37.80368532742074,0 -122.4852740731228,37.79030198106968,0 -122.4896165151773,37.78944295039132,0 -122.4934943711236,37.78704076122546,0 -122.4993537904587,37.78801158092645,0 -122.5053049028735,37.78753142090459,0 -122.5130871546928,37.78141100877379,0 -122.5135887966352,37.77664199099782,0 -122.5126617417181,37.76386202181646,0 -122.5101027125875,37.75380800042708,0 -122.5078685437284,37.73928202617464,0 -122.5064116085885,37.72706191544106,0 -122.5030398776291,37.71006807720432,0 -122.4992989084347,37.6971395534814,0 -122.4948912015918,37.68037723229899,0'
ps = PolygonSampler()
ps.points_from_kml(x)
gdg = GoogleDirectionGetter(key=API_KEY)
gdb = Database(DB_PATH, buffer_size=10)

for n in range(30):
    gdb.record(gdg.get(ps.sample(), ps.sample()))
    time.sleep(.5)

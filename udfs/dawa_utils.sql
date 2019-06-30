

CREATE OR REPLACE PYTHON3 SCALAR SCRIPT ETL_UDFS.DAWA_UTILS()
Returns INT AS

import sys
import glob
import datetime

sys.path.extend(
    glob.glob('/buckets/bfsdefault/default/dawa-sdk-0.0.2/dawa-sdk-0.0.2')
)

from dawa import API

def parse_datetime(d):
    try:
        if d.endswith('Z'):
            d = d[:-1]
        d = datetime.datetime.strptime(d,'%Y-%m-%dT%H:%M:%S.%f')

    except (TypeError, ValueError):
        pass

    return d

def parse_point(obj):
    point = None

    if obj is not None:
        point = 'POINT(%s)' % ' '.join(str(e) for e in obj['coordinates'])

    return point

def parse_polygon(obj):
    polygon = None


    if obj is not None:
        points = []
        for point in obj['coordinates']:
            for axe in point:
                points.append(' '.join(str(e) for e in axe))

        polygon = 'POLYGON((%s))' % ', '.join(points)

    return polygon

def parse_multipolygon(obj):
    multipolygon = None

    if obj is not None:
        if 'coordinates' in obj:
            pols = []
            for polygons in obj['coordinates']:
                pol = []
                for polygon in polygons:
                    pol_points = []
                    for points in polygon:
                        pol_points.append(' '.join(str(e) for e in points))
                    pol.append('(%s)' % ', '.join(pol_points))
                pols.append('(%s)' % ', '.join(pol))
            multipolygon = 'MULTIPOLYGON(%s)' % ', '.join(pols)
    return multipolygon

def get(endpoint, til, fra):

    api = API()

    txidtil = None
    txidfra = None
    if til is not None:
        txidtil = str(til)

    if fra is not None:
        txidfra = str(fra)

    return api.get(endpoint, txidfra=txidfra, txidtil=txidtil)

def run(ctx):
    pass

/
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import URIRef, Literal
from shapely import wkt
from shapely.geometry import shape
from geopy.distance import geodesic

GEOF_SFCONTAINS = URIRef(
    "http://www.opengis.net/def/function/geosparql/sfContains"
)
GEOF_DISTANCE = URIRef("http://www.opengis.net/def/function/geosparql/distance")
GEOF_WITHIN = URIRef("http://www.opengis.net/def/function/geosparql/sfWithin")
GEOF_INTESECT = URIRef("http://www.opengis.net/def/function/geosparql/sfIntersects")
GEOF_OVERLAPS = URIRef("http://www.opengis.net/def/function/geosparql/sfOverlaps")
GEOF_CROSSES = URIRef("http://www.opengis.net/def/function/geosparql/sfCrosses")

import json
from shapely.geometry import shape
from shapely import wkt
from rdflib import Literal

"""
PyRCC8 1.2.2
"""

def geof_distance(geom1, geom2):
    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)
    
    p1 = g1.centroid
    p2 = g2.centroid
    

    coord1 = (p1.y, p1.x)
    coord2 = (p2.y, p2.x)
    
    distancia = geodesic(coord1, coord2).meters
    
    return Literal(distancia)

def getBbox(g):
    minx, miny, maxx, maxy = g.bounds
    return f"{minx},{miny},{maxx},{maxy}"

def parse_geom(g):

    g = g.toPython() if type(g) is not str else g

    if isinstance(g, dict):   # GeoJSON dict
        return shape(g)

    if isinstance(g, str):

        g = g.strip()

        if g.startswith("{"):   # GeoJSON string
            return shape(json.loads(g))

        else:                   # WKT
            return wkt.loads(g)

    raise ValueError("Unknown geometry format")


def geof_sfContains(geom1, geom2):

    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)

    return Literal(g1.contains(g2))

def geof_within(geom1, geom2):

    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)

    return Literal(g1.within(g2))   

def geof_intersects(geom1, geom2):

    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)

    return Literal(g1.intersects(g2))

def geof_overlaps(geom1, geom2):

    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)

    return Literal(g1.overlaps(g2))

def geof_crosses(geom1, geom2):

    g1 = parse_geom(geom1)
    g2 = parse_geom(geom2)

    return Literal(g1.crosses(g2))
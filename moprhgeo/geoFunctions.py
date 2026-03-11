from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import URIRef, Literal
from shapely import wkt
from shapely.geometry import shape

GEOF_SFCONTAINS = URIRef(
    "http://www.opengis.net/def/function/geosparql/sfContains"
)

import json
from shapely.geometry import shape
from shapely import wkt
from rdflib import Literal

def parse_geom(g):

    g = g.toPython()

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
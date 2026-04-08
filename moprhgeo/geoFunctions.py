from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import URIRef, Literal
from shapely import wkt
from shapely.geometry import shape
from geopy.distance import geodesic

GEOF_SFCONTAINS = URIRef(
    "http://www.opengis.net/def/function/geosparql/sfContains"
)
GEOF_DISTANCE = URIRef("http://www.opengis.net/def/function/geosparql/sfDistance")
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

def getBbox(geoms: list):
    if not geoms:
        return None
    fgeom = geoms[0]
    minx, miny, maxx, maxy = get_offset_bounds(fgeom)

    for g in geoms[1:]:
        g_minx, g_miny, g_maxx, g_maxy = get_offset_bounds(g)

        minx = max(minx, g_minx)
        miny = max(miny, g_miny)
        maxx = min(maxx, g_maxx)
        maxy = min(maxy, g_maxy)

        if minx > maxx or miny > maxy:
            return None  # o "" si prefieres string vacío

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

import math
def meters_to_degrees(dist_meters, lat_reference):
    """
    Convierte una distancia en metros a desplazamientos en grados (offset_x, offset_y).
    lat_reference: Latitud en la que se encuentra la geometría (en grados decimales).
    """
    # Constante aproximada del radio de la Tierra en metros (WGS84)
    METERS_PER_DEGREE_LAT = 111111.0

    # 1. El offset en Latitud (Y) es constante
    offset_y = dist_meters / METERS_PER_DEGREE_LAT

    # 2. El offset en Longitud (X) depende de la latitud (se estrecha hacia los polos)
    # Convertimos la latitud a radianes para la función coseno
    cos_lat = math.cos(math.radians(lat_reference))
    
    # Evitamos división por cero en los polos
    if abs(cos_lat) < 1e-10:
        offset_x = 0
    else:
        offset_x = dist_meters / (METERS_PER_DEGREE_LAT * cos_lat)

    return offset_x, offset_y

def get_offset_bounds(g_tuple):
    geom_raw, dist_m = g_tuple
    dist_m = float(dist_m) if dist_m else 0
    # Obtenemos los límites originales en grados
    b_minx, b_miny, b_maxx, b_maxy = parse_geom(geom_raw).bounds
    
    if dist_m == 0:
        return (b_minx, b_miny, b_maxx, b_maxy)

    # Calculamos la latitud media para ajustar la deformación del eje X
    lat_media = (b_miny + b_maxy) / 2
    
    # Obtenemos los offsets en grados
    dx, dy = meters_to_degrees(dist_m, lat_media)

    # Aplicamos el margen (Buffer)
    return (b_minx - dx, b_miny - dy, b_maxx + dx, b_maxy + dy)
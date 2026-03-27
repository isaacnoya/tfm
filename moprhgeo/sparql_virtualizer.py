from rdflib.plugins.sparql import CUSTOM_EVALS
from rdflib.plugins.sparql.sparql import FrozenBindings, QueryContext
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import Variable
from typing import Generator

from virtual import getStarShapedSubqueries, candidateMappingSelection, materializeVirtualMappingGroup, getMappingsFromBGP, evalVirtualBGP, getMappingGroups, orderTriplesStatic
from mappings import getMappingsFromTxT
from classes import TriplePattern, MappingContext
from geoFunctions import GEOF_SFCONTAINS, geof_sfContains, GEOF_GETBBOX, getBBox, GEOF_DISTANCE, geof_distance



mappings = getMappingsFromTxT("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/moprhgeo/mappings.txt")

"""
!!! hacer un selectNextTriplePattern? para evaluar dinamicamente la tripleta a evaluar?
!!! igual no deberia de eliminar el mappingGroup despues de ejecutarlo, sencillamente tener una lista con las consultas ya ejecutadas
!!! si el objeto del tp es una variable y el mapping tiene filterx, se le mete en la uri como ?nombre={variable(o)} o algo asi, y para cada binding que entre se sustituye y se ejecuta.


!!! Estudiar que forma es mejor de agrupar mappnigns
!!! Me compensa hacer muchas consultas filtradas por nombre cada una? o mejor la consulta general y ya 

--- Operaciones geo en local son costosas, igual si que es mejor delegar en la API, el problema es que no todas las APIs permitern filtros espaciales
--- Lo que si que soportan son bboxes, entonces puedo ir filtrando la consulta igual


+++ Solucionar termCompatibility, sobre todo para diferenciar templates de sujeto(deberian de ser subclase de URIRef) y referencias de objeto


El contains se puede hacer por defecto con el bbox= que es rapido y todas las APIs lo tienen implementado
"""

def virtual_bgp_eval(ctx: QueryContext, part) -> Generator[FrozenBindings, None, None]:
    if part.name != "BGP":
        raise NotImplementedError()
    
    tps = []
    for s, p, o in part.triples:
        tp = TriplePattern(s, p, o)
        tps.append(tp)

    subqueries = getStarShapedSubqueries(tps)

    for suj, tps in subqueries.items():
        mappings_for_subq = candidateMappingSelection(tps, mappings)

        ctx = materializeVirtualMappingGroup(
            mappings_for_subq,
            ctx
        )

    triples = sorted(
        part.triples, key=lambda t: len([n for n in t if ctx[n] is None])
    )
    return rdflib.plugins.sparql.evaluate.evalBGP(ctx, triples)

def virtual_bgp_eval2(ctx: QueryContext, part) -> Generator[FrozenBindings, None, None]:
    if part.name != "BGP":
        raise NotImplementedError()
    
    tps = []
    for s, p, o in part.triples:
        tp = TriplePattern(s, p, o)
        tps.append(tp)

    subqueries = getStarShapedSubqueries(tps)

    for suj, tps in subqueries.items():
        mappings_for_subq = set()
        ctxMapping = MappingContext()
        for m in getMappingsFromBGP(ctxMapping, tps, mappings):
            mappings_for_subq.add(m)
        
        ctx = materializeVirtualMappingGroup(
            mappings_for_subq,
            ctx
        )

    triples = sorted(
        part.triples, key=lambda t: len([n for n in t if ctx[n] is None])
    )
    return rdflib.plugins.sparql.evaluate.evalBGP(ctx, triples)

from collections import defaultdict
def virtual_bgp_eval3(ctx: QueryContext, part) -> Generator[FrozenBindings, None, None]:
    if part.name != "BGP":
        raise NotImplementedError()
    
    tps = []
    """
    triples = sorted(
        part.triples, key=lambda t: len([n for n in t if ctx[n] is None])
    )
    """
    triples = orderTriplesStatic(ctx, part.triples)
    
    for s, p, o in triples:
        tp = TriplePattern(s, p, o)
        tps.append(tp)

    mappingsBGP = set()
    ctxMapping = MappingContext()
    for m in getMappingsFromBGP(ctxMapping, tps, mappings):
        mappingsBGP.add(m)    

    mappingGroups = getMappingGroups(mappingsBGP)
    # TODO: optimizeMappingGroups() -> Si 2 grupos de mappings tienen el mismo merged source, igual hay que unificarlos
    
    trigers = defaultdict(lambda: None)
    queriesMade = set()
    return evalVirtualBGP(ctx, tps, mappingGroups, trigers, queriesMade)

def virtual_filter(ctx, part):
    return None


CUSTOM_EVALS["virtual_bgp"] = virtual_bgp_eval3
register_custom_function(GEOF_SFCONTAINS, geof_sfContains)
register_custom_function(GEOF_GETBBOX, getBBox)
register_custom_function(GEOF_DISTANCE, geof_distance)

import rdflib
g = rdflib.Graph()


query = """
PREFIX ogc: <http://www.ogc.org/>
PREFIX ine: <https://stats.linkeddata.es/voc/cubes/obs/>
PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX ex: <http://example.org/function/>

SELECT ?y WHERE {
    ?x a ogc:railwaystationnode ;
        ogc:nombre "Apartadero de Padrón Barbanza" ;
        ogc:use ?use .
    ?y a ogc:railwaystationnode ;
        ogc:use ?use .
}
"""

qres = g.query(query)
for r in qres:
    print(r)
    pass
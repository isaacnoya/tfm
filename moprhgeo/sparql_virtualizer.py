from rdflib.plugins.sparql import CUSTOM_EVALS
from rdflib.plugins.sparql.sparql import FrozenBindings, QueryContext
from rdflib.plugins.sparql.evaluate import evalPart
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import Variable, Namespace
from typing import Generator

from rdflib.plugins.sparql.evalutils import (
    _ebv,
    _eval,
    _fillTemplate,
    _join,
    _minus,
    _val,
)

from virtual import getStarShapedSubqueries, candidateMappingSelection, materializeVirtualMappingGroup, getMappingsFromBGP, evalVirtualBGP, getMappingGroups, orderTriplesStatic
from mappings import getMappingsFromTxT
from classes import TriplePattern, MappingContext, geoBindings
from geoFunctions import GEOF_SFCONTAINS, geof_sfContains, GEOF_DISTANCE, geof_distance, GEOF_WITHIN, geof_within, GEOF_INTESECT, geof_intersects, GEOF_OVERLAPS, geof_overlaps, GEOF_CROSSES, geof_crosses

EX = Namespace("http://example.com/")


mappings = getMappingsFromTxT("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/moprhgeo/mappings.txt")

"""
#TODO:  
    -Implementar parentTriplesMap (aqui voy a tener que modificar el materializaGroup porque no contemplo que el objeto sea un Template)
    +-Mejorar como accedo a los filtros, porque estoy haciendo directamente part.expr.expr pero es poco robusto, en caso de que haya un sfWithin() && sfWithin() esto rompe.

+++ Query-Specific Pruning of RML Mappings:
    -Puedo implementar el prunning al principio? ns si servira de algo, porque despues ya hago el select mapping
    -En todo caso, la definición de incompatibilidad me la quedo

+++ en QueriesMade, tambien tener en cuenta si se ha hecho la misma consulta pero con un bbox que contenga plenamente al bbox a consultar.
+++ hacer un selectNextTriplePattern? para evaluar dinamicamente la tripleta a evaluar?
+++ Solucionar termCompatibility, sobre todo para diferenciar templates de sujeto(deberian de ser subclase de URIRef) y referencias de objeto
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

def virtualGeoFilter(ctx: QueryContext, part) -> Generator[FrozenBindings, None, None]:
    if part.name != "Filter":
        raise NotImplementedError()
    
    if part.expr.iri == GEOF_SFCONTAINS or part.expr.iri == GEOF_WITHIN:
        container, contained = part.expr.expr if part.expr.iri == GEOF_SFCONTAINS else (part.expr.expr[1], part.expr.expr[0])

        if type(contained) is Variable and type(container) is rdflib.term.Literal:
            geoBindings[contained].append(container)
        if type(contained) is Variable and type(container) is Variable:
            geoBindings[contained].append(container)
    elif part.expr.iri == GEOF_INTESECT or part.expr.iri == GEOF_OVERLAPS or part.expr.iri == GEOF_CROSSES:
        geom1, geom2 = part.expr.expr
        if type(geom1) is Variable and type(geom2) is rdflib.term.Literal:
            geoBindings[geom1].append(geom2)
        if type(geom2) is Variable and type(geom1) is rdflib.term.Literal:
            geoBindings[geom2].append(geom1)
        if type(geom2) is Variable and type(geom1) is Variable:
            geoBindings[geom2].append(geom1)
            geoBindings[geom1].append(geom2)
    elif part.expr.expr.iri == GEOF_DISTANCE and (part.expr.op == '<' or part.expr.op == '=' or part.expr.op == '<='):
        geom1, geom2 = part.expr.expr.expr
        distance = part.expr.other
        if type(geom1) is Variable and type(geom2) is rdflib.term.Literal:
            geoBindings[geom1].append(geom2+ ":-:" +str(distance))
        if type(geom2) is Variable and type(geom1) is rdflib.term.Literal:
            geoBindings[geom2].append(geom1+ ":-:" +str(distance))
        if type(geom2) is Variable and type(geom1) is Variable:
            geoBindings[geom2].append(geom1+ ":-:" +str(distance))
            geoBindings[geom1].append(geom2+ ":-:" +str(distance))


    def _auxGen(ctx, part):
        for c in evalPart(ctx, part.p):
            if _ebv(
                part.expr,
                c.forget(ctx, _except=part._vars) if not part.no_isolated_scope else c,
            ):
                yield c

    return _auxGen(ctx, part)


CUSTOM_EVALS["virtual_bgp"] = virtual_bgp_eval3
CUSTOM_EVALS["virtualGeofilter"] = virtualGeoFilter
register_custom_function(GEOF_SFCONTAINS, geof_sfContains)
register_custom_function(GEOF_DISTANCE, geof_distance)
register_custom_function(GEOF_WITHIN, geof_within)
register_custom_function(GEOF_INTESECT, geof_intersects)
register_custom_function(GEOF_OVERLAPS, geof_overlaps)
register_custom_function(GEOF_CROSSES, geof_crosses)

import rdflib
g = rdflib.Graph()


query = """
PREFIX ogc: <http://www.ogc.org/>
PREFIX ine: <https://lod.ine.es/voc/cubes/vocabulary#>
PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX ex: <http://example.org/function/>
PREFIX qb: <http://purl.org/linked-data/cube#>

SELECT DISTINCT ?y ?gy ?s ?gs WHERE {
    ?s a ogc:administrativeunit ;
       ogc:nameunit "Santiago de Compostela" ;
       geo:hasGeometry ?gs .
    ?y a ogc:watercourselinksequence ;
        geo:hasGeometry ?gy .
    FILTER (geof:sfWithin(?gy, ?gs))
}
"""

qres = g.query(query)
for r in qres:
    print(r)
    pass
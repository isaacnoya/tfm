from rdflib.plugins.sparql import CUSTOM_EVALS
from rdflib.plugins.sparql.sparql import FrozenBindings, QueryContext
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import Variable
from typing import Generator

from virtual import getStarShapedSubqueries, candidateMappingSelection, materializeVirtualMappingGroupCTX
from mappings import getMappingsFromTxT
from classes import TriplePattern
from geoFunctions import GEOF_SFCONTAINS, geof_sfContains



mappings = getMappingsFromTxT("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/moprhgeo/mappings.txt")


def virtual_bgp_eval(ctx: QueryContext, part) -> Generator[FrozenBindings, None, None]:
    if part.name != "BGP":
        raise NotImplementedError()
    
    ctx = ctx.pushGraph(rdflib.Graph())
    tps = []
    for s, p, o in part.triples:
        tp = TriplePattern(s, p, o)
        tps.append(tp)

    subqueries = getStarShapedSubqueries(tps)

    for suj, tps in subqueries.items():
        mappings_for_subq = candidateMappingSelection(tps, mappings)

        ctx = materializeVirtualMappingGroupCTX(
            mappings_for_subq,
            ctx
        )

    triples = sorted(
        part.triples, key=lambda t: len([n for n in t if ctx[n] is None])
    )
    return rdflib.plugins.sparql.evaluate.evalBGP(ctx, triples)

CUSTOM_EVALS["virtual_bgp"] = virtual_bgp_eval
register_custom_function(GEOF_SFCONTAINS, geof_sfContains)

import rdflib
g = rdflib.Graph()

query = """
PREFIX ogc: <http://www.ogc.org/>
PREFIX ine: <https://stats.linkeddata.es/voc/cubes/obs/>
PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

SELECT ?a WHERE {
    ?r a ogc:railwaystationnode ;
        ogc:nombre "Estación de A Coruña                                                                                " ;
        geo:hasGeometry ?geom1 .
    ?a a ogc:agua:estado_masas_aguasub ;
        geo:hasGeometry ?geom2 .
    FILTER ( geof:sfContains(?geom2, ?geom1) )
}
"""

qres = g.query(query)
for r in qres:
    print(r)
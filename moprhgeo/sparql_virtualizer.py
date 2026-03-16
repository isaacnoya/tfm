from rdflib.plugins.sparql import CUSTOM_EVALS
from rdflib.plugins.sparql.sparql import FrozenBindings, QueryContext
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import Variable
from typing import Generator

from virtual import getStarShapedSubqueries, candidateMappingSelection, materializeVirtualMappingGroup
from mappings import getMappingsFromTxT
from classes import TriplePattern
from geoFunctions import GEOF_SFCONTAINS, geof_sfContains, GEOF_GETBBOX, getBBox, GEOF_DISTANCE, geof_distance



mappings = getMappingsFromTxT("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/moprhgeo/mappings.txt")

"""
SELECT ?r ?p ?o WHERE {
    ?r a ogc:railwaystationnode ;
        ?p "Estación de A Coruña                                                                                " .
}
no se boundea por sujeto, si ya se que la variable tiene que ser de una clase, deberia de obviar el resto de mappings que matchean, en caso de que sean subclase y tal, deberia de haber un rewritting de la query
esto no lo tira bien, la segunda tripleta deberia de estar boundeada, pero no
Voy a tener que ir triple pattern uno a uno? Deberia de acumular consultas en un stack y ejecutar al final del starsahped subquery?

el materialize virtual mappings mio tendria que ser el evalBGP, entonces tendria que sacar el getVirtualMappingsGroups y meterlo antes tambien.
    Entender como va lo del push del ctx para poder guardar toda la informacion necesaria bien.

Tengo que implementar lo de los bindings, en mi propio evalBGP, porque las optimizaciones de filtro son sobre literales que me vienen en la consulta, no de los que consigo en consultas anteriores,
tengo que guardar en el contexto los bindings de las variables, de forma que pueda hacer filtros en las consultas

El contains se puede hacer por defecto con el bbox= que es rapido y todas las APIs lo tienen implementado
"""

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

        ctx = materializeVirtualMappingGroup(
            mappings_for_subq,
            ctx
        )

    triples = sorted(
        part.triples, key=lambda t: len([n for n in t if ctx[n] is None])
    )
    return rdflib.plugins.sparql.evaluate.evalBGP(ctx, triples)

CUSTOM_EVALS["virtual_bgp"] = virtual_bgp_eval
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

SELECT ?y ?dist WHERE {
    ?x a ogc:railwaystationnode ;
        geo:hasGeometry ?geom1 ;
        ogc:nombre "Apartadero de Padrón Barbanza" .
    ?y a ogc:railwaylink ;
        geo:hasGeometry ?geom2 .
    BIND(geof:distance(?geom1, ?geom2) AS ?dist)
    FILTER(?dist < 10000) # Lugares a menos de 5km
  }
"""

qres = g.query(query)
for r in qres:
    print(r)
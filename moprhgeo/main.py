import pandas as pd
import rdflib


from rdflib.plugins.sparql import prepareQuery
from tabulate import tabulate
from classes import *
from virtual import getStarShapedSubqueries, candidateMappingSelection, materializeVirtualMappingGroup
from mappings import getMappingsFromTxT

"""
---- NOTAS ----
Funciona pero el nombre de los literales tiene que cuadrar 100%

Falta tratar bien los parentTriple joins
Falta meter ahora los bindings

se deberia de meter el project siempre (el properties=) ??

"""


sparql_query = """
PREFIX geo: <http://www.opengis.net/ont/geosparql#> 
PREFIX ogc: <http://www.ogc.org/> 
SELECT ?x ?y WHERE {
    ?x a ogc:railwaystationnode .
    ?x ogc:nombre "Estación de A Coruña" .
} 
"""

"""
https://wmts.mapama.gob.es/sig-api/ogc/features/v1/collections/agua:masas_aguasub_2027/items?f=json&filter=s_contains(shape,%20POINT(-2.514604067437437%2042.84881824371775))
"""

sparql_queryx = """
PREFIX geo: <http://www.opengis.net/ont/geosparql#> 
PREFIX ogc: <http://www.ogc.org/> 
PREFIX sdmx-measure: <http://purl.org/linked-data/sdmx/2009/measure#>
PREFIX qb: <http://purl.org/linked-data/cube#> 
PREFIX ine: <https://stats.linkeddata.es/voc/cubes/obs/>
SELECT ?x ?z WHERE {
    ?x a ogc:railwaystationnode . 
    ?x ogc:nombre "Estación de A Coruña                                                                                " .
    ine:1.6_200301 sdmx-measure:obsValue ?o  .
} 
"""

query = prepareQuery(sparql_query, initNs={"geo": "http://www.opengis.net/ont/geosparql#", "ogc": "http://www.ogc.org/", "sdmx-measure": "http://purl.org/linked-data/sdmx/2009/measure#"})

algebra = query.algebra


tps= []

for t in algebra.p.p.triples:
    tc = TriplePattern(*t)
    tps.append(tc)

subqueries = getStarShapedSubqueries(tps)

mappings = getMappingsFromTxT("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/moprhgeo/mappings.txt")

lista_it = []
bindings = []

for suj, tps in subqueries.items():
    mappings_for_subq = candidateMappingSelection(tps, mappings)
    l, bindings = materializeVirtualMappingGroup(mappings_for_subq, bindings, tps)
    lista_it.append(l)

df_final = pd.concat(lista_it, ignore_index=True)
print(tabulate(df_final, headers='keys', tablefmt='psql', showindex=False))
df_final.to_csv('xd.csv', index=False, encoding='utf-8')
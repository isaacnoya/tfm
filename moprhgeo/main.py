import pandas as pd
import rdflib
from rdflib import Graph, URIRef, BNode, Literal, Namespace, Variable

from pathlib import Path
from jsonpath import JSONPath
from io import StringIO

import requests

from rdflib.plugins.sparql import prepareQuery
from utils import *
from tabulate import tabulate
import copy
from classes import *

"""
---- NOTAS ----
Mirarse https://github.com/RDFLib/rdflib-hdt/blob/master/rdflib_hdt/sparql_op.py

Falta meter ahora los bindings

se deberia de meter el project siempre (el properties=) ??
"""

def getMappings(mapping_file):
    mappings = rdflib.Graph()
    mappings = rdf_class_to_pom(mappings.parse(mapping_file, format="turtle"))

    # select all mapping rules with the form (url, iterator, subject, predicate, object)
    mappingRuleQuery = prepareQuery("""
    PREFIX rml: <http://w3id.org/rml/>
    PREFIX htv: <http://www.w3.org/2011/http#>
    PREFIX void: <http://rdfs.org/ns/void#> 

    SELECT ?subjectTemplate ?predicate ?object ?reference ?url ?iterator ?nextPage ?filterx ?projectx WHERE {
        ?tm a rml:TriplesMap ;
            rml:logicalSource ?ls ;
            rml:predicateObjectMap ?pom .
        ?ls rml:source ?source ;
            rml:iterator ?iterator ;
            void:nextPage ?nextPage .
        ?source htv:absoluteURI ?url .
        ?pom rml:predicate ?predicate .
        ?tm rml:subjectMap ?sm .
        ?sm rml:template ?subjectTemplate .
        OPTIONAL { ?pom rml:objectMap ?om .        
                    ?om void:filterx ?filterx 
        } .
        OPTIONAL {?pom rml:objectMap ?om .        
                    ?om void:projectx ?projectx 
        } .
        OPTIONAL { ?pom rml:object ?object } .
        OPTIONAL { 
                ?pom rml:objectMap ?om .                    
                ?om rml:reference ?reference .
        } .
    }
    """)
    mrules = []
    for m in mappings.query(mappingRuleQuery):
        vb = VirtualMapping(*m)
        mrules.append(vb)
    return mrules

def candidateMappingSelection(subquery: list[TriplePattern], mappings: list[VirtualMapping]):
    r = []
    mappings = [copy.copy(m) for m in mappings]
    for tp in subquery:
        compatible_mappings = get_compatible_mappings(tp, mappings)
        if len(compatible_mappings) == 0:  #All the tp in the subquery should have min 1 compatible mapping, otherwise, de conjuctive query shall not be satisfacible
            return []
        for m in compatible_mappings:
            params = {}
            if isinstance(tp.o, Literal) and m.filterx is not None:
                param = m.filterx.replace("@{1}", str(tp.o))
                key, value = param.split('=')
                params = params | {key: value}
            if m.projectx is not None and False:
                key, value = m.projectx.split('=')
                params = params | {key: value}
            elif isinstance(tp.o, Variable) and m.filterx is not None and False: # Aqui se deberia de usar el property select creo.
                params = m.filterx.replace("@{1}", f"var({str(tp.o)})")
                key, value = params.split('=')
                payload = {key: value}
                req = requests.Request('GET', m.source, params=payload).prepare()

            req = requests.Request('GET', m.source, params=params).prepare()
            m.source=req.url   
            r.append(m) if m not in r else None
    return r


def materializeVirtualMappingGroup(vms : list[VirtualMapping], bindings):
    triples_acc = []
    vms_groups = getVirtualMappingsGroups(vms)
    for url, mappings in vms_groups.items():
        url_next = url
        while url_next:
            try:
                r = requests.get(url_next, params={"f": "json", "limit": "2000"}).json()
                print(url_next)
            except:
                return pd.DataFrame([], columns=['Subject', 'Predicate', 'Object']), bindings
            #Podemos usar mappings[0] porque todos los mappings comparten sujeto?
            next = JSONPath(mappings[0].nextPage).parse(r)

            url_next = next[0] if len(next) else False
            
            subj_ref = re.search(r"\{(.*?)\}", mappings[0].s).group(1)
            r_subj = JSONPath(subj_ref).parse(r)
            r_subj = [re.sub(r"\{.*?\}", str(valor), mappings[0].s) for valor in r_subj]

            for m in mappings: 
                if isinstance(m.o, Reference):
                    r_obj = JSONPath(m.o).parse(r) 

                    tripletas = [
                    (sujeto, m.p, objeto) 
                    for sujeto, objeto in zip(r_subj, r_obj)
                    ]
                else: # Object is a constant URI, not a Reference   
                    tripletas = [(sujeto, m.p, m.o) for sujeto in r_subj]

                triples_acc.extend(tripletas)

    df_triples = pd.DataFrame(triples_acc, columns=['Subject', 'Predicate', 'Object'])    

    return df_triples, bindings

sparql_query = """
PREFIX geo: <http://www.opengis.net/ont/geosparql#> 
PREFIX ogc: <http://www.ogc.org/> 
SELECT ?x ?y WHERE {
    ?x a ogc:railwaystationnode .
    ?x ogc:nombre "Estación de A Coruña" .
    ?y a <http://www.ogc.org/agua:estado_masas_aguasub> .
} 
"""

"""

https://wmts.mapama.gob.es/sig-api/ogc/features/v1/collections/agua:masas_aguasub_2027/items?f=json&filter=s_contains(shape,%20POINT(-2.514604067437437%2042.84881824371775))
"""

sparql_queryx = """
PREFIX geo: <http://www.opengis.net/ont/geosparql#> 
PREFIX ogc: <http://www.ogc.org/> 
SELECT ?x WHERE {
    ?x a ogc:railwaystationnode . 
    ?x ogc:nombre "Estación de A Coruña" .
    ?x geo:hasGeometry ?geom .
} 
"""

query = prepareQuery(sparql_query, initNs={"geo": "http://www.opengis.net/ont/geosparql#", "ogc": "http://www.ogc.org/"})

algebra = query.algebra

tps= []

for t in algebra.p.p.triples:
    tc = TriplePattern(*t)
    tps.append(tc)

subqueries = getStarShapedSubqueries(tps)

mappings = getMappings("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/idee-features/mappings/railwaystationnode_mapping.ttl")
mappings2 = getMappings("/Users/kekojohns/Library/CloudStorage/OneDrive-Personal/muia/oeg/tfm/mapama-features/mappings/agua:estado_masas_aguasub_mapping.ttl")

mappings.extend(mappings2)

lista_it = []
bindings = []

for suj, tps in subqueries.items():
    mappings_for_subq = candidateMappingSelection(tps, mappings)
    l, bindings = materializeVirtualMappingGroup(mappings_for_subq, bindings)
    lista_it.append(l)

df_final = pd.concat(lista_it, ignore_index=True)
print(tabulate(df_final, headers='keys', tablefmt='psql', showindex=False))
df_final.to_csv('xd.csv', index=False, encoding='utf-8')


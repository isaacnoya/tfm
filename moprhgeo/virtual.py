from classes import *
from utils import get_invariant

import copy
from mappings import get_compatible_mappings
import rdflib
import requests
from rdflib import Graph, URIRef, BNode, Literal, Namespace, Variable
import pandas as pd
from rdflib.plugins.sparql.sparql import FrozenBindings, QueryContext


from pathlib import Path
from jsonpath import JSONPath
from io import StringIO

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
            m.setBindingVariables(tp.s, tp.p, tp.o)
            r.append(m) if m not in r else None
    return r

def materializeVirtualMappingGroupCTX(vms : list[VirtualMapping], ctx: QueryContext):
    vms_groups = getVirtualMappingsGroups(vms)
    for url, mappings in vms_groups.items():
        url_next = url
        while url_next:
            try:
                r = requests.get(url_next, params={"f": "json", "limit": "2000"}).json()
                #print(url_next)
            except:
                r = {} 
            #Podemos usar mappings[0] porque todos los mappings comparten sujeto?
            next = JSONPath(mappings[0].nextPage).parse(r) if mappings[0].nextPage != None else []

            url_next = next[0] if len(next) else False
            
            if isinstance(mappings[0].s, Reference):
                template = mappings[0].s
                refs = re.findall(r"\{(.*?)\}", template)
                values_per_ref = [JSONPath(ref).parse(r) for ref in refs]
                r_subj = []
                for vals in zip(*values_per_ref):  # empareja 1 a 1
                    result = template
                    for ref, val in zip(refs, vals):
                        result = result.replace(f"{{{ref}}}", str(val))
                    r_subj.append(result)
            else:
                r_subj=mappings[0].s

            for m in mappings: 
                if isinstance(m.o, Reference):
                    r_obj = JSONPath(m.o).parse(r) 
                    r_subj = [mappings[0].s for _ in r_obj] if isinstance(mappings[0].s, URIRef) else r_subj # In case subject is a constant, r_subj and r_obj must be same size in order to zip correctly

                    for sujeto, objeto in zip(r_subj, r_obj):
                        ctx.graph.add((URIRef(sujeto), URIRef(m.p), Literal(objeto)))
                elif isinstance(m.o, URIRef): # Object is a constant URI, not a Reference
                    if isinstance(mappings[0].s, Reference):
                        for sujeto in r_subj:
                            ctx.graph.add((URIRef(sujeto), URIRef(m.p), URIRef(m.o)))
                    else: 
                        ctx.graph.add((URIRef(r_subj), URIRef(m.p), URIRef(m.o)))
    return ctx

def materializeVirtualMappingGroup(vms : list[VirtualMapping], bindings: list[dict], tps: list[TriplePattern]):
    triples_acc = []
    vms_groups = getVirtualMappingsGroups(vms)
    for url, mappings in vms_groups.items():
        url_next = url
        while url_next:
            try:
                r = requests.get(url_next, params={"f": "json", "limit": "2000"}).json()
                #print(url_next)
            except:
                return pd.DataFrame([], columns=['Subject', 'Predicate', 'Object']), bindings
            #Podemos usar mappings[0] porque todos los mappings comparten sujeto?
            next = JSONPath(mappings[0].nextPage).parse(r) if mappings[0].nextPage != None else []

            url_next = next[0] if len(next) else False
            
            if isinstance(mappings[0].s, Reference):
                template = mappings[0].s
                refs = re.findall(r"\{(.*?)\}", template)
                values_per_ref = [JSONPath(ref).parse(r) for ref in refs]
                r_subj = []
                for vals in zip(*values_per_ref):  # empareja 1 a 1
                    result = template
                    for ref, val in zip(refs, vals):
                        result = result.replace(f"{{{ref}}}", str(val))
                    r_subj.append(result)
            else:
                r_subj=mappings[0].s

            for m in mappings: 
                if isinstance(m.o, Reference):
                    r_obj = JSONPath(m.o).parse(r) 
                    r_subj = [mappings[0].s for _ in r_obj] if isinstance(mappings[0].s, URIRef) else r_subj # In case subject is a constant, r_subj and r_obj must be same size in order to zip correctly

                    tripletas = [
                    (sujeto, m.p, objeto) 
                    for sujeto, objeto in zip(r_subj, r_obj)
                    ]
                elif isinstance(m.o, URIRef): # Object is a constant URI, not a Reference   
                    tripletas = [(sujeto, m.p, m.o) for sujeto in r_subj] if isinstance(mappings[0].s, Reference) else [(r_subj, m.p, m.o)] # If both subject and object are constant (not Reference), only a triple is generated
                triples_acc.extend(tripletas)

    df_triples = pd.DataFrame(triples_acc, columns=['Subject', 'Predicate', 'Object'])    

    return df_triples, bindings

from collections import defaultdict

def getStarShapedSubqueries(triple_patterns):
    star_groups = defaultdict(list)
    
    for tp in triple_patterns:
        invariant = get_invariant(tp.s)
        star_groups[invariant].append(tp)
    
    return dict(star_groups)

from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
def getVirtualMappingsGroups(mappings: list[VirtualMapping]):
    grouped_params = defaultdict(dict)
    grouped_mappings = defaultdict(list)

    for vm in mappings:
        parsed = urlparse(vm.source)
        base_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        
        current_params = dict(parse_qsl(parsed.query))
        grouped_params[base_url].update(current_params)
        
        grouped_mappings[base_url].append(vm)

    final_result = {}
    for base, mappings_list in grouped_mappings.items():
        combined_query = urlencode(grouped_params[base])
        full_url = f"{base}?{combined_query}" if combined_query else base
        
        final_result[full_url] = mappings_list

    return final_result
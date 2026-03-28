from classes import *
from utils import get_invariant, getBaseURL, merge_urls
from geoFunctions import getBbox, parse_geom

import copy
from mappings import get_compatible_mappings
import rdflib
import requests
from rdflib import Graph, URIRef, BNode, Literal, Namespace, Variable
import pandas as pd
from rdflib.plugins.sparql.sparql import (
    AlreadyBound,
    FrozenBindings,
    FrozenDict,
    Query,
    QueryContext,
    SPARQLError,
)


from pathlib import Path
from jsonpath import JSONPath
from io import StringIO
from typing import Generator

EX = rdflib.Namespace("http://example.com/")


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

def materializeVirtualMappingGroup(vms : list[VirtualMapping], ctx: QueryContext):
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

def yield_flatten(items):
    for item in items:
        if isinstance(item, set):
            yield from yield_flatten(item)
        else:
            yield item

def getMappingsFromBGP(ctx: MappingContext, tps: list[TriplePattern], mappings: list[VirtualMapping]):
    if not tps:
        yield from yield_flatten(ctx.mappings)
        #yield ctx.mappings
        return

    tp = tps[0]
    s, p, o = tp.s, tp.p, tp.o
    _s = ctx[s] 
    _p = ctx[p] 
    _o = ctx[o] 

    _tp = TriplePattern(_s, _p, _o) 
    c_mappings = get_compatible_mappings(_tp, mappings) 
    for m in c_mappings:
        m = copy.copy(m)
        ss = m.s
        sp = m.p
        so = m.o
    
        if None in (_s, _p, _o):
            c = ctx.push()
        else:
            c = ctx

        try:
            if _p is None:
                c[p] = sp
        except AlreadyBound:
            continue
    
        try:
            if _o is None:
                c[o] = so
        except AlreadyBound:
            continue
        
        if _s is None:
            c[s] = ss 

        m.setBindingVariables(s, p , o)
        params = {}

        if (type(_tp.o) is Literal or type(tp.o) is Variable) and m.filterx is not None and _p is not None:
            param = m.filterx.replace("@{1}", str(tp.o)) if type(_tp.o) is Literal else m.filterx.replace("@{1}", "variable("+str(tp.o)+")")
            key, value = param.split('=')
            params = params | {key: value}
            req = requests.Request('GET', m.source, params=params).prepare()
            m.source=req.url
        
        if type(_tp.o) is BoundedGeometry and m.filterx is not None and _p is not None:
            param = m.filterx.replace("@{1}", getBbox(parse_geom(_tp.o)))
            key, value = param.split('=')
            params = params | {key: value}
            req = requests.Request('GET', m.source, params=params).prepare()
            m.source=req.url


        """
        for vm in ctx.mappings:
            if m.safeUnifySourceMapping(vm):
                req = requests.Request('GET', vm.source, params=params).prepare()
                vm.source=req.url
        """
        c.mappings.append(m) 

        for res in getMappingsFromBGP(c, tps[1:], mappings):
            yield res

    return None


def evalVirtualBGP(ctx: QueryContext, bgp: list[TriplePattern],  mappingGroups: dict, triggers, queriesMade):
    if not bgp:
        yield ctx.solution()
        return
    
    tp = bgp[0]
    s, p, o = tp.s, tp.p, tp.o

    _s = ctx[s] 
    _p = ctx[p] 
    _o = ctx[o] 

    materializeCompatibleMappingGroup(ctx, tp, mappingGroups, triggers, queriesMade)    
    for ss, sp, so in ctx.graph.triples((_s, _p, _o)):  # type: ignore[union-attr, arg-type]
        if None in (_s, _p, _o):
            c = ctx.push()
        else:
            c = ctx

        if _s is None:
            # type error: Incompatible types in assignment (expression has type "Union[Node, Any]", target has type "Identifier")
            c[s] = ss  # type: ignore[assignment]

        try:
            if _p is None:
                # type error: Incompatible types in assignment (expression has type "Union[Node, Any]", target has type "Identifier")
                c[p] = sp  # type: ignore[assignment]
        except AlreadyBound:
            continue

        try:
            if _o is None:
                # type error: Incompatible types in assignment (expression has type "Union[Node, Any]", target has type "Identifier")
                c[o] = so  # type: ignore[assignment]
        except AlreadyBound:
            continue

        for x in evalVirtualBGP(c, bgp[1:], mappingGroups, triggers, queriesMade):
            yield x

    return None

def isCompatibleMappingGroup(tp, mappings):
    for m in mappings:
        ms, mp, mo = m.bindingVariables
        if tp.s == ms and tp.p == mp and tp.o == mo:
            return True
    return False

import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

def injectBindings(ctx, url):
    url_parts = list(urlparse(url))
    query_params = parse_qsl(url_parts[4]) 
    
    nuevos_params = []
    
    for key, value in query_params:
        match = re.search(r'variable\((\w+)\)', value)        
        if match:
            var_name = match.group(1)
            valor_ctx = ctx[Variable(var_name)]

            var_id = URIRef(f"urn:var:{var_name}")
            bbox = ctx.graph.value(var_id, EX.hasBBox)
            
            if valor_ctx is not None:
                nuevo_valor = value.replace(match.group(0), str(valor_ctx)) 
                nuevos_params.append((key, nuevo_valor))
            elif bbox is not None:
                nuevo_valor = value.replace(match.group(0), getBbox(parse_geom(bbox)))
                nuevos_params.append((key, nuevo_valor))

        else:
            nuevos_params.append((key, value))

    url_parts[4] = urlencode(nuevos_params)
    return urlunparse(url_parts)

def materializeGroup(ctx, mappings, suj, queriesMade):
    url_next = merge_urls([m.source for m in mappings])
    url_next = injectBindings(ctx, url_next)

    if (url_next, suj) in queriesMade:
        return ctx

    queriesMade.add((url_next, suj))

    while url_next:
        try:
            r = requests.get(url_next, params={"f": "json", "limit": "2000"}).json()
            print(url_next)
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

def materializeCompatibleMappingGroup(ctx, tp, mappingGroups, triggers, queriesMade):
    for key in list(mappingGroups.keys()):
        mappings = mappingGroups[key]

        # Only materialize when tp is the trigger tp for the mappingGroup (the firts tp)
        if isCompatibleMappingGroup(tp, mappings) and (triggers[key] == tp or triggers[key] is None):
            triggers[key] = tp
            ctx = materializeGroup(ctx, mappings, key[1], queriesMade)
            #del mappingGroups[key] 

    return ctx, mappingGroups

def getMappingGroups(mappings: set[VirtualMapping]) -> dict:
    groups = defaultdict(list)

    for m in mappings:
        key = (
            getBaseURL(m.source),
            m.bindingVariables[0], #variable sujeto
            m.s
        )
        groups[key].append(m)
    
    return groups


def orderTriplesStatic(ctx, triples) -> list:
    def count_free_vars(t):
        return sum(1 for node in t if ctx[node] is None)

    sujeto_min_vars = {}
    for t in triples:
        s = t[0]
        v_count = count_free_vars(t)
        if s not in sujeto_min_vars or v_count < sujeto_min_vars[s]:
            sujeto_min_vars[s] = v_count
        
    triplesOut = sorted(
        triples,
        key=lambda t: (
            sujeto_min_vars[t[0]], # Primero el grupo del sujeto más "prometedor"
            t[0],                  # Forzamos que el mismo sujeto esté contiguo
            count_free_vars(t)     # Dentro del grupo, la más restrictiva primero
        )
    )
    
    return triplesOut
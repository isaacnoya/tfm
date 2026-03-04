RML_NAMESPACE = 'http://w3id.org/rml/'
RML_SUBJECT_MAP = f'{RML_NAMESPACE}subjectMap'
RML_PREDICATE_MAP = f'{RML_NAMESPACE}predicateMap'
RML_OBJECT_MAP = f'{RML_NAMESPACE}objectMap'
RML_SUBJECT_SHORTCUT = f'{RML_NAMESPACE}subject'
RML_PREDICATE_SHORTCUT = f'{RML_NAMESPACE}predicate'
RML_OBJECT_SHORTCUT = f'{RML_NAMESPACE}object'
RML_PREDICATE_OBJECT_MAP = f'{RML_NAMESPACE}predicateObjectMap'
RML_CLASS = f'{RML_NAMESPACE}class'


import rdflib
from rdflib.plugins.sparql import prepareQuery
import re
from classes import Reference, VirtualMapping
  

def rdf_class_to_pom(mapping_graph):
    """
    Replace rr:class definitions by predicate object maps.
    """

    query = 'SELECT ?tm ?c WHERE { ' \
            f'?tm <{RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{RML_CLASS}> ?c . }}'
    for tm, c in mapping_graph.query(query):
        blanknode = rdflib.BNode()
        mapping_graph.add((tm, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), blanknode))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_PREDICATE_SHORTCUT), rdflib.RDF.type))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_OBJECT_SHORTCUT), c))

    mapping_graph.remove((None, rdflib.term.URIRef(RML_CLASS), None))

    return mapping_graph



def get_invariant(mapping_template):
    """
    Extrae el invariante: la subcadena inicial común compartida por todos 
    los triples en el rango del mapeo.
    """
    template_str = str(mapping_template)
    # Buscamos la posición del primer '{' para obtener la parte estática
    match = re.split(r'\{', template_str)
    return match[0]

def is_compatible_old(pattern, mapping):
    """
    Verifica la compatibilidad posición a posición.
    pattern: (s, p, o)
    mapping: (subject_template, predicate, object_reference)
    """
    p_s, p_p, p_o = pattern
    m_s, m_p, m_o = mapping.s, mapping.p, mapping.o

    # 1. Matching de Predicado (Lo más restrictivo y eficiente) 
    if not isinstance(p_p, rdflib.term.Variable) and p_p != m_p:
        return False

    # 2. Matching de Sujeto (Invariante) 
    if not isinstance(p_s, rdflib.term.Variable) and get_invariant(m_s) != get_invariant(p_s):
        return False

    # 3. Matching de Objeto 
    if not isinstance(p_o, rdflib.term.Variable) and not isinstance(p_o, rdflib.term.Literal) and get_invariant(m_o) != get_invariant(p_o):
        return False    
    
    return True


def get_compatible_mappings(pattern, mappings):
    """
    Devuelve la lista de mapeos compatibles con el patrón dado.
    """
    compatible_mappings = []
    for mapping in mappings:
        if is_compatible(pattern, mapping):
            compatible_mappings.append(mapping)
    return compatible_mappings


def termMapCompatibility(t1, t2):
    i1 = get_invariant(t1)
    i2 = get_invariant(t2)

    if isinstance(t1, rdflib.term.Variable) or isinstance(t2, rdflib.term.Variable):
        return True
    if not isinstance(t1, type(t2)) and not isinstance(t2, type(t1)):
        return False
    if ( i1 != i2 or not i1.startswith(i2) or not i2.startswith(i1) ) and not isinstance(t1, Reference) and not isinstance(t2, Reference):
        return False

    return True


def is_compatible(pattern, mapping):
    p_s, p_p, p_o = pattern.s, pattern.p, pattern.o
    m_s, m_p, m_o = mapping.s, mapping.p, mapping.o

    if termMapCompatibility(p_s, m_s) and termMapCompatibility(p_p, m_p) and termMapCompatibility(p_o, m_o):
        return True
    return False


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
import rdflib
import re
from classes import *
  

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



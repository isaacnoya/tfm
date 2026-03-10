from classes import Reference, VirtualMapping
import rdflib
from utils import is_compatible, rdf_class_to_pom
from rdflib.plugins.sparql import prepareQuery


def get_compatible_mappings(pattern, mappings):
    """
    Devuelve la lista de mapeos compatibles con el patrón dado.
    """
    compatible_mappings = []
    for mapping in mappings:
        if is_compatible(pattern, mapping):
            compatible_mappings.append(mapping)
    return compatible_mappings


def getMappings(mapping_file):
    mappings = rdflib.Graph()
    mappings = rdf_class_to_pom(mappings.parse(mapping_file, format="turtle"))

    # select all mapping rules with the form (url, iterator, subject, predicate, object)
    mappingRuleQuery = prepareQuery("""
    PREFIX rml: <http://w3id.org/rml/>
    PREFIX htv: <http://www.w3.org/2011/http#>
    PREFIX void: <http://rdfs.org/ns/void#> 

    SELECT ?subject ?predicate ?object ?reference ?url ?iterator ?nextPage ?filterx ?projectx WHERE {
        ?tm a rml:TriplesMap ;
            rml:logicalSource ?ls ;
            rml:predicateObjectMap ?pom .
        ?ls rml:source ?source ;
            rml:iterator ?iterator .
        ?source htv:absoluteURI ?url .
        ?pom rml:predicate ?predicate .
        ?tm rml:subjectMap ?sm .
        OPTIONAL {
            ?sm rml:template ?subject .
        } .
        OPTIONAL {
            ?sm rml:constant ?subject .
        } .
        OPTIONAL { ?pom rml:objectMap ?om .        
                    ?om void:filterx ?filterx 
        } .
        OPTIONAL {?pom rml:objectMap ?om .        
                    ?om void:projectx ?projectx 
        } .
        OPTIONAL {
                    ?ls void:nextPage ?nextPage
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

def getMappingsFromTxT(file_name):
    all_rules = []
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                mapping_file = line.strip()                
                if mapping_file:
                    rules = getMappings(mapping_file)
                    all_rules.extend(rules)                    
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de rutas en {file_name}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        
    return all_rules
import rdflib
import re

class Reference(rdflib.term.Literal):
    def __init__(self, value, *args, **kwargs):
            # Opcional: puedes añadir lógica específica aquí
            super().__init__()    

class VirtualMapping:
    def __init__(self, subject=None, predicate=None, object=None, reference=None, source=None, iterator=None, nextPage=None, filterx=None, projectx=None):
        if subject != None:
            self.s = self.saturateLiteral(subject, iterator) if isinstance(subject, rdflib.term.Literal) and iterator!=None else subject
        if predicate != None:
            self.p = self.saturateLiteral(predicate, iterator) if isinstance(predicate, rdflib.term.Literal) and iterator!=None else predicate
        if object != None and reference == None:
            self.o = object
        elif reference != None and object == None:
            self.o = Reference(self.saturateLiteral(reference, iterator).toPython()) if iterator!=None else reference
        if source != None:
            self.source = source
        
        self.nextPage = nextPage
        self.filterx = filterx
        self.projectx = projectx

    def saturateLiteral(self, literal, iterator):
        """
        reciba un literal flow "https://api-features.idee.es/collections/railwaystationnode/items/{id}" y un iterator flow "$.features.*" y me devuelva "https://api-features.idee.es/collections/railwaystationnode/items/{$.features.*.id}"
        """
        literal = str(literal.toPython()) if hasattr(literal, 'toPython') else str(literal)
        if "{" in literal:
            patron = r'\{([^}]+)\}'
        
            def reemplazo(match):
                variable_original = match.group(1) 
                return f"{{{iterator}.{variable_original}}}"
                
            resultado = re.sub(patron, reemplazo, literal)
            return rdflib.term.Literal(resultado)
        else:
            return rdflib.term.Literal(f"{iterator}.{literal}")
        
class TriplePattern:
    def __init__(self, s, p, o):
        self.s=s
        self.p=p
        self.o=o

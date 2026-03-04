import os
import json
from groq import Groq
from dotenv import load_dotenv
import requests

load_dotenv()
    
def searchLLM(term, type="class",description="", model="openai/gpt-oss-120b"):  

    api_key = os.getenv("GROQ_API_KEY")

    client = Groq(api_key=api_key)
    
    # El "System Prompt" es clave para que el LLM no alucine y devuelva JSON puro
    prompt_sistema = """
    You are an expert in Semantic Web and Linked Data.
    Your task is to map user {type} to Wikidata and DBpedia.
    Respond ONLY in JSON format with the following structure:
    {
    "{type}": "...",
    "wikidata_qid": "Q...",
    "dbpedia_uri": "http://dbpedia.org/ontology/..."
    }
    If you are not sure, set a low confidence value.
    """
    
    prompt_usuario = f"Find the equivalent Wikidata ID and DBpedia {type} URI for the {type}: '{term}'" if not description else f"Find the equivalent Wikidata QID and DBpedia {type} URI for the {type}: '{term}' with the following description: '{description}'"

    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": prompt_sistema},
                {"role": "user", "content": prompt_usuario}
            ],
            model=model, # Usamos el modelo grande para mejor precisión
            temperature=0.1, # Temperatura baja para que sea determinista
            response_format={"type": "json_object"} # Forzamos salida JSON
        )

        # Parsear la respuesta
        respuesta_json = json.loads(chat_completion.choices[0].message.content)
        return respuesta_json

    except Exception as e:
        print(f"Error con Groq: {e}")
        return None

from SPARQLWrapper import SPARQLWrapper, JSON

def existe_en_wikidata(id_recurso):
    """Consulta si un ID (ej: Q42, P31) existe en Wikidata."""
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    # El User-Agent es obligatorio para Wikidata
    sparql.agent = "MiBotVerificador/1.0" 
    
    query = f"""
    ASK {{
      wd:{id_recurso} ?p ?o .
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    resultado = sparql.query().convert()
    return resultado["boolean"]

def existe_en_dbpedia(recurso_ontologia):
    """Consulta si una clase o propiedad (ej: City, birthDate) existe en la ontología de DBpedia."""
    sparql = SPARQLWrapper("https://dbpedia.org/sparql")
    
    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    ASK {{
      dbo:{recurso_ontologia} ?p ?o .
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    resultado = sparql.query().convert()
    return resultado["boolean"]

def buscar_dbpedia_label(label):
    """Busca una clase o propiedad en DBpedia por su label."""
    sparql = SPARQLWrapper("https://dbpedia.org/sparql")
    
    query = f"""
    SELECT ?resource WHERE {{
      ?resource rdfs:label "{label}"@es .
    }} LIMIT 1
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    resultado = sparql.query().convert()
    if resultado["results"]["bindings"]:
        return resultado["results"]["bindings"][0]["resource"]["value"]
    return None

# Ejemplos de uso
if __name__ == "__main__":
    term = ["component_AddressAreaName", "property"]
    if not (result:= buscar_dbpedia_label(term[0])):
        print(f"La clase '{term[0]}' no existe en DBpedia, buscando con LLM...")
        result = searchLLM(term[0], type=term[1])
        print("Resultado:", result)
        # Comprobar si existe la entidad "Douglas Adams" (Q42)
        print(f"¿Existe {result['wikidata_qid']} en Wikidata?: {existe_en_wikidata(result['wikidata_qid'])}")
        
        dbpedia_resource = result['dbpedia_uri'].split('/')[-1] if result['dbpedia_uri'] else None
        # Comprobar si existe la clase "City" en la ontología de DBpedia
        print(f"¿Existe {dbpedia_resource} en DBpedia?: {existe_en_dbpedia(dbpedia_resource)}")
    else:
        print(f"La clase '{term[0]}' ya existe en DBpedia con URI: {result}")



import requests
import json
import psycopg

prompt = input("Escribe tu consulta de PostGIS: ")

parametros = {
    "model": "embeddinggemma:300m",
    "prompt": prompt
}
respuesta = requests.post("http://localhost:11434/api/embeddings", json = parametros)
embedding = respuesta.json()["embedding"]

conexion = psycopg.connect("dbname=Anapoima user=juanma password=geocositas port=5432 host=172.17.52.185")
conexion.autocommit = True
cur = conexion.cursor()

emb_str = "["+ ",".join(f"{x:.6f}" for x in embedding) + "]"
cur.execute("""
    SELECT content, metadata
    FROM kb_items
    ORDER BY embedding <-> %s::vector
    LIMIT 5
""", (emb_str,))

rows = cur.fetchall()

context_parts = []
for content, metadata in rows:
    context_parts.append(f"{content}\n(metadata: {json.dumps(metadata, ensure_ascii = False)})")
contexto = "\n\n".join(context_parts)

llm_prompt = f"""
Eres un asistente experto en SIG.
Utiliza únicamente este contexto para responder (no inventes datos):

{contexto}

Pregunta: {prompt}
"""

parametros2 = {
    "model": "granite4:latest",
    "prompt": llm_prompt,
    "stream": False
}
respuesta2 = requests.post("http://localhost:11434/api/generate", json = parametros2)
respuesta_final = respuesta2.json()["response"]

print(respuesta_final)

cur.close()
conexion.close()
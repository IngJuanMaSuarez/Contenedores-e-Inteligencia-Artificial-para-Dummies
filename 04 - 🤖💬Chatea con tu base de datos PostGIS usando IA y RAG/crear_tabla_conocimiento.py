import requests
import json
import psycopg

# prompt = input("Escribe tu consulta de PostGIS: ")

def get_embedding(prompt):
    parametros = {
        "model": "embeddinggemma:300m",
        "prompt": prompt
    }
    respuesta = requests.post("http://localhost:11434/api/embeddings", json = parametros)
    return respuesta.json()["embedding"]

def embedding_to_str(emb):
    return "["+ ",".join(f"{x:.6f}" for x in emb) + "]"

conexion = psycopg.connect("dbname=Anapoima user=juanma password=geocositas port=5432 host=172.17.52.185")
conexion.autocommit = True
cur = conexion.cursor()

# ---- TABLA: colegios ----
cur.execute("""
SELECT id, nombre, categoria, cupos,
       ST_AsText(ST_Centroid(geom)) AS centroide_wkt,
       ST_AsGeoJSON(ST_Centroid(geom)) AS centroide_geojson,
       ST_Transform(ST_Centroid(geom), 4326) AS centroide_geom
FROM colegios;
""")
rows = cur.fetchall()
for row in rows:
    source_id, nombre, categoria, cupos, centroide_wkt, centroide_geojson, centroide_geom = row
    content = f"Colegio {nombre}, categoría {categoria}, con cupos {cupos}, ubicado en {centroide_wkt}."
    metadata = {
        "tabla": "colegios",
        "id": source_id,
        "nombre": nombre,
        "categoria": categoria,
        "cupos": cupos,
        "centroide_geojson": centroide_geojson
    }
    emb = get_embedding(content)
    emb_str = embedding_to_str(emb)
    # Insertar con geom_centroid
    cur.execute("""
    INSERT INTO kb_items (source_table, source_id, content, metadata, embedding, geom_centroid)
    VALUES (%s, %s, %s, %s, %s::vector, %s)
    """, ("colegios", source_id, content, json.dumps(metadata, ensure_ascii=False), emb_str, centroide_geom))

# ---- TABLA: equipamientos ----
cur.execute("""
SELECT id, nombre,
       ST_AsText(ST_Centroid(geom)) AS centroide_wkt,
       ST_AsGeoJSON(ST_Centroid(geom)) AS centroide_geojson,
       ST_Transform(ST_Centroid(geom), 4326) AS centroide_geom
FROM equipamientos;
""")
rows = cur.fetchall()
for row in rows:
    source_id, nombre, centroide_wkt, centroide_geojson, centroide_geom = row
    content = f"Equipamiento {nombre}, ubicado en {centroide_wkt}."
    metadata = {
        "tabla": "equipamientos",
        "id": source_id,
        "nombre": nombre,
        "centroide_geojson": centroide_geojson
    }
    emb = get_embedding(content)
    emb_str = embedding_to_str(emb)
    cur.execute("""
    INSERT INTO kb_items (source_table, source_id, content, metadata, embedding, geom_centroid)
    VALUES (%s, %s, %s, %s, %s::vector, %s)
    """, ("equipamientos", source_id, content, json.dumps(metadata, ensure_ascii=False), emb_str, centroide_geom))

# ---- TABLA: manzanas ----
cur.execute("""
SELECT id, numero_manzana, area, 
       ST_AsText(ST_Centroid(geom)) AS centroide_wkt,
       ST_AsGeoJSON(ST_Centroid(geom)) AS centroide_geojson,
       ST_Transform(ST_Centroid(geom), 4326) AS centroide_geom
FROM manzanas;
""")
rows = cur.fetchall()
for row in rows:
    source_id, numero_manzana, area, centroide_wkt, centroide_geojson, centroide_geom = row
    content = f"Manzana número {numero_manzana}, área aproximada {area} hectáreas, centroide {centroide_wkt}."
    metadata = {
        "tabla": "manzanas",
        "id": source_id,
        "numero_manzana": numero_manzana,
        "area_ha": area,
        "centroide_geojson": centroide_geojson
    }
    emb = get_embedding(content)
    emb_str = embedding_to_str(emb)
    cur.execute("""
    INSERT INTO kb_items (source_table, source_id, content, metadata, embedding, geom_centroid)
    VALUES (%s, %s, %s, %s, %s::vector, %s)
    """, ("manzanas", source_id, content, json.dumps(metadata, ensure_ascii=False), emb_str, centroide_geom))

# ---- TABLA: usodelsuelo ----
cur.execute("""
SELECT id, tipo_uso, area,
       ST_AsText(ST_Centroid(geom)) AS centroide_wkt,
       ST_AsGeoJSON(ST_Centroid(geom)) AS centroide_geojson,
       ST_Transform(ST_Centroid(geom), 4326) AS centroide_geom
FROM usodelsuelo;
""")
rows = cur.fetchall()
for row in rows:
    source_id, tipo_uso, area, centroide_wkt, centroide_geojson, centroide_geom = row
    content = f"Uso de suelo {tipo_uso}, área aproximada {area} hectáreas, centroide {centroide_wkt}."
    metadata = {
        "tabla": "usodelsuelo",
        "id": source_id,
        "tipo_uso": tipo_uso,
        "area_ha": area,
        "centroide_geojson": centroide_geojson
    }
    emb = get_embedding(content)
    emb_str = embedding_to_str(emb)
    cur.execute("""
    INSERT INTO kb_items (source_table, source_id, content, metadata, embedding, geom_centroid)
    VALUES (%s, %s, %s, %s, %s::vector, %s)
    """, ("usodelsuelo", source_id, content, json.dumps(metadata, ensure_ascii=False), emb_str, centroide_geom))

print("Script finalizado")

cur.close()
conexion.close()
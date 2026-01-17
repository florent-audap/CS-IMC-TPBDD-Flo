import os

import dotenv
import pyodbc
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node

# Charge les variables d'environnement depuis le fichier .env
# override=True force le rechargement des variables d'environnement à chaque exécution
dotenv.load_dotenv(override=True)

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]
driver = os.environ["ODBC_DRIVER"]

neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
neo4j_user = os.environ["TPBDD_NEO4J_USER"]
neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))

# Taille des lots pour les opérations de bulk insert
# Permet de traiter les données par batch plutôt que individuellement pour plus d'efficacité
BATCH_SIZE = 10000

print("Deleting existing nodes and relationships...")
graph.run("MATCH ()-[r]->() DELETE r")
graph.run("MATCH (n:Artist) DETACH DELETE n")
graph.run("MATCH (n:Film) DETACH DELETE n")

with pyodbc.connect(
    "DRIVER=" + driver +
    ";SERVER=tcp:" + server +
    ";PORT=1433;DATABASE=" + database +
    ";UID=" + username +
    ";PWD=" + password
) as conn:
    cursor = conn.cursor()

    # ============================================================================
    # SECTION 1 : Export des Films
    # ============================================================================
    # Récupère tous les films de la table TFilm et les crée en tant que noeuds 
    # "Film" dans Neo4j avec leurs propriétés principales
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM TFilm")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idFilm, primaryTitle, startYear FROM TFilm")
    while True:
        importData = []
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # Créer un objet Node avec comme label Film et les propriétés adéquates
            # On récupère les colonnes : idFilm (index 0), primaryTitle (index 1), startYear (index 2)
            n = Node(
                "Film",
                idFilm=row[0],
                primaryTitle=row[1],
                startYear=row[2]
            )
            importData.append(n)

        try:
            create_nodes(graph.auto(), importData, labels={"Film"})
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} title records exported to Neo4j")
        except Exception as error:
            print(error)

    # ============================================================================
    # SECTION 2 : Export des Artistes/Noms
    # ============================================================================
    # Récupère tous les artistes de la table tArtist et les crée en tant que noeuds 
    # "Artist" dans Neo4j avec leurs propriétés principales
    # Cette section suit le même pattern que la section Films
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM tArtist")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idArtist, primaryName, birthYear FROM tArtist")
    while True:
        importData = []
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # Créer un objet Node avec comme label Artist et les propriétés adéquates
            # On récupère les colonnes : idArtist (index 0), primaryName (index 1), birthYear (index 2)
            n = Node(
                "Artist",
                idArtist=row[0],
                primaryName=row[1],
                birthYear=row[2]
            )
            importData.append(n)

        try:
            create_nodes(graph.auto(), importData, labels={"Artist"})
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} artist records exported to Neo4j")
        except Exception as error:
            print(error)

    try:
        print("Indexing Film nodes...")
        # Crée un index sur la propriété idFilm des noeuds Film pour optimiser les recherches
        # Syntaxe Neo4j 4.x avec IF NOT EXISTS pour éviter les erreurs si l'index existe déjà
        graph.run("""CREATE INDEX film_idFilm_index IF NOT EXISTS FOR (f:Film) ON (f.idFilm)""")
        print("Indexing Artist nodes...")
        # Crée un index sur la propriété idArtist des noeuds Artist pour optimiser les recherches
        graph.run("""CREATE INDEX artist_idArtist_index IF NOT EXISTS FOR (a:Artist) ON (a.idArtist)""")
    except Exception as error:
        print(error)

    # ============================================================================
    # SECTION 3 : Export des Relations
    # ============================================================================
    # Récupère les relations entre Artistes et Films depuis la table tJob
    # Crée des relations typées selon le rôle de l'artiste (acted_in, directed, produced, composed)
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM tJob")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idArtist, category, idFilm FROM tJob")
    while True:
        importData = {"acted in": [], "directed": [], "produced": [], "composed": []}
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            # sécurité si une catégorie inattendue apparaît
            # Ajoute la catégorie au dictionnaire si elle n'existe pas déjà
            if row[1] not in importData:
                importData[row[1]] = []
            # Créer un tuple (Artist_ID, properties, Film_ID)
            # Les properties restent vides pour les relations simples
            relTuple = (row[0], {}, row[2])  # (start_idArtist, props, end_idFilm)
            importData[row[1]].append(relTuple)

        try:
            for cat in importData:
                # Ignore les catégories vides pour éviter de créer des relations inutiles
                if not importData[cat]:
                    continue

                # ATTENTION: remplacez les espaces par des _ pour nommer les types de relation
                # Exemple: "acted in" => "acted_in" (convention Neo4j pour les types de relation)
                rel_type = cat.replace(" ", "_")

                # Utilise la fonction py2neo pour créer les relations en bulk
                # Elle recherche les noeuds Artist par leur idArtist et les noeuds Film par leur idFilm
                # puis crée les relations du type spécifié entre eux
                create_relationships(
                    graph.auto(),
                    importData[cat],
                    rel_type,
                    start_node_key=("Artist", "idArtist"),
                    end_node_key=("Film", "idFilm"),
                )

            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} relationships exported to Neo4j")
        except Exception as error:
            print(error)
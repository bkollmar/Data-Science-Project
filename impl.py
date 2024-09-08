import os
import pandas as pd
import re
import requests
import sqlite3
import json
from rdflib import Graph, URIRef, Literal
from pandas import read_csv
from rdflib.namespace import RDF
from sparql_dataframe import get
from pandas import concat
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from typing import List, Union, Optional

BLAZEGRAPH_ENDPOINT = 'http://127.0.0.1:9999/blazegraph/sparql'
CSV_FILEPATH = 'data/meta.csv'

# REMEMBER: before running this code, please run the Blazegraph instance!
# 
# Run command:
#   `java -server -Xmx4g -jar blazegraph.jar`

class IdentifiableEntity(object): #Rubens
    def __init__(self, id: str):
        self.id = id

    def getId(self) -> str:
        return self.id


class Person(IdentifiableEntity):  # Rubens
    def __init__(self, id: str, name: str):
        self.name = name
        super().__init__(id)

    def getName(self) -> str:
        return self.name


class CulturalHeritageObject(IdentifiableEntity):  # Ben
    def __init__(
        self,
        id: str,
        title: str,
        date: Optional[str],
        owner: str,
        place: str,
        hasAuthor: Union[Person, List[Person], None] = None,
        author_id: Optional[str] = None,
        author_name: Optional[str] = None,
    ):
        super().__init__(str(id))
        self.id = id
        self.title = title
        self.date = date
        self.hasAuthor = hasAuthor or []
        self.owner = str(owner)
        self.place = place
        self.author_id = author_id
        self.author_name = author_name

        if type(hasAuthor) == Person:
            self.hasAuthor.append(Person)
        elif type(hasAuthor) == list:
            self.hasAuthor = hasAuthor

    def getTitle(self) -> str:
        return self.title

    def getOwner(self) -> str:
        return self.owner

    def getPlace(self) -> str:
        return self.place

    def getDate(self) -> Optional[str]:
        if self.date:
            return self.date
        return None

    def getAuthors(self) -> List[Person]:
        return self.hasAuthor


class NauticalChart(CulturalHeritageObject):
    pass


class ManuscriptPlate(CulturalHeritageObject):
    pass


class ManuscriptVolume(CulturalHeritageObject):
    pass


class PrintedVolume(CulturalHeritageObject):
    pass


class PrintedMaterial(CulturalHeritageObject):
    pass


class Herbarium(CulturalHeritageObject):
    pass


class Specimen(CulturalHeritageObject):
    pass


class Painting(CulturalHeritageObject):
    pass


class Model(CulturalHeritageObject):
    pass


class Map(CulturalHeritageObject):
    pass


class Activity(object):  # Rubens
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        institute: str,
        person: Optional[str],
        tool: Union[str, List[str], None],
        start: Optional[str],
        end: Union[str, List[str], None],
    ):
        self.refersTo = refersTo
        self.institute = institute
        self.person = person
        self.tool = []
        self.start = start
        self.end = end

        if type(tool) == str:
            self.tool.append(tool)
        elif type(tool) == list:
            self.tool = tool

    def getResponsibleInstitute(self) -> str:
        return self.institute

    def getResponsiblePerson(self) -> Optional[str]:
        if self.person:
            return self.person
        return None

    def getTools(self) -> set:
        return self.tool

    def getStartDate(self) -> Optional[str]:
        if self.start:
            return self.start
        return None

    def getEndDate(self) -> Optional[str]:
        if self.end:
            return self.end
        return None

    def refersTo(self) -> CulturalHeritageObject:
        return self.refersTo


class Acquisition(Activity):
    def __init__(
        self,
        refersTo: CulturalHeritageObject,
        institute: str,
        technique: str,
        person: Optional[str],
        start: Optional[str],
        end: Optional[str],
        tool: Union[str, List[str], None],
    ):
        super().__init__(refersTo, institute, person, tool, start, end)
        self.technique = technique

    def getTechnique(self) -> str:
        return self.technique


class Processing(Activity):
    pass


class Modelling(Activity):
    pass


class Optimising(Activity):
    pass


class Exporting(Activity):
    pass


class Handler(object):  # Ekaterina
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self) -> str:
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl: str) -> bool:
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl


class UploadHandler(Handler):  # Ekaterina
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, file_path: str) -> bool:
        self.file_path = file_path
        blazegraph_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"


        # split filepath for file extension
        _, extension = os.path.splitext(file_path)
        if extension == ".db":
            # process json data for SQL database -> upload data to SQL Lite data base
            process_qh = ProcessDataUploadHandler()
            result = process_qh.setDbPathOrUrl(self.db_file)
            
        elif extension == ".json":
            # process json data for SQL database -> upload data to SQL Lite data base
            process_qh = ProcessDataUploadHandler()
            result = process_qh.setDbPathOrUrl(self.db_file)

        elif extension == ".csv":
            # process csv data & turn into RDF -> upload RDF data to blazegraph store
            metadata_qh = MetadataUploadHandler()
            result = metadata_qh.setDbPathOrUrl(blazegraph_endpoint)

        else:
            raise Exception("Only .json or .csv files can be uploaded!")
        print("Finished uploading data! ✅")
        return result


class ProcessDataUploadHandler(UploadHandler):  # Ekaterina
    def __init__(self):
        super().__init__


    file_path = "data/process.json"

    try:
        with open(file_path) as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        print("Error: JSON file not found.")
        exit()

    db_file = "json.db"

    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        c.execute(
            """CREATE TABLE IF NOT EXISTS Acquisition (
                        object_id TEXT,
                        responsible_institute TEXT,
                        responsible_person TEXT,
                        technique TEXT,
                        tool TEXT,
                        start_date TEXT,
                        end_date TEXT
                    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS Processing (
                        object_id TEXT,
                        responsible_institute TEXT,
                        responsible_person TEXT,
                        tool TEXT,
                        start_date TEXT,
                        end_date TEXT
                    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS Modelling (
                        object_id TEXT,
                        responsible_institute TEXT,
                        responsible_person TEXT,
                        tool TEXT,
                        start_date TEXT,
                        end_date TEXT
                    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS Optimising (
                        object_id TEXT,
                        responsible_institute TEXT,
                        responsible_person TEXT,
                        tool TEXT,
                        start_date TEXT,
                        end_date TEXT
                    )"""
        )

        c.execute(
            """CREATE TABLE IF NOT EXISTS Exporting (
                        object_id TEXT,
                        responsible_institute TEXT,
                        responsible_person TEXT,
                        tool TEXT,
                        start_date TEXT,
                        end_date DATE
                    )"""
        )

        for item in data:
            object_id = item["object id"]

            acquisition = item["acquisition"]
            c.execute(
                """INSERT INTO Acquisition (object_id, responsible_institute, responsible_person, technique, tool, start_date, end_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    object_id,
                    acquisition["responsible institute"],
                    acquisition["responsible person"],
                    acquisition["technique"],
                    ", ".join(acquisition["tool"]) if acquisition["tool"] else None,
                    acquisition["start date"],
                    acquisition["end date"],
                ),
            )

            processing = item["processing"]
            c.execute(
                """INSERT INTO Processing (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    object_id,
                    processing["responsible institute"],
                    processing["responsible person"],
                    ", ".join(processing["tool"]) if processing["tool"] else None,
                    processing["start date"],
                    processing["end date"],
                ),
            )

            modelling = item["modelling"]
            c.execute(
                """INSERT INTO Modelling (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    object_id,
                    modelling["responsible institute"],
                    modelling["responsible person"],
                    ", ".join(modelling["tool"]) if modelling["tool"] else None,
                    modelling["start date"],
                    modelling["end date"],
                ),
            )

            optimising = item["optimising"]
            c.execute(
                """INSERT INTO Optimising (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    object_id,
                    optimising["responsible institute"],
                    optimising["responsible person"],
                    ", ".join(optimising["tool"]) if optimising["tool"] else None,
                    optimising["start date"],
                    optimising["end date"],
                ),
            )

            exporting = item["exporting"]
            c.execute(
                """INSERT INTO Exporting (object_id, responsible_institute, responsible_person, tool, start_date, end_date)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    object_id,
                    exporting["responsible institute"],
                    exporting["responsible person"],
                    ", ".join(exporting["tool"]) if exporting["tool"] else None,
                    exporting["start date"],
                    exporting["end date"],
                ),
            )
        conn.commit()

    except sqlite3.Error as e:
        print("\nSQLite error:", e)
    finally:
        conn.close()
    print("\nData insertion and querying completed successfully.")



class MetadataUploadHandler(UploadHandler):  # Ekaterina
    def __init__(self):
        super().__init__

    my_graph = Graph()

    # Classes of resources of CulturalHeritageObject
    NauticalChart = URIRef("https://schema.org/NauticalChart")
    ManuscriptPlate = URIRef("https://schema.org/ManuscriptPlate")
    ManuscriptVolume = URIRef("https://schema.org/ManuscriptVolume")
    PrintedVolume = URIRef("https://schema.org/PrintedVolume")
    PrintedMaterial = URIRef("https://schema.org/PrintedMaterial")
    Herbarium = URIRef("https://schema.org/Herbarium")
    Specimen = URIRef("https://schema.org/Specimen")
    Painting = URIRef("https://schema.org/Painting")
    Model = URIRef("https://schema.org/Model")
    Map = URIRef("https://schema.org/Map")
    Author = URIRef("https://schema.org/Author")

    # Attributes related to classes
    title = URIRef("https://schema.org/name")
    date = URIRef("https://schema.org/dateCreated")
    owner = URIRef("https://schema.org/provider")
    place = URIRef("https://schema.org/contentLocation")
    identifier = URIRef("https://schema.org/identifier")
    label = URIRef("http://www.w3.org/2000/01/rdf-schema#label")

    # Relations among classes
    hasAuthor = URIRef("https://schema.org/creator")

    # This is the string defining the base URL used to define
    # the URLs of all the resources created from the data
    base_url = "https://github.com/katyakrsn/ds24project/"

    file_path_csv = "data/meta.csv"
    heritage = read_csv(
        file_path_csv,
        keep_default_na=False,  # Prevent pandas from treating certain values as NaN
        dtype={
            "Id": "string",
            "Type": "string",
            "Title": "string",
            "Date": "string",
            "Author": "string",
            "Owner": "string",
            "Place": "string",
        },
    )

    # Iterate over each row in the CSV file
    for idx, row in heritage.iterrows():
        # Determine the class URI based on the type of CulturalHeritageObject
        class_uri = None
        if row["Type"] == "Nautical chart":
            class_uri = NauticalChart
        elif row["Type"] == "Manuscript plate":
            class_uri = ManuscriptPlate
        elif row["Type"] == "Manuscript volume":
            class_uri = ManuscriptVolume
        elif row["Type"] == "Printed volume":
            class_uri = PrintedVolume
        elif row["Type"] == "Printed material":
            class_uri = PrintedMaterial
        elif row["Type"] == "Herbarium":
            class_uri = Herbarium
        elif row["Type"] == "Specimen":
            class_uri = Specimen
        elif row["Type"] == "Painting":
            class_uri = Painting
        elif row["Type"] == "Model":
            class_uri = Model
        elif row["Type"] == "Map":
            class_uri = Map

        # Create a URI for the resource using the base URL and the row ID
        resource_uri = URIRef(f"{base_url}{row['Id']}")

        # Handle missing date values by assigning a default value
        if not row["Date"]:
            row["Date"] = "Unknown"
            print(f"Missing Date at index {idx}")

        # Add triples to the graph
        my_graph.add((resource_uri, RDF.type, class_uri))
        my_graph.add((resource_uri, identifier, Literal(row["Id"])))
        my_graph.add((resource_uri, title, Literal(row["Title"])))
        my_graph.add((resource_uri, date, Literal(row["Date"])))
        my_graph.add((resource_uri, owner, Literal(row["Owner"])))
        my_graph.add((resource_uri, place, Literal(row["Place"])))

        if row["Author"]:
            # Split the text into two parts
            text_parts = row["Author"].split(" (")
            author = row["Author"]
            text_before_parentheses = author.split(" (")[0]
            authorID = re.findall(r"\((.*?)\)", author)
            authorID = authorID[0] if authorID else "noID"
            authorIRI = base_url + text_before_parentheses.replace(" ", "_").replace(
                ",", ""
            )

            my_graph.add((resource_uri, hasAuthor, URIRef(authorIRI)))
            my_graph.add((URIRef(authorIRI), identifier, Literal(authorID)))
            my_graph.add((URIRef(authorIRI), RDF.type, Author))
            my_graph.add((URIRef(authorIRI), label, Literal(text_before_parentheses)))
        else:
            row["Author"] = "Unknown"
            print(f"Missing Author at index {idx}")

    my_graph.serialize(destination="output_triples.ttl", format="ttl")

    # Initialize SPARQLUpdateStore to interact with Blazegraph
    store = SPARQLUpdateStore()
    endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
    store.open((endpoint, endpoint))
    # Upload triples to the Blazegraph database
    for triple in my_graph.triples((None, None, None)):
        store.add(triple)
    store.close()

    sparql_query = f"""
        SELECT ?subject ?predicate ?object
    WHERE {{
        ?subject ?predicate ?object .
    }}
    ORDER BY ASC(xsd:integer(REPLACE(str(?subject), "{base_url}", "")))
    """
    sparql_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

    # Send the SPARQL query to the Blazegraph endpoint
    response = requests.post(sparql_endpoint, data={"query": sparql_query})
    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.reason}")

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, input_id: str) -> pd.DataFrame:  # Ekaterina/Rubens
        endpoint = self.blazegraph_endpoint
        id_author_query = f"""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>

        SELECT ?identifier ?name ?title
        WHERE {{
            ?entity schema:identifier "{input_id}" .
            ?entity schema:creator ?Author .
            ?Author rdfs:label ?name .
            ?Author schema:identifier ?identifier .
            ?entity schema:name ?title
        }}
        """
        df_sparql = get(endpoint, id_author_query, True)
        return df_sparql


class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        self.blazegraph_endpoint = BLAZEGRAPH_ENDPOINT
        self.csv_file_path = CSV_FILEPATH

    def getAllPeople(self) -> pd.DataFrame:  # Rubens
        sparql_query = """
        PREFIX schema: <https://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?id ?name
        WHERE {
            ?entity schema:creator ?Author .
            ?Author rdfs:label ?name .
            ?Author schema:identifier ?id .
        }
        """
        df_sparql = get(self.blazegraph_endpoint, sparql_query, True)
        return df_sparql

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:  # Ekaterina
        endpoint = self.blazegraph_endpoint
        cultural_object_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>

        SELECT (REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name) ?id ?title ?date ?owner ?place ?author_id ?author_name
        WHERE {
        ?cultural_object rdf:type ?type .
        ?cultural_object schema:name ?title .
        OPTIONAL { ?cultural_object schema:identifier ?id }
        OPTIONAL { ?cultural_object schema:dateCreated ?date }
        OPTIONAL { ?cultural_object schema:provider ?owner }
        OPTIONAL { ?cultural_object schema:contentLocation ?place }
        OPTIONAL { ?cultural_object schema:creator ?author }
        OPTIONAL { ?author schema:identifier ?author_id }
        OPTIONAL { ?author rdfs:label ?author_name }
        
        FILTER(?type IN (
        <https://schema.org/NauticalChart>,
        <https://schema.org/ManuscriptPlate>,
        <https://schema.org/ManuscriptVolume>,
        <https://schema.org/PrintedVolume>,
        <https://schema.org/PrintedMaterial>,
        <https://schema.org/Herbarium>,
        <https://schema.org/Specimen>,
        <https://schema.org/Painting>,
        <https://schema.org/Model>,
        <https://schema.org/Map>
        ))
        FILTER(?author_name != "NaN")
        FILTER(?author_id != "NaN")
        }
        """
        df_sparql = get(endpoint, cultural_object_query, True)
        return df_sparql

    def getAuthorsOfCulturalHeritageObject(self, input_id) -> pd.DataFrame:  # Rubens
        endpoint = self.blazegraph_endpoint
        id_author_query = f"""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?name
        WHERE {{
            ?entity schema:identifier "{input_id}" .
            ?entity schema:creator ?Author .
            ?Author rdfs:label ?name .
            ?Author schema:identifier ?id .
        }}
        """
        df_sparql = get(endpoint, id_author_query, True)
        return df_sparql

    def getCulturalHeritageObjectsAuthoredBy(
        self, input_id
    ) -> pd.DataFrame:  # Ekaterina
        endpoint = self.blazegraph_endpoint
        id_cultural_query = f"""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX schema: <https://schema.org/>

        SELECT ?object ?type_name ?id ?title ?date ?owner ?place ?name ?author_id
            WHERE {{
            ?entity schema:identifier "{input_id}" .
            ?entity schema:creator ?Author .
            ?object schema:creator ?Author .
            ?object rdf:type ?type .
            ?object schema:name ?title .
            ?object schema:identifier ?id .
            ?object schema:dateCreated ?date .
            ?object schema:provider ?owner .
            ?object schema:contentLocation ?place .
            OPTIONAL {{
                ?object schema:creator ?Author .
                ?Author rdfs:label ?name .
                ?Author schema:identifier ?author_id .
            }}
            BIND(REPLACE(STR(?type), "https://schema.org/", "") AS ?type_name)
            FILTER(?type IN (
                    <https://schema.org/NauticalChart>,
                    <https://schema.org/ManuscriptPlate>,
                    <https://schema.org/ManuscriptVolume>,
                    <https://schema.org/PrintedVolume>,
                    <https://schema.org/PrintedMaterial>,
                    <https://schema.org/Herbarium>,
                    <https://schema.org/Specimen>,
                    <https://schema.org/Painting>,
                    <https://schema.org/Model>,
                    <https://schema.org/Map>
                    ))
            }}
            """

        df_sparql = get(endpoint, id_cultural_query, True)
        df_sparql.drop_duplicates(inplace=True)
        return df_sparql


class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, id: str):  # Rubens
        return pd.DataFrame()

    def getAllActivities(self) -> pd.DataFrame:  # Rubens
        db_file = "json.db"
        try:
            conn = sqlite3.connect(db_file)

            # Use LIKE operator to match partially with the technique string
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type  FROM Acquisition
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type  FROM Processing
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type  FROM Modelling
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type  FROM Optimising
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type  FROM Exporting
            """
            df = pd.read_sql_query(query, conn)
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getActivitiesByResponsibleInstitution(
        self, institution_str: str
    ) -> pd.DataFrame:  # Ekaterina
        db_file = "json.db"
        try:
            conn = sqlite3.connect(db_file)

            # Use LIKE operator to match partially with the technique string
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type  FROM Acquisition WHERE responsible_institute LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type  FROM Processing WHERE responsible_institute LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type  FROM Modelling WHERE responsible_institute LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type  FROM Optimising WHERE responsible_institute LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type  FROM Exporting WHERE responsible_institute LIKE ?
            """
            like_param = f"%{institution_str}%"
            params = (like_param, like_param, like_param, like_param, like_param)

            # Fetch the data using pandas
            df = pd.read_sql_query(query, conn, params=params)
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getActivitiesByResponsiblePerson(
        self, responsible_person_str: str
    ) -> pd.DataFrame:  # Ben
        db_file = "json.db"
        try:
            conn = sqlite3.connect(db_file)

            # Use LIKE operator to match partially with the technique string
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type  FROM Acquisition WHERE responsible_person LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type  FROM Processing WHERE responsible_person LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type  FROM Modelling WHERE responsible_person LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type  FROM Optimising WHERE responsible_person LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type  FROM Exporting WHERE responsible_person LIKE ?
            """
            like_param = f"%{responsible_person_str}%"
            params = (like_param, like_param, like_param, like_param, like_param)

            df = pd.read_sql_query(query, conn, params=params)
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getActivitiesUsingTool(self, tool_str: str) -> pd.DataFrame:  # Rubens
        db_file = "json.db"

        try:
            conn = sqlite3.connect(db_file)

            # Use LIKE operator to match partially with the tool string
            query = """
                SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type  FROM Acquisition WHERE tool LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type  FROM Processing WHERE tool LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type  FROM Modelling WHERE tool LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type  FROM Optimising WHERE tool LIKE ?
                UNION
                SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type  FROM Exporting WHERE tool LIKE ?
            """
            like_param = f"%{tool_str}%"
            params = (like_param, like_param, like_param, like_param, like_param)

            df = pd.read_sql_query(query, conn, params=params)
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getActivitiesStartedAfter(self, start_date: str) -> pd.DataFrame:  # Amanda
        db_file = "json.db"

        try:
            conn = sqlite3.connect(db_file)
            query = """
            SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type 
            FROM Acquisition WHERE start_date >= ? 
            UNION 
            SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type 
            FROM Processing WHERE start_date >= ? 
            UNION 
            SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type 
            FROM Modelling WHERE start_date >= ? 
            UNION 
            SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type 
            FROM Optimising WHERE start_date >= ? 
            UNION 
            SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type 
            FROM Exporting WHERE start_date >= ?
            """
            df = pd.read_sql_query(
                query,
                conn,
                params=(start_date, start_date, start_date, start_date, start_date),
            )
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getActivitiesEndedBefore(self, end_date: str) -> pd.DataFrame:  # Amanda
        db_file = "json.db"

        try:
            conn = sqlite3.connect(db_file)

            # Construct the SQL query with NULL columns where necessary
            query = (
                "SELECT object_id, responsible_institute, responsible_person, technique, NULL as tool, start_date, end_date, 'Acquisition' as type FROM Acquisition WHERE end_date <= ? UNION "
                "SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Processing' as type FROM Processing WHERE end_date <= ? UNION "
                "SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Modelling' as type FROM Modelling WHERE end_date <= ? UNION "
                "SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Optimising' as type  FROM Optimising WHERE end_date <= ? UNION "
                "SELECT object_id, responsible_institute, responsible_person, NULL as technique, NULL as tool, start_date, end_date, 'Exporting' as type FROM Exporting WHERE end_date <= ?"
            )

            df = pd.read_sql_query(
                query, conn, params=(end_date, end_date, end_date, end_date, end_date)
            )
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()

    def getAcquisitionsByTechnique(self, technique_str: str) -> pd.DataFrame:  # Rubens
        db_file = "json.db"
        try:
            conn = sqlite3.connect(db_file)

            # Use LIKE operator to match partially with the technique string
            query = f"SELECT * FROM Acquisition WHERE technique LIKE ?"

            # Execute the query and pass the technique_str wrapped with '%' for partial match
            df = pd.read_sql_query(query, conn, params=("%" + technique_str + "%",))

            # Add the type column
            df["type"] = "Acquisition"
            return df

        except sqlite3.Error as e:
            print("SQLite error:", e)
        finally:
            conn.close()


class BasicMashup(object):
    def __init__(
        self,
        metadataQuery: List[MetadataQueryHandler],
        processQuery: List[ProcessDataQueryHandler],
    ) -> None:  # Rubens
        self.metadataQuery = metadataQuery if metadataQuery is not None else []
        self.processQuery = processQuery if processQuery is not None else []

    def cleanMetadataHandlers(self) -> bool:  # Rubens
        self.metadataQuery.clear()
        return True

    def cleanProcessHandlers(self) -> bool:  # Rubens
        self.processQuery.clear()
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:  # Rubens
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:  # Rubens
        self.processQuery.append(handler)
        return True

    def getEntityById(self, id: str) -> IdentifiableEntity | None:  # Rubens
        id_entity: List[Person] = []
        processed_ids = set()  # Set to keep track of processed IDs

        for handler in self.metadataQuery:
            people_df = handler.getById(id)
            for _, row in people_df.iterrows():
                person_id = row["identifier"]
                # Check if the ID has already been processed
                if person_id not in processed_ids:
                    person = Person(id=person_id, name=row["name"])
                id_entity.append(person)
                processed_ids.add(person_id)  # Add the ID to the set of processed IDs

        print("Entity found by Id:")
        for person in id_entity:
            print(
                f"Name: {person.name}, Id: {person.id}, Type: {type(person).__name__}"
            )
        if id_entity == []:
            id_entity = None
            
        return id_entity

    def getAllPeople(self) -> List[Person]:  # Ben/Rubens
        all_people: List[Person] = []
        processed_ids = set()  # Set to keep track of processed IDs

        for handler in self.metadataQuery:
            people_df = handler.getAllPeople()
            for _, row in people_df.iterrows():
                person_id = row["id"]
                # Check if the ID has already been processed
                if person_id not in processed_ids:
                    person = Person(id=person_id, name=row["name"])
                    all_people.append(person)
                    processed_ids.add(
                        person_id
                    )  # Add the ID to the set of processed IDs

        print("Person list created:")
        for person in all_people:
            print(f"Name: {person.name}, Type: {type(person).__name__}")
        return all_people

    def getAllCulturalHeritageObjects(
        self,
    ) -> List[CulturalHeritageObject]:  # Ekaterina
        objects_list = []
        df = pd.DataFrame()

        if len(self.metadataQuery) > 0:
            df = self.metadataQuery[0].getAllCulturalHeritageObjects()
            print("DataFrame returned from SPARQL query:")

        if df.empty:
            print("The DataFrame is empty.")
        else:
            for _, row in df.iterrows():
                id = str(row["id"])
                title = row["title"]
                date = row["date"]
                owner = str(row["owner"])
                place = row["place"]
                author_id = str(row["author_id"])
                author_name = row["author_name"]

                hasAuthor = None
                if author_id and author_name:
                    hasAuthor = [Person(author_id, author_name)]

                try:
                    # Use if-elif-else to instantiate objects based on type name
                    type_name = row["type_name"]
                    if type_name == "NauticalChart":
                        obj = NauticalChart(id, title, date, owner, place, hasAuthor)
                    elif type_name == "ManuscriptPlate":
                        obj = ManuscriptPlate(id, title, date, owner, place, hasAuthor)
                    elif type_name == "ManuscriptVolume":
                        obj = ManuscriptVolume(id, title, date, owner, place, hasAuthor)
                    elif type_name == "PrintedVolume":
                        obj = PrintedVolume(id, title, date, owner, place, hasAuthor)
                    elif type_name == "PrintedMaterial":
                        obj = PrintedMaterial(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Herbarium":
                        obj = Herbarium(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Specimen":
                        obj = Specimen(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Painting":
                        obj = Painting(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Model":
                        obj = Model(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Map":
                        obj = Map(id, title, date, owner, place, hasAuthor)
                    else:
                        print(f"No class defined for type: {type_name}")

                    # Append the instantiated object to the list
                    objects_list.append(obj)
                except ValueError as e:
                    print(f"Error creating CulturalHeritageObject: {e}")

            print("Objects list created:")
            for obj in objects_list:
                print(
                    f"Object ID: {obj.id}, Title: {obj.title}, Type: {obj.__class__.__name__}, hasAuthor: {', '.join([f'{author.name} ({author.id})' for author in obj.hasAuthor]) if obj.hasAuthor else 'None'}"
                )

        return objects_list

    def getAuthorsOfCulturalHeritageObject(
        self, object_id: str
    ) -> List[Person]:  # Ekaterina
        authors_list = []

        for metadata_qh in self.metadataQuery:
            authors_df = metadata_qh.getAuthorsOfCulturalHeritageObject(object_id)
            print(authors_df)

            for _, row in authors_df.iterrows():
                author = Person(row["id"], row["name"])
                authors_list.append(author)

        return authors_list

    def getCulturalHeritageObjectsAuthoredBy(
        self, input_id: str
    ) -> List[CulturalHeritageObject]:  # Ekaterina
        objects_list = []
        df = pd.DataFrame()

        if len(self.metadataQuery) > 0:
            df = self.metadataQuery[0].getCulturalHeritageObjectsAuthoredBy(input_id)
            print("DataFrame returned from SPARQL query:")
            print(df)

        if not df.empty:
            for _, row in df.iterrows():
                id = str(row["id"])
                title = row["title"]
                date = str(row["date"])
                owner = str(row["owner"])
                place = row["place"]
                author_id = str(row["author_id"]) if "author_id" in df.columns else None
                author_name = (
                    row["author_name"] if "author_name" in df.columns else None
                )

                hasAuthor = None
                if author_id and author_name:
                    hasAuthor = [Person(author_id, author_name)]

                try:
                    # Use if-elif-else to instantiate objects based on type name
                    type_name = row["type_name"]
                    if type_name == "NauticalChart":
                        obj = NauticalChart(id, title, date, owner, place, hasAuthor)
                    elif type_name == "ManuscriptPlate":
                        obj = ManuscriptPlate(id, title, date, owner, place, hasAuthor)
                    elif type_name == "ManuscriptVolume":
                        obj = ManuscriptVolume(id, title, date, owner, place, hasAuthor)
                    elif type_name == "PrintedVolume":
                        obj = PrintedVolume(id, title, date, owner, place, hasAuthor)
                    elif type_name == "PrintedMaterial":
                        obj = PrintedMaterial(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Herbarium":
                        obj = Herbarium(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Specimen":
                        obj = Specimen(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Painting":
                        obj = Painting(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Model":
                        obj = Model(id, title, date, owner, place, hasAuthor)
                    elif type_name == "Map":
                        obj = Map(id, title, date, owner, place, hasAuthor)
                    else:
                        print(f"No class defined for type: {type_name}")
                        continue

                    # Append the instantiated object to the list
                    objects_list.append(obj)
                except ValueError as e:
                    print(f"Error creating CulturalHeritageObject: {e}")

            print("Objects list created:")
            for obj in objects_list:
                print(
                    f"Object ID: {obj.id}, Title: {obj.title}, Type: {type(obj).__name__}"
                )

        return objects_list

    def getAllActivities(self) -> List[Activity]:  # Ben/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities()
            print("DataFrame returned from SQL query:")
            print(activities_df)

            for _, row in activities_df.iterrows():
                activity_type = row["type"]
                activity = None  # Initialize activity variable

                # Convert necessary fields to strings
                object_id = str(row["object_id"])
                responsible_institute = str(row["responsible_institute"])
                responsible_person = str(row["responsible_person"])
                tool = str(row["tool"])
                start_date = str(row["start_date"])
                end_date = str(row["end_date"])

                # Create CulturalHeritageObject instance
                cultural_heritage_object = CulturalHeritageObject(
                    object_id, "", "", "", ""
                )

                if activity_type == "Acquisition":
                    technique = str(row["technique"])
                    activity = Acquisition(
                        cultural_heritage_object,
                        responsible_institute,
                        technique,
                        responsible_person,
                        start_date,
                        end_date,
                        tool,
                    )
                elif activity_type == "Processing":
                    activity = Processing(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Modelling":
                    activity = Modelling(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Optimising":
                    activity = Optimising(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Exporting":
                    activity = Exporting(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )

                if activity:
                    all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getActivitiesByResponsibleInstitution(
        self, institute_name: str
    ) -> List[Activity]:  # Ben/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities()
            print("DataFrame returned from SQL query:")
            print(activities_df)

            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for _, row in activities_df.iterrows():
                responsible_institute = str(row["responsible_institute"])

                if institute_name.lower() in responsible_institute.lower():
                    activity_type = row["type"]
                    object_id = str(row["object_id"])
                    responsible_person = str(row["responsible_person"])
                    tool = str(row["tool"])
                    start_date = str(row["start_date"])
                    end_date = str(row["end_date"])

                    cultural_heritage_object = CulturalHeritageObject(
                        object_id, "", "", "", ""
                    )

                    activity = None  # Initialize activity variable
                    if activity_type == "Acquisition":
                        technique = str(row["technique"])
                        activity = Acquisition(
                            cultural_heritage_object,
                            responsible_institute,
                            technique,
                            responsible_person,
                            start_date,
                            end_date,
                            tool,
                        )
                    elif activity_type == "Processing":
                        activity = Processing(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Modelling":
                        activity = Modelling(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Optimising":
                        activity = Optimising(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Exporting":
                        activity = Exporting(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )

                    if activity:
                        all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getActivitiesByResponsiblePerson(
        self, person_name: str
    ) -> List[Activity]:  # Ben/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities()
            print("DataFrame returned from SQL query:")
            print(activities_df)

            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for _, row in activities_df.iterrows():
                responsible_person = str(row["responsible_person"])

                if person_name.lower() in responsible_person.lower():
                    activity_type = row["type"]
                    object_id = str(row["object_id"])
                    responsible_institute = str(row["responsible_institute"])
                    tool = str(row["tool"])
                    start_date = str(row["start_date"])
                    end_date = str(row["end_date"])

                    cultural_heritage_object = CulturalHeritageObject(
                        object_id, "", "", "", ""
                    )

                    activity = None
                    if activity_type == "Acquisition":
                        technique = str(row["technique"])
                        activity = Acquisition(
                            cultural_heritage_object,
                            responsible_institute,
                            technique,
                            responsible_person,
                            start_date,
                            end_date,
                            tool,
                        )
                    elif activity_type == "Processing":
                        activity = Processing(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Modelling":
                        activity = Modelling(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Optimising":
                        activity = Optimising(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Exporting":
                        activity = Exporting(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )

                    if activity:
                        all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getActivitiesUsingTool(self, tool_name: str) -> List[Activity]:  # Ben/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAllActivities()
            print("DataFrame returned from SQL query:")
            print(activities_df)

            # Check if 'type' column exists
            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for _, row in activities_df.iterrows():
                tool = str(row["tool"])

                if tool_name.lower() in tool.lower():
                    activity_type = row["type"]
                    object_id = str(row["object_id"])
                    responsible_person = str(row["responsible_person"])
                    responsible_institute = str(row["responsible_institute"])
                    start_date = str(row["start_date"])
                    end_date = str(row["end_date"])

                    cultural_heritage_object = CulturalHeritageObject(
                        object_id, "", "", "", ""
                    )

                    activity = None
                    if activity_type == "Acquisition":
                        technique = str(row["technique"])
                        activity = Acquisition(
                            cultural_heritage_object,
                            responsible_institute,
                            technique,
                            responsible_person,
                            start_date,
                            end_date,
                            tool,
                        )
                    elif activity_type == "Processing":
                        activity = Processing(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Modelling":
                        activity = Modelling(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Optimising":
                        activity = Optimising(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )
                    elif activity_type == "Exporting":
                        activity = Exporting(
                            cultural_heritage_object,
                            responsible_institute,
                            responsible_person,
                            tool,
                            start_date,
                            end_date,
                        )

                    if activity:
                        all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getActivitiesStartedAfter(
        self, date: str
    ) -> List[Activity]:  # Amanda/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getActivitiesStartedAfter(date)
            print("DataFrame returned from SQL query:")
            print(activities_df)

            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for index, row in activities_df.iterrows():
                activity_type = row["type"]
                object_id = str(row["object_id"])
                tool = str(row["tool"])
                responsible_person = str(row["responsible_person"])
                responsible_institute = str(row["responsible_institute"])
                start_date = str(row["start_date"])
                end_date = str(row["end_date"])

                cultural_heritage_object = CulturalHeritageObject(
                    object_id, "", "", "", ""
                )

                activity = None
                if activity_type == "Acquisition":
                    technique = str(row["technique"])
                    activity = Acquisition(
                        cultural_heritage_object,
                        responsible_institute,
                        technique,
                        responsible_person,
                        start_date,
                        end_date,
                        tool,
                    )
                elif activity_type == "Processing":
                    activity = Processing(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Modelling":
                    activity = Modelling(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Optimising":
                    activity = Optimising(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Exporting":
                    activity = Exporting(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )

                if activity:
                    all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getActivitiesEndedBefore(self, date: str) -> List[Activity]:  # Amanda/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getActivitiesEndedBefore(date)
            print("DataFrame returned from SQL query:")
            print(activities_df)

            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for index, row in activities_df.iterrows():
                activity_type = row["type"]
                object_id = str(row["object_id"])
                tool = str(row["tool"])
                responsible_person = str(row["responsible_person"])
                responsible_institute = str(row["responsible_institute"])
                start_date = str(row["start_date"])
                end_date = str(row["end_date"])

                cultural_heritage_object = CulturalHeritageObject(
                    object_id, "", "", "", ""
                )

                activity = None
                if activity_type == "Acquisition":
                    technique = str(row["technique"])
                    activity = Acquisition(
                        cultural_heritage_object,
                        responsible_institute,
                        technique,
                        responsible_person,
                        start_date,
                        end_date,
                        tool,
                    )
                elif activity_type == "Processing":
                    activity = Processing(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Modelling":
                    activity = Modelling(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Optimising":
                    activity = Optimising(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )
                elif activity_type == "Exporting":
                    activity = Exporting(
                        cultural_heritage_object,
                        responsible_institute,
                        responsible_person,
                        tool,
                        start_date,
                        end_date,
                    )

                if activity:
                    all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities

    def getAcquisitionsByTechnique(self, technique: str):  # Amanda/Ekaterina
        all_activities = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getAcquisitionsByTechnique(technique)
            print("DataFrame returned from SQL query:")
            print(activities_df)

            if "type" not in activities_df.columns:
                print("Warning: 'type' column not found in the DataFrame.")
                return all_activities

            for index, row in activities_df.iterrows():
                activity_type = row["type"]
                object_id = str(row["object_id"])
                tool = str(row["tool"])
                responsible_person = str(row["responsible_person"])
                responsible_institute = str(row["responsible_institute"])
                start_date = str(row["start_date"])
                end_date = str(row["end_date"])

                cultural_heritage_object = CulturalHeritageObject(
                    object_id, "", "", "", ""
                )

                activity = None
                if activity_type == "Acquisition":
                    technique = str(row["technique"])
                    activity = Acquisition(
                        cultural_heritage_object,
                        responsible_institute,
                        technique,
                        responsible_person,
                        start_date,
                        end_date,
                        tool,
                    )

                if activity:
                    all_activities.append(activity)

            print("Activities list created:")
            for activity in all_activities:
                print(
                    f"Activity Type: {type(activity).__name__}, "
                    f"Responsible Institute: {activity.institute}, "
                    f"Responsible Person: {activity.person}, "
                    f"Tool: {activity.tool}, "
                    f"Start Date: {activity.start}, "
                    f"End Date: {activity.end}"
                )

        return all_activities


class AdvancedMashup(BasicMashup):
    def __init__(self, metadataQuery=None, processQuery=None):
        super().__init__(metadataQuery, processQuery)
        
    def getActivitiesOnObjectsAuthoredBy(
        self, author_id: str
    ) -> list[Activity]:  # Rubens
        related_cultural_heritage_objects = self.metadataQuery[
            0
        ].getCulturalHeritageObjectsAuthoredBy(author_id)

        related_ids = set(related_cultural_heritage_objects["id"])
        print("Related IDs:", related_ids)
        related_ids_str = {str(id) for id in related_ids}

        all_activities = self.processQuery[0].getAllActivities()
        # print(all_activities)

        # Convert object_id column to string (if not already)

        all_activities["object_id"] = all_activities["object_id"].astype(str)
        selected_rows = all_activities[
            all_activities["object_id"].isin(related_ids_str)
        ]
        
        # Convert selected_rows to a list of Activity objects
        activities_list = [Activity(row) for index, row in selected_rows.iterrows()]
        
        return activities_list

    def getObjectsHandledByResponsiblePerson(
        self, responsible_person: str
    ) -> List[CulturalHeritageObject]:  # Ekaterina
        all_objects = []
        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getActivitiesByResponsiblePerson(
                responsible_person
            )

            if len(self.metadataQuery) > 0:
                objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()

                object_ids = []
                for index, row in activities_df.iterrows():
                    activity_id = row["object_id"]
                    if activity_id not in object_ids:
                        object_ids.append(activity_id)
                        object_data = objects_df[objects_df["id"] == activity_id].iloc[
                            0
                        ]
                        object_type = object_data["type_name"]
                        if object_type == "NauticalChart":
                            obj = NauticalChart(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "ManuscriptPlate":
                            obj = ManuscriptPlate(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "ManuscriptVolume":
                            obj = ManuscriptVolume(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "PrintedVolume":
                            obj = PrintedVolume(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "PrintedMaterial":
                            obj = PrintedMaterial(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Herbarium":
                            obj = Herbarium(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Specimen":
                            obj = Specimen(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Painting":
                            obj = Painting(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Model":
                            obj = Model(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=str(object_data["author_id"]),
                                author_name=object_data["author_name"],
                            )
                        elif object_type == "Model":
                            obj = Model(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Map":
                            obj = Map(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        else:
                            print(f"No class defined for type: {object_type}")
                            continue

                    all_objects.append(obj)

        print("Cultural Heritage Objects list created:")
        for obj in all_objects:
            print(
                f"Object ID: {obj.id}, Title: {obj.title}, Type: {type(obj).__name__}"
            )

        return all_objects

    def getObjectsHandledByResponsibleInstitution(
        self, institute_name: str
    ) -> List[CulturalHeritageObject]:  # Ekaterina
        all_objects = []
        activities_df = pd.DataFrame()

        if len(self.processQuery) > 0:
            activities_df = self.processQuery[0].getActivitiesByResponsibleInstitution(
                institute_name
            )

            if len(self.metadataQuery) > 0:
                objects_df = self.metadataQuery[0].getAllCulturalHeritageObjects()

                object_ids = []
                for _, row in activities_df.iterrows():
                    activity_id = row["object_id"]
                    if activity_id not in object_ids:
                        object_ids.append(activity_id)
                        object_data = objects_df[objects_df["id"] == activity_id].iloc[
                            0
                        ]
                        object_type = object_data["type_name"]
                        if object_type == "NauticalChart":
                            obj = NauticalChart(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "ManuscriptPlate":
                            obj = ManuscriptPlate(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "ManuscriptVolume":
                            obj = ManuscriptVolume(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "PrintedVolume":
                            obj = PrintedVolume(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "PrintedMaterial":
                            obj = PrintedMaterial(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Herbarium":
                            obj = Herbarium(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Specimen":
                            obj = Specimen(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Painting":
                            obj = Painting(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Model":
                            obj = Model(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=str(object_data["author_id"]),
                                author_name=object_data["author_name"],
                            )
                        elif object_type == "Model":
                            obj = Model(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        elif object_type == "Map":
                            obj = Map(
                                id=str(object_data["id"]),
                                title=object_data["title"],
                                date=str(object_data["date"]),
                                owner=str(object_data["owner"]),
                                place=object_data["place"],
                                author_id=(
                                    str(object_data["author_id"])
                                    if "author_id" in objects_df.columns
                                    else None
                                ),
                                author_name=(
                                    object_data["author_name"]
                                    if "author_name" in objects_df.columns
                                    else None
                                ),
                            )
                        else:
                            print(f"No class defined for type: {object_type}")
                            continue

                        all_objects.append(obj)

        print("Cultural Heritage Objects list created:")
        for obj in all_objects:
            print(
                f"Object ID: {obj.id}, Title: {obj.title}, Type: {type(obj).__name__}"
            )

        return all_objects

    def getAuthorsOfObjectsAcquiredInTimeFrame(
        self, start_date: str, end_date: str
    ) -> list[Person]:  # Rubens
        acquired_authors = []

        activities_started = self.processQuery[0].getActivitiesStartedAfter(start_date)
        started_ids = set(
            activities_started[activities_started["type"] == "Acquisition"]["object_id"]
        )

        activities_ended = self.processQuery[0].getActivitiesEndedBefore(end_date)
        ended_ids = set(
            activities_ended[activities_ended["type"] == "Exporting"]["object_id"]
        )

        common_ids = started_ids.intersection(ended_ids)
        common_ids_int = {int(id) for id in common_ids}
        print("IDs of this timeframe:", common_ids_int)

        for item in common_ids_int:
            authors = self.getAuthorsOfCulturalHeritageObject(item)
            acquired_authors.extend(authors)

        return acquired_authors
    


# -*- coding: utf-8 -*-
# Copyright (c) 2023, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.
import unittest
from os import sep
from pandas import DataFrame
from impl import MetadataUploadHandler, ProcessDataUploadHandler
from impl import MetadataQueryHandler, ProcessDataQueryHandler
from impl import AdvancedMashup
from impl import Person, CulturalHeritageObject, Activity, Acquisition

# REMEMBER: before launching the tests, please run the Blazegraph instance!
# 
# Run command:
#   `java -server -Xmx4g -jar blazegraph.jar`

class TestProjectBasic(unittest.TestCase):

    # The paths of the files used in the test should change depending on what you want to use
    # and the folder where they are. Instead, for the graph database, the URL to talk with
    # the SPARQL endpoint must be updated depending on how you launch it - currently, it is
    # specified the URL introduced during the course, which is the one used for a standard
    # launch of the database.
    metadata = "data" + sep + "meta.csv"
    process = "data" + sep + "process.json"
    relational = "." + sep + "relational.db"
    graph = "http://127.0.0.1:9999/blazegraph/sparql"
    
    # works
    def test_01_MetadataUploadHandler(self):
        u = MetadataUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.graph))
        self.assertEqual(u.getDbPathOrUrl(), self.graph)
        self.assertTrue(u.pushDataToDb(self.metadata))

    # works
    def test_02_ProcessDataUploadHandler(self):
        u = ProcessDataUploadHandler()
        self.assertTrue(u.setDbPathOrUrl(self.relational))
        self.assertEqual(u.getDbPathOrUrl(), self.relational)
        self.assertTrue(u.pushDataToDb(self.process))
    
    # works
    def test_03_MetadataQueryHandler(self):
        q = MetadataQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.graph))
        self.assertEqual(q.getDbPathOrUrl(), self.graph)

        self.assertIsInstance(q.getById("just_a_test"), DataFrame)

        self.assertIsInstance(q.getAllPeople(), DataFrame)
        self.assertIsInstance(q.getAllCulturalHeritageObjects(), DataFrame)
        self.assertIsInstance(q.getAuthorsOfCulturalHeritageObject("just_a_test"), DataFrame)
        self.assertIsInstance(q.getCulturalHeritageObjectsAuthoredBy(
            "just_a_test"), DataFrame)
    
    def test_04_ProcessDataQueryHandler(self):
        q = ProcessDataQueryHandler()
        self.assertTrue(q.setDbPathOrUrl(self.graph))
        self.assertEqual(q.getDbPathOrUrl(), self.graph)

        self.assertIsInstance(q.getById("just_a_test"), DataFrame)

        self.assertIsInstance(q.getAllActivities(), DataFrame)
        self.assertIsInstance(q.getActivitiesByResponsibleInstitution(
            "just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesByResponsiblePerson("just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesUsingTool("just_a_test"), DataFrame)
        self.assertIsInstance(q.getActivitiesStartedAfter("1088-01-01"), DataFrame)
        self.assertIsInstance(q.getActivitiesEndedBefore("2029-01-01"), DataFrame)
        self.assertIsInstance(q.getAcquisitionsByTechnique("just_a_test"), DataFrame)
        
    def test_05_AdvancedMashup(self):
        qm = MetadataQueryHandler()
        qm.setDbPathOrUrl(self.graph)
        qp = ProcessDataQueryHandler()
        qp.setDbPathOrUrl(self.relational)

        am = AdvancedMashup()
        self.assertIsInstance(am.cleanMetadataHandlers(), bool)
        self.assertIsInstance(am.cleanProcessHandlers(), bool)
        self.assertTrue(am.addMetadataHandler(qm))
        self.assertTrue(am.addProcessHandler(qp))
        self.assertEqual(am.getEntityById("just_a_test"), None)

        r = am.getAllPeople()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)

        r = am.getAllCulturalHeritageObjects()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAuthorsOfCulturalHeritageObject("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)

        r = am.getCulturalHeritageObjectsAuthoredBy("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAllActivities()
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesByResponsibleInstitution("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesByResponsiblePerson("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesUsingTool("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesStartedAfter("1088-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getActivitiesEndedBefore("2029-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)

        r = am.getAcquisitionsByTechnique("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Acquisition)

        # BUG Test fails here!!!
        r = am.getActivitiesOnObjectsAuthoredBy("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Activity)
        
        r = am.getObjectsHandledByResponsiblePerson("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)
        
        r = am.getObjectsHandledByResponsibleInstitution("just_a_test")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, CulturalHeritageObject)

        r = am.getAuthorsOfObjectsAcquiredInTimeFrame("1088-01-01", "2029-01-01")
        self.assertIsInstance(r, list)
        for i in r:
            self.assertIsInstance(i, Person)

# Running tests:
if __name__ == '__main__':
    testProject = TestProjectBasic()
    print("\n\n#####################################")
    print("## TESTING PROJECT 'impl.py' FILE: ##")
    print("#####################################\n")
    print("\n============================\n")
    print('Test 1 MetadataUploadHandler...\n')
    testProject.test_01_MetadataUploadHandler()
    print("\n============================\n")
    print('Test 2 ProcessDataUploadHandler...\n')
    testProject.test_02_ProcessDataUploadHandler()
    print("\n============================\n")
    print('Test 3 MetadataQueryHandler...\n')
    testProject.test_03_MetadataQueryHandler()
    print("\n============================\n")
    print('Test 4 ProcessDataQueryHandler...\n')
    testProject.test_04_ProcessDataQueryHandler()
    print("\n============================\n")
    print('Test 5 AdvancedMashup...\n')
    testProject.test_05_AdvancedMashup()
    
    unittest.main()


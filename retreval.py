import chromadb
from sentence_transformers import SentenceTransformer
from google import genai
import ast
import mysql_connect
import requests
import os
from dotenv import load_dotenv
load_dotenv()
api = genai.Client(api_key=os.getenv("genai_API_KEY"))
client = chromadb.PersistentClient(path=os.getenv("VECTOR_DB_PATH", "chroma_db"))
collection = client.get_collection(name=os.getenv("VECTOR_COLLECTION_NAME", "fossil_fuel_emissions"))

model = SentenceTransformer(os.getenv("VECTOR_EMBEDDING_MODEL", 'all-MiniLM-L6-v2'))

def retrieve(query):
    response = api.models.generate_content(
        model="gemini-2.5-flash",
        contents = [
        f"{query}\n",
        "Your name is patuta, An climate analyser especially in fossil fuel emission developed by Team patutaas",
        "Never say what type of data you have like it is stored in sql etc just say sources",
        "Give the answer in a consised manner untill the user asks for more details.",
        "Follow this principle summarise,concise,human readable and bullets points if required"
        """Return the output as a list of two elements:
            1. The first element must be either "true" or "false" or "neutral" or "graph":
            - "true" if the query is descriptive or semantic and should be answered by RAG.
            - "false" if the query requires numerical analysis or computation (e.g., highest, lowest, difference, growth, trend) and should be answered via SQL.
            - "graph" if the query requires a graph to be plotted or something like graph,bar diagram or something then it is this.
            - "neutral" if general questions like hi,bye,etc give a proper response if it goes beyond the context then say you dont understand .
            2. The second element must be:
            - If the first element is "true": rewrite the query it should be relevant to the question which the user asked.
            - If the first element is "false": generate the corresponding SQL query to retrieve the required numerical data from MySQL,The table name will be fossil_fuel it has 6 attributes (year,emissions,longitude,latitude,change,pct_change).
            - If the first element is "graph": generate the corresponding SQL query to retrieve the required numerical data from MySQL,The table name will be fossil_fuel it has 6 attributes (year,emissions,longitude,latitude,change,pct_change).
            Give only in list format like this ["true","****"] or ["false","****"]  or ["neutral","****"]
            """
    ]

    )
    query_embedding = model.encode(query).tolist()
    def clean_response(text):
    # remove markdown fences like json ...

        return text.strip().removeprefix("json").removesuffix("").strip()
    r = clean_response(response.text)
    m = ast.literal_eval(r)
# print(m[0])
    if m[0] == "true":
        # print(m[0])
        ai_response_embedding = model.encode(m[1]).tolist()
        # some_query_embedding = model.encode("Always give the latest data").tolist()

        restults = collection.query(
        query_embeddings=[query_embedding, ai_response_embedding],
        n_results=10)

        res = api.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
            f"{query}\n",
            f"{m[1]}\n",
            f"{restults}",
            f"Your name is patuta, An climate analyser especially in fossil fuel emission developed by Team patutaas",
            f"Never say what type of data you have like it is stored in sql etc just say sources",
            f"Give the answer in a consised manner untill the user asks for more details.If the answer is big then give it in understandable bullet point manner"
            f"Analyse properly and give me the correct final answer for the query",
            f"Follow this principle summarise,concise ,human readable and bullets points if required",
            f"Speak like a human not like a robot",
            f"Give the answer in a consised manner untill the user asks for more details",
            f"Give it as a proper pragraph.Do not specify anyhthing",
            f"If you are not dont have the answer to the query then analyse with the data you if u get a solution return it or else tell No data provided.But never halosinate things"
        ])
        
        return res.text.replace("*", "")
    elif m[0] == "false":
        # print(m[0])
        sql_query = m[1]
        print(sql_query)
        data = mysql_connect.getdata(sql_query)

        system_prompt = "Your name is patuta, An climate analyser especially in fossil fuel emission developed by Team patutaas.Never say what type of data you have like it is stored in sql etc just say sources.With the data provided,analyse the data and give me a proper response in a human readable format.Never say that it is a single data point or a single value.Follow this principle summarise,concise,human readable and bullets points if required"
        res = api.models.generate_content(
            model="gemini-2.5-flash",
            contents=[str(query) + str(system_prompt) + str(data)]
        )
        return res.text.replace("*", "")
    elif m[0] == "graph":
        sql_query = m[1]
        data = mysql_connect.getdata(sql_query)
        # print(data)

        # print(response.json())  # see API reply
        print(data)
        return data

    else:
        return m[1].replace("*", "")
    

# query_s = input("Enter your query: ")
# while query_s != "exit":
#     print(retrieve(query_s))
#     query_s = input("Enter your query: ")
# l = "which year had the highest emissions of fossil fuel CO2?"

# print(retrieve(l))
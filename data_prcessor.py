import xarray as xr
import pandas as pd
import glob
import re
from sentence_transformers import SentenceTransformer
import chromadb
import mysql_connect
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
database = os.getenv("DB_NAME")
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
vector_db_path = os.getenv("VECTOR_DB_PATH", "chroma_db")
client = chromadb.PersistentClient(path=f"{vector_db_path}")
vector_collection_name = os.getenv("VECTOR_COLLECTION_NAME", "fossil_fuel_emissions")
collection = client.create_collection(name=f"{vector_collection_name}")
# Pick up all NetCDF files in the folder (any name)
files = sorted(glob.glob("data/fossil_fuel_emision/*.nc"))
text_files = sorted(glob.glob("data/fossil_fuel_emision/*.txt"))

records = []

for f in files:
    ds = xr.open_dataset(f)
    
    print(ds)
    # yearly global total (sum over space + months)
    yearly_total = (ds['land'] + ds['intl_bunker']).sum(dim=["lat", "lon", "month"])
    # print(yearly_total.values)
    # --- Extract year ---
    year = None
    # Try filename first
    match = re.search(r"\d{4}", f)
    year = int(match.group())
    
    lon = ds['lon'].values
    lat = ds['lat'].values
    r_lon = f"{lon.min()}-{lon.max()}"
    r_lat = f"{lat.min()}-{lat.max()}"

    print(f"File: {f}, Year: {year}, Emissions: {float(yearly_total.values)},lon:{r_lon},lat:{r_lat}")
    if year:
        records.append((year, float(yearly_total.values),r_lon,r_lat))

# Put into DataFrame
df = pd.DataFrame(records, columns=["year", "emissions","longitude","latitude"]).sort_values("year")

# Add yearly change and percent change
if not df.empty:
    df["change"] = df["emissions"].diff()
    df["pct_change"] = df["emissions"].pct_change() * 100

    change_value = df["change"].iloc[-1]
    pct_change_value = df["pct_change"].iloc[-1]

    df.to_sql(
        name='fossil_fuel',
        con=engine,
        if_exists='replace',
        index=False,
    )

    # mysql_connect.insertion(year, float(yearly_total.values), r_lon, r_lat, change_value, pct_change_value)
    # Save to CSV
    # df.to_csv("yearly_emissions.csv", index=False)

    # print("\nFinal Data:")
    # print(df)


    documents = [f"The global fossil fuel CO₂ emissions datafrom the year of {df['year'].min()} till {df['year'].max()}:"]

    for i in range(len(df)):
        documents.append(f"year = {df.iloc[i,0]},emissions = {df.iloc[i,1]},change = {df.iloc[i,2]},percentage change {df.iloc[i,3]}")

    # print(documents)


    model = SentenceTransformer("all-MiniLM-L6-v2")

    embedding = model.encode(documents).tolist()
        # Insert into ChromaDB
    collection.add(
        documents=documents,
        embeddings=embedding,
        ids=[f"doc_{i}" for i in range(len(documents))]
    )
    print("\n✅ Data inserted into ChromaDB successfully.")

for tf in text_files:
    with open(tf, 'r', encoding='utf-8') as file:
        content = file.read()
        documents = [content]
        embedding = model.encode(documents).tolist()
        collection.add(
            documents=documents,    
            embeddings=embedding,
            ids=[f"text_{tf}"]
        )
    print(f"\n✅ Text data from {tf} inserted into ChromaDB successfully.")
else:
    print("\n⚠️ No valid years found in files. Please check filenames or attributes.")
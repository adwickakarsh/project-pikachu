from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import io

def upload_full_pokemon_data():
    all_pokemon = []
    base_url = "https://pokeapi.co/api/v2/pokemon?limit=1000"
    response = requests.get(base_url)
    response.raise_for_status()
    pokemon_list = response.json()["results"]

    for poke in pokemon_list:
        details = requests.get(poke["url"]).json()
        types = [t["type"]["name"] for t in details["types"]]
        abilities = [a["ability"]["name"] for a in details["abilities"]]
        stats = {s["stat"]["name"]: s["base_stat"] for s in details["stats"]}

        all_pokemon.append({
            "id": details["id"],
            "name": details["name"],
            "types": ",".join(types),
            "abilities": ",".join(abilities),
            "height": details["height"],
            "weight": details["weight"],
            "base_experience": details["base_experience"],
            "hp": stats.get("hp"),
            "attack": stats.get("attack"),
            "defense": stats.get("defense"),
            "special-attack": stats.get("special-attack"),
            "special-defense": stats.get("special-defense"),
            "speed": stats.get("speed")
        })

    df = pd.DataFrame(all_pokemon)

    account_name = "dataprojectpikachu"
    account_key = "<Your-Account-Key>"
    container_name = "bronze"

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )

    service_client = DataLakeServiceClient.from_connection_string(conn_str)
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    file_name = f"pokemon_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    file_client = file_system_client.create_file(file_name)
    data = csv_buffer.getvalue()
    file_client.append_data(data, offset=0, length=len(data))
    file_client.flush_data(len(data))

    print(f"✅ Uploaded {file_name} with {len(df)} Pokémon records to Azure Data Lake container '{container_name}'")

with DAG(
    "poke_full_to_azure",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pokemon", "azure", "databricks"]
) as dag:

    upload_task = PythonOperator(
        task_id="upload_full_pokemon_data",
        python_callable=upload_full_pokemon_data
    )

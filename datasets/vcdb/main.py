from datasets.vcdb.dataframes import *
from datasets.vcdb.helpers import write_dataframe
from datasets.vcdb.merge import run_merge

print("Processando dataframes... ", end="")

dataframes = {
    "action_notes": get_action_notes(),
    "action_varieties": get_action_varieties(),
    "actors_varieties": get_actor_varieties(),
    "assets_varieties": get_assets_varieties(),
    "location": get_location(),
    "timeline": get_timeline()
}

print("done!")

for name, data in dataframes.items():
    print(f"Escrevendo {name}")
    write_dataframe(data, name)

run_merge()


from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
import requests as rq
import json
import pandas as pd

def get_pokemon_details(df):
    for index, row in df.iterrows():
        detail_url = row['url']
        detail_response = rq.get(detail_url)
        detail_data = detail_response.json()

        sprites = detail_data['sprites']
        image_urls = [sprites['front_default'], sprites['back_default'],
                  sprites['front_shiny'], sprites['back_shiny']]
        
        # handling null base-experience as its causing errors
        base_experience = detail_data.get('base_experience', 0)
        if base_experience is None:
            base_experience = 0

        yield {
            "id": detail_data['id'],
            "name": detail_data['name'],
            "base_experience": base_experience,
            "pokemon_order": detail_data['order'],
            "height": detail_data['height'],
            "weight": detail_data['weight'],
            "sprite_front_default": image_urls[0],
            "sprite_back_default": image_urls[1],
            "sprite_front_shiny": image_urls[2],
            "sprite_back_shiny": image_urls[3],
            # Add any additional fields you want to extract
        }

def schema(configuration: dict):
    return [
        {
            "table": "pokemon",
            "primary_key": ["name"],
        },
    ]

def update(configuration: dict, state: dict):

    # Get pokemon data
    url = "https://pokeapi.co/api/v2/pokemon/"
    all_results = []
    counter = 0
    max_pages = None

    while url and (max_pages is None or counter < max_pages):
        response = rq.get(url)
        data = response.json()
        #print(json.dumps(response.json(), indent=4))
        
        all_results.extend(data['results'])
        url = data['next']
        
        counter += 1
        # Print a '.' every 5 loops
        if counter % 5 == 0:
            print('.', end='', flush=True)

    df = pd.DataFrame(all_results)
    print("\nFetched {} rows of Pokémon".format(df.shape[0])) 
    #print(df)

    # Use the generator function to deliver data to Fivetran
    # to trouble shoot only some pokemon df.iloc[start_row:end_row]
    for data in get_pokemon_details(df):
        yield op.upsert(table="pokemon", data=data)

connector = Connector(update=update)

if __name__ == "__main__":
    connector.debug()
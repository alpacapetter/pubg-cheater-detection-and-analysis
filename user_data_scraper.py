# from banned_user_list_data_cleaning import banned_user_list
from datetime import datetime
import json
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def extractData(soup_json):
    '''Returns multiple rows of data from given json soup'''
    game_num = 0
    vals = []
    rows = []
    for i in range(20):
        game_num = i
        try:
            vals.append(soup_json['matches']['items'][game_num]['type'])
            vals.append(soup_json['matches']['items'][game_num]['mode'])
            vals.append(datetime.strptime(soup_json['matches']['items'][game_num]['started_at'], '%Y-%m-%dT%H:%M:%S+0000'))
            vals.append(soup_json['matches']['items'][game_num]['total_rank'])
            vals.append(soup_json['matches']['items'][game_num]['queue_size'])
            vals.append(soup_json['matches']['items'][game_num]['map_name'])
            vals.append(soup_json['matches']['items'][game_num]['match_type'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['user']['nickname'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['rank'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['heals'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['boosts'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['death_type'])
            vals.append(int(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['time_survived']))
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['kda']['kills'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['kda']['assists'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['kda']['kill_steaks'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['kda']['headshot_kills'])
            vals.append(int(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['kda']['longest_kill']))
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['damage']['damage_dealt'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['dbno']['knock_downs'])
            vals.append(soup_json['matches']['items'][game_num]['participant']['stats']['combat']['dbno']['revives'])
            rows.append(vals)
            vals = []
        except IndexError:
            break
    return rows

def extractMoreData(df, soup_json):
    '''Continue to get data and append to original df'''
    counter = 1
    while True:
        try:
            counter += 1
            if counter == 11:
                break
            print(f'ID:{user} current page:{counter}')
            time.sleep(1)
            
            # Find new url with more data using "offset" key
            offset = soup_json['matches']['items'][19]['offset']
            url = f'https://pubg.op.gg/api/users/{data_user_id}/matches/recent?after={offset}'
            result = requests.get(url, timeout=10)
            soup = BeautifulSoup(result.text, 'html.parser')
            soup_json = json.loads(soup.text)

            # Append the new row (new df) into the original df
            rows = extractData(soup_json)
            new_row = spark.createDataFrame(rows, columns)
            df = df.union(new_row)
        except IndexError:
            break
    return df

def dataUserIdFinder(user):
    # Extract data-user_id from opgg
    url = "https://pubg.op.gg/user/" + user
    result = requests.get(url)
    soup = BeautifulSoup(result.text, 'html.parser')
    player_summary = soup.find("div", {"class": "player-summary__name"})
    data_user_id = player_summary.get('data-user_id')
    return data_user_id


spark = SparkSession.builder.appName('pubg_data').getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

df = None
banned_user_list = ["txQQ-2158195697", "watermeloong"]
progress_counter = 0

for user in banned_user_list:
    # Find user data id
    data_user_id = dataUserIdFinder(user)

    # Navigate to initial json data page
    url = f'https://pubg.op.gg/api/users/{data_user_id}/matches/recent'
    result = requests.get(url)
    soup = BeautifulSoup(result.text, 'html.parser')
    soup_json = json.loads(soup.text)

    # Create df and insert initial data
    if df == None:
        rows = extractData(soup_json)
        columns = ['type','mode','started_at',
                'total_rank','queue_size','map_name',
                'match_type','nickname','rank',
                'heals','boosts','death_type',
                'time_survived','kills','assists',
                'kill_steaks','headshot_kills','longest_kill',
                'damage_dealt','knock_downs','revives']
        df = spark.createDataFrame(rows, columns)
    else:
        # Append the new row (new df) into the original df
        rows = extractData(soup_json)
        new_row = spark.createDataFrame(rows, columns)
        df = df.union(new_row)

    # Continue to get more data
    df = extractMoreData(df, soup_json)
    progress_counter += 1
    print(progress_counter / len(banned_user_list) * 100, " percent done")

# Save data
df.write.parquet("./cheater_data.parquet", mode='overwrite')
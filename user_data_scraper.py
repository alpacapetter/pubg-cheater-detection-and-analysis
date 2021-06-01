from banned_user_list_data_cleaning import banned_user_list
from datetime import datetime
import json
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def extractData(soup_json):
    '''Returns multiple rows of data from given json soup'''

    def checkKey(soup_json_nest, key):
        '''Returns None if key doesn't exist in json soup'''
        if key in soup_json_nest:
            return soup_json_nest[key]
        else:
            return None

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
            vals.append(checkKey(soup_json['matches']['items'][game_num], 'map_name'))
            vals.append(checkKey(soup_json['matches']['items'][game_num], 'match_type'))
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
            # time.sleep(.1)
            
            # Find new url with more data using "offset" key
            offset = soup_json['matches']['items'][19]['offset']
            url = f'https://pubg.op.gg/api/users/{data_user_id}/matches/recent?after={offset}'
            result = requests.get(url, timeout=10)
            soup = BeautifulSoup(result.text, 'html.parser')
            soup_json = json.loads(soup.text)

            # Append the new row (new df) into the original df
            rows = extractData(soup_json)
            new_row = spark.createDataFrame(rows, schema=schema)
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


# Start Spark Session and df
spark = SparkSession.builder.appName('pubg_data').getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

df = None
progress_counter = 0
counter = 1

while len(banned_user_list) != 0:
    user = banned_user_list.pop()

    # Find user data id
    try:
        data_user_id = dataUserIdFinder(user)
    except:
        continue

    # Navigate to initial json data page
    url = f'https://pubg.op.gg/api/users/{data_user_id}/matches/recent'
    result = requests.get(url)
    soup = BeautifulSoup(result.text, 'html.parser')
    soup_json = json.loads(soup.text)

    # Create df and insert initial data
    if df == None:
        rows = extractData(soup_json)

        schema = StructType(
            [
                StructField("type", StringType(), True), 
                StructField("mode", StringType(), True), 
                StructField("started_at", TimestampType(), True), 
                StructField("total_rank", IntegerType(), True), 
                StructField("queue_size", IntegerType(), True), 
                StructField("map_name", StringType(), True), 
                StructField("match_type", StringType(), True), 
                StructField("nickname", StringType(), True), 
                StructField("rank", IntegerType(), True), 
                StructField("heals", IntegerType(), True), 
                StructField("boosts", IntegerType(), True), 
                StructField("death_type", StringType(), True), 
                StructField("time_survived", IntegerType(), True), 
                StructField("kills", IntegerType(), True), 
                StructField("assists", IntegerType(), True), 
                StructField("kill_steaks", IntegerType(), True), 
                StructField("headshot_kills", IntegerType(), True), 
                StructField("longest_kill", IntegerType(), True), 
                StructField("damage_dealt", IntegerType(), True), 
                StructField("knock_downs", IntegerType(), True), 
                StructField("revives", IntegerType(), True)
            ])
        df = spark.createDataFrame(rows, schema=schema)
    else:
        # Append the new row (new df) into the original df
        rows = extractData(soup_json)
        new_row = spark.createDataFrame(rows, schema=schema)
        df = df.union(new_row)

    # Continue to get more data
    df = extractMoreData(df, soup_json)
    progress_counter += 1

    # Progress checker
    print(progress_counter / len(banned_user_list) * 100, "percent done")

    # Save data (ever 50 player)
    if counter == 50:
        df.write.parquet(f"./cheater_data/cheater_data_{str(progress_counter)}.parquet", mode='overwrite')
        counter = 0

    counter += 1
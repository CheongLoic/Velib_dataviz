import pandas as pd
import requests
import sys


def get_station_status():
    api = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    response = requests.get(api)

    if (response.status_code == 200):
        print("API station_status is OK")
        #print(response.json())
        station_status_data = pd.DataFrame(response.json()["data"]["stations"])
        #print(station_status_data)
        #print(station_status_data.columns)
        station_status_data = station_status_data.astype({'is_installed':bool, 'is_returning':bool, 'is_renting':bool, 'stationCode':int,'last_reported':'datetime64[s]'})

        num_bikes_available_types = station_status_data["num_bikes_available_types"].apply(lambda l: pd.Series({**l[0], **l[1]}))

        station_status_data =  pd.concat([station_status_data, num_bikes_available_types], axis=1)
        station_status_data.drop(columns=['numBikesAvailable', 'numDocksAvailable', 'num_bikes_available_types'], inplace = True)
        station_status_data.rename({
        "mechanical": "mechanical_bike_available",
        "ebike": "electrical_bike_available",
        "last_reported": "time",
    }, axis=1, inplace=True)
        print("station_status data fetched")
        return station_status_data
    else:
        print("Something went wrong with velib station status API ")
        sys.exit(1)



def get_station_info():
    api = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
    response = requests.get(api)

    if (response.status_code == 200):
        print("API station_information is OK")
        #print(response.json())
        df = pd.DataFrame(response.json()["data"]["stations"])
        df = df.astype({'stationCode':int})
        df['credit_card'] = df.rental_methods.str.contains('CREDITCARD', regex=False)
        df.drop(columns=['rental_methods'], inplace = True)
        df.rename({
                "lat": "latitude",
                "lon": "longitude",
                "name": "station_name",
            }, axis=1, inplace=True)
        print("station_information fetched")
        return df
    else:
        print("Something went wrong with velib station info API ")
        sys.exit(1)

def fetch_velib_data():
    station_info = get_station_info()
    station_status = get_station_status()
    df = station_status.merge(station_info, on=["station_id","stationCode"])
    df.to_csv('historique_stations.csv', header=True, index=False)

fetch_velib_data()

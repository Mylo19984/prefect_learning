import requests
from prefect import flow, task
import pandas as pd


BASE_URL = 'https://fantasy.premierleague.com/api/'
url_path = 'bootstrap-static/'

@flow(log_prints=True)
def wrangle_data(ply_name: str) -> None:
    '''
    Prints the name, total points, and current cost of a specified player from the Fantasy Premier League.

    Args:
        ply_name (str): The web_name of the player to retrieve data for (e.g., "M.Salah").

    Notes:
        If the player is not found, no data will be printed.
    '''
    data = pull_players_data(url=url_path)
    player_data = find_player(name=ply_name, data=data)

    if player_data:
        print(f'players is {player_data['web_name']}, and his total points are {player_data['total_points']}, and total cost is {player_data['now_cost']}')
    else:
        print(f'player {ply_name}, not found')

    save_data_parquet(data=data)
    no_weeks = 10
    pull_all_weeks(no_weeks=no_weeks)

def pull_weeks_data(url: str) -> dict:
    '''
    Retrieves data for one week from the Fantasy Premier League API.

    Args:
        url (str): The path to the API endpoint relative to the base URL (e.g., 'bootstrap-static/').

    Returns:
        dict: A dictionary containing the JSON response from the API if successful.
        None: If the request fails.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
    '''
    full_url = BASE_URL + url
    try:
        response = requests.get(full_url)
        response.raise_for_status()
        print('Data fetched successfully!')
        data = response.json()
        print(data)
        return data
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch data: {e}')    
        return None

@task
def pull_all_weeks(no_weeks: int) -> dict:
    '''
    Retrieves data for all weeks from the Fantasy Premier League API.

    Args:
        no_weeks (int): Number to what weeks user wants to pull the data.

    Returns:

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
    '''

    for week in range(no_weeks):
        url_path = f'event/{no_weeks}/live/'
        data = pull_weeks_data(url_path)
        print(data['elements'])

@task
def pull_players_data(url: str) -> dict:
    '''
    Retrieves all player data from the Fantasy Premier League API.

    Args:
        url (str): The path to the API endpoint relative to the base URL (e.g., 'bootstrap-static/').

    Returns:
        dict: A dictionary containing the JSON response from the API if successful.
        None: If the request fails.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
    '''
    full_url = BASE_URL + url
    try:
        response = requests.get(full_url)
        response.raise_for_status()
        print('Data fetched successfully!')
        data = response.json()
        #print(data)
        return data
    except requests.exceptions.RequestException as e:
        print(f'Failed to fetch data: {e}')    
        return None

@task
def find_player(name: str, data: dict) -> dict:
    '''
    Searches for a player by their web_name in the Fantasy Premier League API data.

    Args:
        name (str): Name of the player that we want to find.
        data (dict): Fantasy api data.

    Returns:
        dict: A dictionary containing the JSON with data regading the searched player.
        None: If the request fails.
    '''
    try:
        print('find player starts --------------')
        for player in data['elements']:
            if player['web_name'] == name:
                return player
    except KeyError as e:
        print(f'Malformed data: {e}')
    
    return None

@flow(log_prints=True)
def save_data_parquet(data: dict) -> pd.DataFrame:
    '''
    Saves the whole players data to parquet file.

    Args:
        data (dict): Fantasy api data.
    '''
    try:
        df = pd.DataFrame(data['elements'])
        df.to_parquet('players.parquet')
        check_parquet_data(df)

    except KeyError as e:
        print(f'Error in the process: {e}')

@task
def check_parquet_data(df: pd.DataFrame) -> None:
    '''
    Prints basic information regarding df data.

    Args:
        df (pd.DataFrae): data frame with fantasy api platers.
    '''
    try:
        print(f'total number of records is: {len(df)}')
        print(f'columns are: {df.columns}')
        print('preview data')
        print(df.head())

    except KeyError as e:
        print(f'Error in the process: {e}')

        
if __name__ == "__main__":
    name = 'M.Salah'    
    wrangle_data(ply_name=name)

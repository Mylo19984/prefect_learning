import requests
from prefect import flow, task

BASE_URL = 'https://fantasy.premierleague.com/api/'
url_path = 'bootstrap-static/'

@flow(log_prints=True)
def show_data(ply_name):
    data = pull_players_data(url=url_path)
    player_data = find_player(name=ply_name, data=data)

    print(f'players is {player_data['web_name']}, and his total points are {player_data['total_points']}, and total cost is {player_data['now_cost']}')

@task
def pull_players_data(url):
    full_url = BASE_URL + url
    response = requests.get(full_url)
    if response.status_code == 200:
        print('Data fetched successfully!')
        data = response.json()
        #print(data)
    else:
        print('Failed to fetch data')
    
    return data

@task
def find_player(name, data):
    print('find player starts --------------')
    for player in data['elements']:
        if player['web_name'] == name:
            return player
        
if __name__ == "__main__":
    name = 'M.Salah'    
    show_data(ply_name=name)

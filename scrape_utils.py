import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

def get_soup(url):
    headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/39.0.2171.95 Safari/537.36')}
    r = requests.get(url, headers=headers)
    r.encoding = 'unicode-escape'
    return BeautifulSoup(r.content, 'html.parser')

def get_data_from_table(table, data_type, skip_rows):
    """Helper method to get the data from a table. """
    # https://stackoverflow.com/questions/42285417/how-to-preserve-links-when-scraping-a-table-with-beautiful-soup-and-pandas
    if data_type == 'title':
        data = [[td.a.get('title') if td.find('a') else ''.join(td.stripped_strings) for td in row.find_all('td')]
                for row in table.find_all('tr')]
    if data_type == 'link':
        data = [[td.a['href'] if td.find('a') else ''.join(td.stripped_strings) for td in row.find_all('td')]
                for row in table.find_all('tr')]
    else:
        data = [[td.a.string if td.find('a') else ''.join(td.stripped_strings) for td in row.find_all('td')]
                for row in table.find_all('tr')]   
    
    data = [d for d in data if len(d)!=0][0::skip_rows]
    
    return data

def get_fbref_big5(url):
    soup = get_soup(url)
    df = pd.read_html(str(soup))[0]
    
    # column names - collapse the multiindex
    col1 = list(df.columns.get_level_values(0))
    col1 = ['' if c[:7]=='Unnamed' else c.replace(' ', '_').lower() for c in col1]
    col2 = list(df.columns.get_level_values(1))
    col2 = [c.replace(' ', '_').lower() for c in col2]
    cols = [f'{c}_{col2[i]}' if c != '' else col2[i] for i, c in enumerate(col1)]
    df.columns = cols
    
    # remove lines that are the header row repeated
    df = df[df.rk != 'Rk'].copy()
    
    # add the url for the player profile and match logs
    # https://stackoverflow.com/questions/42285417/how-to-preserve-links-when-scraping-a-table-with-beautiful-soup-and-pandas
    parsed_table = soup.find_all('table')[0]
    data = [[td.a['href'] if td.find('a') else ''.join(td.stripped_strings) for td in row.find_all('td')]
        for row in parsed_table.find_all('tr')]
    data = [d for d in data if len(d)!=0]
    match_log = [d[-1] for d in data]
    player_profile = [d[0] for d in data]
    df['match_link'] = match_log
    df['player_link'] = player_profile
    
    # remove players who haven't played a minute from the playing time table
    if 'playing_time_mp' in df.columns:
        df = df[df.playing_time_mp != '0'].copy()
        df.reset_index(drop=True, inplace=True)
        df['rk'] = df.index + 1
        
    # drop the matches column
    df.drop('matches', axis='columns', inplace=True)

    # columns to numeric columns
    df[df.columns[6:-2]] = df[df.columns[6:-2]].apply(pd.to_numeric, errors='coerce', axis='columns')
    return df

def get_fbref_player_dob(url):
    soup = get_soup(url)
    info = soup.findAll("div", {"class": "players"})[0]
    if info.find("span", itemprop="birthDate"):
        dob = pd.to_datetime(info.find("span", itemprop="birthDate")['data-birth'])
    else:
        dob = pd.to_datetime(np.nan)
    info = BeautifulSoup(str(info)[:str(info).find('Position')], 'html.parser')
    name = info.find('p').getText()
    if name == '':
        name = info.find('span').getText()
    return name, dob

def get_tm_team_league(soup):
    """Get the team name and league from a team page."""
    team_name, league = soup.find("meta", attrs={'name':'keywords'})['content'].split(',')[:2]
    return team_name, league

def format_tm_market_value(df):
    """Helper function to format the market value in a dataframe from a string (euros) to millons. """
    df['market_value'] = df['market_value'].str.replace('€', '')
    mask_thousand = df['market_value'].str[-3:] == 'Th.'
    df['market_value'] = df['market_value'].str.replace('Th.', '').str.replace('m', '')
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce')
    df.loc[mask_thousand, 'market_value'] = df.loc[mask_thousand, 'market_value'] / 1000
    df.rename({'market_value': 'market_value_euro_millions'}, axis='columns', inplace=True)
    return df

def get_tm_team_links(url):
    """Get links for each team from the league page."""
    soup = get_soup(url)
    table = soup.find_all('table')[3]
    links = table.find_all('a', class_='vereinprofil_tooltip')
    links = [l['href'] for l in links]
    links = list(set(links))
    links = [f'https://www.transfermarkt.com{l}' for l in links]
    return links

def get_tm_team_squad(url):
    """ Get the team squad from a team url."""
    soup = get_soup(url)
    team_name, league = get_tm_team_league(soup)
    table = soup.find_all('table')[1]
    data = get_data_from_table(table, 'string', 3)
    
    # format data as a dataframe
    df = pd.DataFrame(data) 
    if len(df.columns) == 14:
        df = df.drop([2, 6, 7, 11], axis=1)
    else:
        df = df.drop([2, 6, 10], axis=1)  
    df.columns = ['number', 'transfer_details', 'player', 'position', 'dob_age', 'height', 'foot',
                  'joined', 'contract_expires', 'market_value']
    df['team_name'] = team_name
    df['league'] = league
    
    # get the links and add to the dataframe
    data2 = get_data_from_table(table, 'link', 3)
    player_links = [d[3] for d in data2]
    signed_from = [d[10] for d in data2]
    df['player_link'] = player_links
    df['signed_from_link'] = signed_from
    
    return df

def get_tm_arrivals_and_departures(url):
    """Get the team arrivals and departures from a team transfer page."""
    soup = get_soup(url)
    team_name, league = get_team_league(soup)
    
    # get data from tables
    tables = soup.find_all('table')
    # arrivals
    arrival_table = tables[2]
    arrival_data = get_data_from_table(arrival_table, 'string', 5)
    arrival_data_links = get_data_from_table(arrival_table, 'link', 5)
    # departures
    departure_table = [t for t in tables if 'Joined' in str(t)][0]
    departure_data = get_data_from_table(departure_table, 'string', 5)
    departure_data_links = get_data_from_table(departure_table, 'link', 5)
       
    # arrival dataframe
    df_arrival = pd.DataFrame(arrival_data[:-1])
    df_arrival.drop([0, 1, 2, 7, 8, 9], axis='columns', inplace=True)
    df_arrival.columns = ['player', 'pos', 'age', 'market_value', 'left', 'left_league', 'fee']
    arrival_player_links = [d[1] for d in arrival_data_links[:-1]]
    df_arrival['player_link'] = arrival_player_links
    df_arrival['joined'] = team_name
    df_arrival['joined_league'] = league
    
    # departure dataframe
    df_departure =  pd.DataFrame(departure_data[:-1])
    df_departure.drop([0, 1, 2, 7, 8, 9], axis='columns', inplace=True)
    df_departure.columns = ['player', 'pos', 'age', 'market_value', 'joined', 'joined_league', 'fee']
    departure_player_links = [d[1] for d in departure_data_links[:-1]]
    df_departure['player_link'] = departure_player_links
    df_departure['left'] = team_name
    df_departure['left_league'] = league
    
    df = pd.concat([df_arrival, df_departure])
    
    return df

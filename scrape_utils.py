import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import numpy as np
from io import StringIO
import os
import re

URL_BASE = 'https://fbref.com/en/comps/'
COMP_YEAR = {2024: '2023-2024',
             2023: '2022-2023',
             2022: '2021-2022',
            }
COMP_ID = {'england1': '9', 
           'england2': '10', 
           'italy1': '11', 
           'spain1': '12', 
           'france1': '13', 
           'spain2': '17', 
           'germany1': '20', 
           'usa1': '22', 
           'netherlands1': '23', 
           'brazil1': '24', 
           'mexico1': '31', 
           'portugal1': '32', 
           'belgium1': '37', 
           'germany2': '33', 
           'france2': '60', 
           'argentina1': '21', 
           'italy2': '18', 
          }
COMP_NAME = {'england1': 'Premier-League', 
             'england2': 'Championship', 
             'italy1': 'Serie-A', 
             'spain1': 'La-Liga', 
             'france1': 'Ligue-1', 
             'spain2': 'Segunda-Division', 
             'germany1': 'Bundesliga', 
             'usa1': 'Major-League-Soccer-Stats', 
             'netherlands1': 'Eredivisie', 
             'brazil1': 'Serie-A', 
             'mexico1': 'Liga-MX', 
             'portugal1': 'Primeira-Liga', 
             'belgium1': 'Belgian-Pro-League', 
             'germany2': '2-Bundesliga', 
             'france2': 'Ligue-2', 
             'argentina1': 'Liga-Profesional-Argentina', 
             'italy2': 'Serie-B',
            }
VALID_STATS = ['stats', 'keepersadv', 'keepers', 'shooting', 'passing', 'passing_types',
               'defense', 'gca', 'possession', 'playingtime', 'misc']

def get_soup(url):
    headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/39.0.2171.95 Safari/537.36')}
    r = requests.get(url, headers=headers)
    r.encoding = 'unicode-escape'
    return BeautifulSoup(r.content, 'html.parser')

def get_url(competition, year, stat):
    assert stat in VALID_STATS, f'{stat} not in {VALID_STATS}'
    assert year in COMP_YEAR.keys(), f'{year} not in {list(COMP_YEAR.keys())}'
    assert competition in COMP_NAME.keys(), f'{competition} not in {list(COMP_NAME.keys())}'
    comp_name = COMP_NAME[competition]
    comp_id = COMP_ID[competition]
    comp_year = COMP_YEAR[year]
    return f'{URL_BASE}{comp_id}/{comp_year}/{stat}/{comp_year}-{comp_name}-Stats'

def flatten_cols(df):
    col_level1 = list(df.columns.get_level_values(0))
    col_level1 = ['' if c[:7]=='Unnamed' else c.replace(' ', '_').lower() for c in col_level1]
    col_level2 = list(df.columns.get_level_values(1))
    col_level2 = [c.replace(' ', '_').lower() for c in col_level2]
    cols = [f'{c}_{col_level2[i]}' if c != '' else col_level2[i] for i, c in enumerate(col_level1)]
    cols = [re.sub('[^0-9a-zA-Z]+', '_',
                   (c.replace('%', '_percent')
                    .replace('take-on', 'take_on')
                    .replace('+/-', '_plus_minus_')
                    .replace('+', '_plus_')
                    .replace('-', '_minus_')
                  )).rstrip('_') for c in cols]
    df.columns = cols

def extract_stats(url):
    soup = get_soup(url)
    comments = soup.findAll(string=lambda string:isinstance(string, Comment))
    extracted_comments = [comment.extract() for comment in comments if 'table' in str(comment)]   
    df = pd.read_html(StringIO(str(extracted_comments[0])))[0]
    flatten_cols(df)
    df = df[df['rk'] != 'Rk'].copy()
    df.drop(['rk', 'matches'], axis='columns', inplace=True)
    # add player and match links
    table = BeautifulSoup(extracted_comments[0], 'html.parser')
    data = [[td.a['href'] if td.find('a') else ''.join(td.stripped_strings) for td in row.find_all('td')]
             for row in table.find_all('tr')]
    data = [d for d in data if len(d)!=0]
    match_log = [d[-1] for d in data]
    player_profile = [d[0] for d in data]
    df['match_link'] = match_log
    df['player_link'] = player_profile
    return df

def cols_to_numeric(df):
    STRING_COLS = ['player', 'nation', 'pos', 'squad', 'match_link', 'player_link']
    for col in df.columns:
        if col == 'age':
            df[col] = pd.to_numeric(df[col].str.split('-').str[0])
        elif col not in STRING_COLS:
            df[col] = pd.to_numeric(df[col])

def stats_to_parquet(competition, year, stat, directory='data', sub_directory='fbref'):
    url = get_url(competition, year, stat)
    df = extract_stats(url)
    cols_to_numeric(df)
    dir_path = os.path.join(directory, sub_directory, competition, str(year))
    os.makedirs(dir_path, exist_ok=True)
    file_name = os.path.join(dir_path, f'{stat}.parquet')
    df.to_parquet(file_name)

def get_fbref_player_dob(url):
    soup = get_soup(url)
    info = soup.findAll("div", {"class": "players"})[0]
    squad = [p for p in info.find_all('p') if 'Club' in p.getText()]
    if len(squad) == 0:
        squad = None
    else:
        squad = squad[0].find('a').contents[0]
        
    dob = [span.text.strip() for span in soup.find_all('span') if span.get('id') == 'necro-birth']
    if dob:
        dob = pd.to_datetime(dob[0])
    else:
        dob = pd.to_datetime(np.nan)

    info = BeautifulSoup(str(info)[:str(info).find('Position')], 'html.parser')
    name = info.find('p').getText()
    if name == '':
        name = info.find('span').getText()
    return name, dob, squad

def get_tm_team_links(url):
    """Get links for each team from the league page."""
    soup = get_soup(url)
    table = soup.find_all('table')[1]
    links = [td.find('a').get('href') for td in table.find_all("td", class_='zentriert')
             if td.find('a') and 'kader' in td.find('a').get('href')]
    links = [f'https://www.transfermarkt.com{l}/plus/1' for l in links]
    return links

def get_tm_team_squad(url):
    soup = get_soup(url)
    table = soup.find_all('table')[1]
    cells = []
    for td in table.find_all('td', class_='zentriert'):
        if td.text:
            cells.append(td.text)
        elif td.img:
            cells.append(td.img.get('title'))
        else:
            cells.append(None)
    df = pd.DataFrame(np.array(cells).reshape(-1, 8))
    if df[4].isin(['right', 'left', 'both']).sum() > 0:
        df.columns = ['jersey_number', 'date_of_birth_and_age', 'nationality',
                      'height', 'foot', 'joined', 'signed_from', 'contract']
        df['current_club'] = None
    else:
        df.columns = ['jersey_number', 'date_of_birth_and_age', 'nationality',
                      'current_club', 'height', 'foot', 'joined', 'signed_from']
        df['contract'] = None
    df['player'] = [tab.find('img').get('title') 
                    for tab in soup.find_all('table', class_='inline-table')]
    df['position'] = [tab.find_all('td')[-1].text.strip()
                      for tab in soup.find_all('table', class_='inline-table')]
    df['player_url'] = [td.find('a').get('href') 
                        for td in table.find_all('td', attrs={'class': 'hauptlink'}) 
                        if 'rechts' not in td.attrs['class']]
    df['market_value'] = [td.text for td in table.find_all('td', class_='rechts hauptlink')]
    df['team_name'] = soup.find("meta", attrs={'name':'keywords'})['content'].split(',')[0]
    df = df[['team_name', 'jersey_number', 'player', 'position',
             'date_of_birth_and_age', 'nationality', 'current_club', 'height',
             'foot', 'joined',
             'signed_from', 'contract', 'market_value', 'player_url']]
    return df

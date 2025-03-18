import os.path
import time
from datetime import datetime, timedelta
import psycopg2
import sqlite3
from pytz import timezone as pytz_timezone

from requests import request
from bs4 import BeautifulSoup as bs, element

'''
CREATE TABLE Trackers(url TEXT PRIMARY KEY , finished TEXT, start_time TIMESTAMP WITH TIME ZONE, end_time TIMESTAMP WITH TIME ZONE);
CREATE TABLE Stats(url TEXT, timestamp TIMESTAMP WITH TIME ZONE, number INTEGER, name TEXT, game_name TEXT, 
checks_done INTEGER, checks_total INTEGER, percentage REAL, connection_status TEXT);
CREATE TABLE Stats_Total(url TEXT, timestamp TIMESTAMP WITH TIME ZONE, number INTEGER, name TEXT, game_name TEXT, 
games_done INTEGER, games_total INTEGER, checks_done INTEGER, checks_total INTEGER, percentage REAL, 
connection_status TEXT);
'''


def add_playerinfo_to_dict(player_dict, info_list, timestamp) -> None:
    # print(info_list)
    info_list[4] = info_list[4].split('/')
    info_list[6] = 0 if info_list[6] == "None" else int(float(info_list[6]))
    player_dict[int(info_list[0])] = {
        'number': int(info_list[0]),
        'name': info_list[1].replace("'", "''"),
        'game_name': info_list[2].replace("'", "''"),
        'connection_status': info_list[3],
        'checks_done': int(info_list[4][0]),
        'checks_total': int(info_list[4][1]),
        'percentage': float(info_list[5]),
        'timestamp': timestamp,
        'last_activity': f'{info_list[6]//3600}:{(info_list[6]//60) % 60}',
    }

def add_totalinfo_to_dict(player_dict, info_list, timestamp) -> None:
    # print(info_list)
    info_list[4] = info_list[4].split('/')
    info_list[6] = 0 if info_list[6] == "None" else int(float(info_list[6]))
    player_dict[int(info_list[0])] = {
        'number': int(info_list[0]),
        'name': info_list[1].replace("'", "''"),
        'game_name': info_list[2].replace("'", "''"),
        'connection_status': info_list[3],
        'checks_done': int(info_list[4][0]),
        'checks_total': int(info_list[4][1]),
        'percentage': float(info_list[5]),
        'timestamp': timestamp,
        'last_activity': f'{info_list[6] // 3600}:{(info_list[6] // 60) % 60}',
        'games_done': int(info_list[7][0]),
        'games_total': int(info_list[7][1]),
    }


def add_old_playerinfo_to_dict(player_dict, info_list) -> None:
    # print(info_list)
    player_dict[info_list[2]] = {
        'number': int(info_list[2]),
        'name': info_list[3].replace("'", "''"),
        'game_name': info_list[4].replace("'", "''"),
        'connection_status': info_list[8],
        'checks_done': int(info_list[5]),
        'checks_total': int(info_list[6]),
        'percentage': float(info_list[7]),
        'timestamp': info_list[1],
    }

def add_old_totalinfo_to_dict(player_dict, info_list) -> None:
    # print(info_list)
    player_dict[info_list[2]] = {
        'number': int(info_list[2]),
        'name': info_list[3].replace("'", "''"),
        'game_name': info_list[4].replace("'", "''"),
        'connection_status': info_list[10],
        'checks_done': int(info_list[7]),
        'checks_total': int(info_list[8]),
        'percentage': float(info_list[9]),
        'timestamp': info_list[1],
        'games_done': int(info_list[5]),
        'games_total': int(info_list[6]),
    }



def crawl_tracker(tracker_url: str) -> dict[int, dict[str, str|int|float]]:
    tracker_page = request('get', tracker_url)

    tracker_html = bs(tracker_page.text, 'html.parser')

    timestamp = datetime.now(pytz_timezone('Europe/Berlin'))
    player_data_dict = {}
    for player in tracker_html.find('tbody').contents:
        tmp = []
        for player_data in player.contents:
            if isinstance(player_data, element.Tag):
                tmp.append(player_data.get_text(strip=True))
        add_playerinfo_to_dict(player_data_dict, tmp, timestamp)

    tmp = [0]
    for total_data in tracker_html.find('tfoot').contents[1].contents:
        if isinstance(total_data, element.Tag):
            tmp.append(total_data.get_text(strip=True))
    total_check = tmp[3].split(' ')[0].split('/')
    if total_check[0] == total_check[1]:
        tmp[3] = 'Done'
    else:
        tmp[3] = 'Ongoing'
    tmp[6] = 0.0
    tmp.append(total_check)
    add_totalinfo_to_dict(player_data_dict, tmp, timestamp)

    return player_data_dict


def push_to_db(db_connector, db_cursor, tracker_url:str) -> None:
    '''

    :param db_connector:
    :param db_cursor:
    :param tracker_url: URL for the AP-Multitracker Pageto crawl the information
    :param spreadsheet: URL for possible target to push the data into. meant for the big asyncs spreadsheet. normally empty
    :return:
    '''
    timer = time.time()
    capture = crawl_tracker(tracker_url)
    print("time taken to capture: ", time.time() - timer)
    timer = time.time()
    # db_cursor.execute(f"SELECT * FROM Stats JOIN (SELECT max(timestamp) AS time, number, FROM Stats WHERE url = "
    #                   f"'{tracker_url}' GROUP BY number) AS Ts ON Stats.timestamp = Ts.time AND Stats.number = "
    #                   f"Ts.number AND Stats.url = Ts.url")
    db_cursor.execute(f"SELECT * FROM Stats LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
                      f"url AS ts_url FROM Stats WHERE url = '{tracker_url}' GROUP BY number, url) AS Ts ON "
                      f"Stats.timestamp = Ts.time AND Stats.number = Ts.ts_number AND Stats.url = Ts.ts_url WHERE "
                      f"Stats.url = '{tracker_url}'")

    old_player_data = db_cursor.fetchall()
    print(f"time taken to fetch old data: {time.time() - timer}")
    old_player_data_dict = {}
    for row in old_player_data:
        add_old_playerinfo_to_dict(old_player_data_dict, row)
    # print(len(capture))
    db_cursor.execute(f"SELECT * FROM Stats_Total LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
                      f"url AS ts_url FROM Stats_Total WHERE url = '{tracker_url}' GROUP BY number, url) AS Ts ON "
                      f"Stats_Total.timestamp = Ts.time AND Stats_Total.number = Ts.ts_number AND Stats_Total.url = "
                      f"Ts.ts_url WHERE Stats_Total.url = '{tracker_url}'")
    old_total_data = db_cursor.fetchall()
    for row in old_total_data:
        add_old_totalinfo_to_dict(old_player_data_dict, row)

    timer = time.time()
    for index, _ in enumerate(old_player_data_dict):
        if old_player_data_dict[index]['checks_done'] == capture[index]["checks_done"] and old_player_data_dict[
            index]['connection_status'] == capture[index]["connection_status"]:
            del capture[index]

    print(f"time taken to compare old data to new data: {time.time() - timer}")
    # print(capture)
    # print(len(capture))
    if capture:
        push_total = False
        timer = time.time()
        query = ('INSERT INTO Stats (timestamp, url, number, name, game_name, checks_done, checks_total, percentage, '
                 'connection_status) VALUES ')
        query_total = ('INSERT INTO Stats_total (timestamp, url, number, name, game_name, games_done, '
                       'games_total, checks_done, checks_total, percentage, connection_status) VALUES ')
        for index, data, in capture.items():
            if old_player_data_dict and (data['timestamp'] - old_player_data_dict[index]['timestamp']).seconds > 300:
            # if old_player_data_dict and (data['timestamp'] - datetime.fromtimestamp(old_player_data_dict[index][
            #                                                                             'timestamp'],
            #                                                                             pytz_timezone(
            #                                                                             'Europe/Berlin'))).seconds
            #                                                                             > 0:
                if data['name'] == "Total":  # total
                    query_total = query_total + (
                        f"(TIMESTAMP '{old_player_data_dict[index]['timestamp']-timedelta(minutes=1)}', '{tracker_url}',"
                        f" {old_player_data_dict[index]['number']}, '{old_player_data_dict[index]['name']}', "
                        f"'{old_player_data_dict[index]['game_name']}', "
                        f" {old_player_data_dict[index]['games_done']},"
                        f" {old_player_data_dict[index]['games_total']},"
                        f" {old_player_data_dict[index]['checks_done']}, {old_player_data_dict[index]['checks_total']},"
                        f" {old_player_data_dict[index]['percentage']}, '{old_player_data_dict[index]['connection_status']}'),")
                    push_total = True
                else:  # players
                    query = query + (
                        f"(TIMESTAMP '{old_player_data_dict[index]['timestamp'] - timedelta(minutes=1)}', '{tracker_url}',"
                        f" {old_player_data_dict[index]['number']}, "
                        f"'{old_player_data_dict[index]['name']}', "
                        f"'{old_player_data_dict[index]['game_name']}', {old_player_data_dict[index]['checks_done']}, {old_player_data_dict[index]['checks_total']},"
                        f" {old_player_data_dict[index]['percentage']}, '{old_player_data_dict[index]['connection_status']}'),")
            if data['name'] == "Total": # total
                query_total = query_total + (
                    f"(TIMESTAMP '{data['timestamp']}', '{tracker_url}', {data['number']}, '{data['name']}', "
                    f"'{data['game_name']}', {data['games_done']}, {data['games_total']},"
                    f"{data['checks_done']}, {data['checks_total']},"
                    f"{data['percentage']}, '{data['connection_status']}'),")
            else: # players
                query = query + (f"(TIMESTAMP '{data['timestamp']}', '{tracker_url}', {data['number']}, '{data['name']}', "
                                 f"'{data['game_name']}', {data['checks_done']}, {data['checks_total']}, "
                                 f"{data['percentage']}, '{data['connection_status']}'),")
        if push_total:
            db_cursor.execute(query_total[:-1])
        db_cursor.execute(query[:-1])

        print("time taken for database push: ", time.time() - timer, "items pushed:", len(capture))

        if 0 in capture.keys() and (capture[0]["checks_done"] == capture[0]["checks_total"] or capture[0][
            "connection_status"] == "Done"):
            db_cursor.execute(f"UPDATE Trackers SET finished = 'x', end_time = "
                              f"{datetime.now(pytz_timezone('Europe/Berlin'))} WHERE url = '{tracker_url}';")
            print(f"Seed with Tracker at {tracker_url} has finished")

    db_connector.commit()
    return


def create_table_if_needed(db_connector, db_cursor):
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Trackers(url TEXT PRIMARY KEY , finished TEXT, start_time "
                      "TIMESTAMPTZ, end_time TIMESTAMPTZ);")
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Stats(url TEXT, timestamp TIMESTAMPTZ, number INTEGER, name TEXT, "
                      "game_name TEXT, checks_done INTEGER, checks_total INTEGER, percentage REAL, connection_status "
                      "TEXT);")
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Stats(url TEXT, timestamp TIMESTAMPTZ, number INTEGER, name TEXT, "
                      "game_name TEXT, games_done INTEGER, games_total INTEGER, checks_done INTEGER, checks_total "
                      "INTEGER, percentage REAL, connection_status TEXT);")
    db_connector.commit()


if __name__ == "__main__":
    db =  psycopg2.connect(dbname="",
                           user="",
                           password="",
                           host="",
                           port="")
    # db = sqlite3.connect("AP-Crawler.db")
    cursor = db.cursor()
    create_table_if_needed(db, cursor)
    while True:

        with open(f'{os.path.curdir}/new_trackers.txt', 'r') as new_trackers:
            new_tracker_urls = new_trackers.readlines()
            for new_url in new_tracker_urls:
                if "/room/" in new_url:
                    room_page = request('get', "https://archipelago.gg/room/vJ66m0maRH6WOP6IqOZIVA")

                    room_html = bs(room_page.text, 'html.parser')
                    room_info = room_html.find("span", id="host-room-info").contents
                    new_url = f"{new_url.split('/room/')[0]}{room_info[1].get('href')}"
                if "/tracker/" in new_url:
                    cursor.execute(f"INSERT INTO Trackers(url, start_time) VALUES ('{new_url.rstrip()}', "
                                   f"TIMESTAMP '{datetime.now(pytz_timezone('Europe/Berlin'))}');")
                    print(f"added {new_url.rstrip()} to database")
                else:
                    print("no valid tracking link found")
            db.commit()
        if len(new_tracker_urls) > 0:
            with open(f'{os.path.curdir}/new_trackers.txt', 'w') as new_trackers:
                new_trackers.write("")

        timer = time.time()

        get_unfinished_seeds_querey = "SELECT URL FROM Trackers WHERE COALESCE(finished, '') = '';"
        cursor.execute(get_unfinished_seeds_querey)
        unfinished_seeds = cursor.fetchall()
        ongoing_seeds = len(unfinished_seeds)
        if ongoing_seeds == 0:
            break
        print(f"crawling {ongoing_seeds} Tracker{'s' if ongoing_seeds > 1 else ''}.")
        for url in unfinished_seeds:
            try:
                push_to_db(db, cursor, url[0])
            except:
                print(f"Error und push_to_db for URL {url[0]}")
                db.close()
                db = psycopg2.connect(dbname="",
                                      user="",
                                      password="",
                                      host="",
                                      port="")
                # db = sqlite3.connect("AP-Crawler.db")
                cursor = db.cursor()

        print("time taken for total: ", time.time() - timer)
        sleep_time = (int(60 - (time.time() - timer)) + 1)
        print(f"sleeping for {sleep_time} seconds")
        time.sleep(sleep_time if sleep_time > 0 else 0)
    db.close()
    print("connection closed")

# https://docs.google.com/spreadsheets/d/16dS6P6IV7a1jN9QzUkySEPSbqXUw4rb0-yYtqcdKk0Y/ copy of big async sheet
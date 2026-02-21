import asyncio
from multiprocessing import Pool, Process
import os.path
import time
from datetime import datetime, timedelta
from typing import Tuple
from os import getenv
from dotenv import load_dotenv
import json

import psycopg2
from asyncio import gather, create_task, run, sleep, TaskGroup
from pytz import timezone as pytz_timezone
import logging
import httpx
from requests import request
from bs4 import BeautifulSoup as bs, element

'''
CREATE TABLE Trackers(tracker_index BIGSERIAL PRIMARY KEY, url TEXT , finished TEXT, start_time TIMESTAMP WITH TIME ZONE, 
end_time TIMESTAMP WITH TIME ZONE, title TEXT);

CREATE TABLE Players(player_index BIGSERIAL PRIMARY KEY, playernumber INTEGER, room_url BIGINT references Trackers(
tracker_index), basename TEXT);

CREATE TABLE Stats_Players(url_index BIGINT references Trackers(tracker_index), timestamp TIMESTAMP WITH TIME ZONE, 
number BIGINT references Players(player_index), name TEXT, game_name TEXT, checks_done INTEGER, checks_total INTEGER, percentage REAL, connection_status TEXT);

CREATE TABLE Stats_Total(url_index BIGINT references Trackers(tracker_index), timestamp TIMESTAMP WITH TIME ZONE, 
number BIGINT references Players(player_index), name TEXT, game_name TEXT, games_done INTEGER, games_total INTEGER, 
checks_done INTEGER, checks_total INTEGER, percentage REAL, hints_found INTEGER, hints_requested INTEGER, hints_total 
INTEGER,
connection_status TEXT);
'''
sec_30 = 30
days_7 = 7*24*60*60 #604800
process_count =  (os.cpu_count()//2)+1

load_dotenv()
db_login = {
    "dbname":getenv('dbname'),
    "user":getenv('user'),
    "password":getenv('password'),
    "host":getenv('host'),
    "port":getenv('port')
}

client_status_lookup = {
    0: "Disconnected",
    5: "Connected",
    10: "Ready",
    20: "Playing",
    30: "Goal Completed",
}

async def fetch_tracker_from_room(new_url):
    room_page = request('get', new_url)
    room_html = bs(room_page.text, 'html.parser')
    try:
        return room_html.find("span", id="host-room-info").contents
    except:
        raise(ValueError(f"Room at '{new_url}' does not exist anymore"))

def get_players(tracker_url:str):
    api_content = request('get', tracker_url.replace('/tracker/', '/api/tracker/')).content
    tracker_api_json = json.loads(api_content)
    return tracker_api_json["aliases"]

async def add_playerinfo_to_dict(player_dict, info_list, timestamp) -> None:
    # print(info_list)
    for new_player_data in info_list:
        new_player_data[4] = new_player_data[4].split('/')
        # info_list[6] = 0 if info_list[6] == None else datetime.strptime(info_list[6], "%a, %d %b %Y %H:%M:%S GMT").strftime(
        #     "%Y-%m-%d %H:%M:%S")
        new_player_data[6] = 0 if new_player_data[6] == "None" else int(float(new_player_data[6]))
        player_dict[int(new_player_data[0])] = {
            'number': int(new_player_data[0]),
            'name': new_player_data[1].replace("'", "''"),
            'game_name': new_player_data[2].replace("'", "''"),
            'connection_status': new_player_data[3],
            'checks_done': int(new_player_data[4][0]),
            'checks_total': int(new_player_data[4][1]),
            'percentage': float(new_player_data[5]),
            'timestamp': timestamp,
            'last_activity': f'{new_player_data[6]//3600}:{(new_player_data[6]//60) % 60}',
            'hints_found': int(new_player_data[7]),
            'hints_requested': int(new_player_data[8]),
            'hints_total': int(new_player_data[9]),
        }

async def add_totalinfo_to_dict(player_dict, info_list, timestamp) -> None:
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
        'hints_found': int(info_list[8]),
        'hints_requested': int(info_list[9]),
        'hints_total': int(info_list[10]),
    }


async def add_old_playerinfo_to_dict(player_dict, info_list) -> None:
    # print(info_list)
    for player_data in info_list:
        player_dict[player_data[2]] = {
            'number': int(player_data[2]),
            'name': player_data[3].replace("'", "''"),
            'game_name': player_data[4].replace("'", "''"),
            'connection_status': player_data[8],
            'checks_done': int(player_data[5]),
            'checks_total': int(player_data[6]),
            'percentage': float(player_data[7]),
            'timestamp': player_data[1],
            'hints_found': int(player_data[10]),
            'hints_requested': int(player_data[11]),
            'hints_total': int(player_data[12]),
        }

async def add_old_totalinfo_to_dict(player_dict, info_list) -> None:
    # print(info_list)
    for total_data in info_list:
        player_dict[total_data[2]] = {
            'number': int(total_data[2]),
            'name': total_data[3].replace("'", "''"),
            'game_name': total_data[4].replace("'", "''"),
            'connection_status': total_data[10],
            'checks_done': int(total_data[7]),
            'checks_total': int(total_data[8]),
            'percentage': float(total_data[9]),
            'timestamp': total_data[1],
            'games_done': int(total_data[5]),
            'games_total': int(total_data[6]),
            'hints_found': int(total_data[12]),
            'hints_requested': int(total_data[13]),
            'hints_total': int(total_data[14]),
        }

async def crawl_tracker_from_api(task_index, client, tracker_api_url: str):
    try:
        api_content = await client.get(tracker_api_url.replace('/tracker/', '/api/tracker/')).content
        static_api_content = await client.get(tracker_api_url.replace('/tracker/', '/api/static_tracker/')).content
        tracker_api_json = json.loads(api_content)
        static_tracker_api_json = json.loads(static_api_content)
    except:
        print(f"Error when fetching content for {tracker_api_url},\nPage probably does not exist anymore")
        return False, dict()
    timestamp = datetime.now(pytz_timezone('Europe/Berlin'))
    player_data_dict = {}
    total_locations = 0
    games_done = 0
    very_last_activity = 0
    try:
        tmp = [0, ]
        timer = time.time()
        new_player_data = []
        for index in range(0,len(tracker_api_json["activity_timers"])):
            current_player_static =  static_tracker_api_json["player_game"][index]
            current_player_api =  tracker_api_json["aliases"][index]

            number = current_player_static["player"]
            name = current_player_api["alias"]
            game_name = current_player_static["game"]
            connection_status = client_status_lookup[current_player_api["status"]]
            checks_done = len(current_player_api["locations"])
            checks_total = current_player_static["total_locations"]
            percentage = checks_done/checks_total
            last_activity = current_player_api["time"]
            tmp = [number, name, game_name, connection_status, (checks_done, checks_total), percentage,
                   last_activity]
            print(tmp)
            new_player_data.append(tmp)
            total_locations += checks_total
            if connection_status == client_status_lookup[30]:
                games_done += 1
            if last_activity > very_last_activity:
                very_last_activity = last_activity
        await add_playerinfo_to_dict(player_data_dict, new_player_data, timestamp)
        print(f"time taken for packing data from api: {time.time() - timer}")
        number = 0
        name = "Total"
        checks_total = total_locations
        checks_done = tracker_api_json["total_checks_done"][0]["checks_done"]
        percentage = checks_done/checks_total
        if checks_done == checks_total:
            connection_status = 'Done'
        else:
            connection_status = 'Ongoing'
        tmp = [number, name, 'All Games', connection_status, (checks_done, checks_total), percentage,
               very_last_activity, (games_done, len(tracker_api_json["activity_timers"]))]
        await add_totalinfo_to_dict(player_data_dict, tmp, timestamp)

        return True, player_data_dict
    except:
        print("error when packing data")
        return False, dict()
        raise (ValueError(f"Room at '{tracker_url}' does not exist anymore"))

async def crawl_tracker_from_html(task_index, client, tracker_url: str) -> Tuple[bool, dict[int, dict[str,
str|int|float]], str, int, float]:
    timer = time.time()
    tracker_page = await client.get(tracker_url, timeout=50)
    tracker_html = bs(tracker_page.text, 'html.parser')
    timestamp = datetime.now(pytz_timezone('Europe/Berlin'))
    time_spend = time.time() - timer
    player_data_dict = {}
    hint_dict = { "Total": (0, 0, 0) }
    try:
        new_player_data = []
        check_table = tracker_html.find(id='checks-table')
        hints_table = tracker_html.find(id='hints-table')
        for hints in hints_table.find('tbody').contents:
            tmp = []
            for hint_data in hints:
                if isinstance(hint_data, element.Tag):
                    tmp.append(hint_data.get_text(strip=True))
            if len(tmp) > 0:
                if not tmp[0] in hint_dict.keys():
                    hint_dict[tmp[0]] = (0, 0, 0) #(done, requested, total)
                if not tmp[1] in hint_dict.keys():
                    hint_dict[tmp[1]] = (0, 0, 0)  # (done, requested, total)
                if tmp[6] == "":
                    hint_dict[tmp[0]] = (hint_dict[tmp[0]][0], hint_dict[tmp[0]][1], hint_dict[tmp[0]][1] + 1)
                    #finding player
                    hint_dict[tmp[1]] = (hint_dict[tmp[1]][0], hint_dict[tmp[1]][1] + 1, hint_dict[tmp[1]][1])
                    #requesting player
                else:
                    hint_dict[tmp[0]] = (hint_dict[tmp[0]][0] + 1, hint_dict[tmp[0]][1], hint_dict[tmp[0]][1] + 1)
                    #finding player
                    hint_dict[tmp[1]] = (hint_dict[tmp[1]][0], hint_dict[tmp[1]][1] + 1, hint_dict[tmp[1]][1])
                    #requesting player
        for key in hint_dict.keys():
            hint_dict["Total"] = (hint_dict["Total"][0]+hint_dict[key][0], hint_dict["Total"][1]+hint_dict[key][1],
                                  hint_dict["Total"][2]+hint_dict[key][2])
            ### each players hint contents look like this
            # 0 Finding player
            # 1 Receiving player
            # 2 Itemname
            # 3 Location
            # 4 Game
            # 5 Entrance
            # 6 Found/not found
        for player in check_table.find('tbody').contents:
            tmp = []
            for player_data in player.contents:
                if isinstance(player_data, element.Tag):
                    tmp.append(player_data.get_text(strip=True))
            if tmp[1] in hint_dict.keys():
                tmp.append(hint_dict[tmp[1]][0]) #hints done
                tmp.append(hint_dict[tmp[1]][1]) #hints requested
                tmp.append(hint_dict[tmp[1]][2]) #hints total
            else:
                tmp.append(0)  # hints done
                tmp.append(0)  # hints requested
                tmp.append(0)  # hints total
            new_player_data.append(tmp)
        await add_playerinfo_to_dict(player_data_dict, new_player_data, timestamp)

        tmp = [0]
        for total_data in check_table.find('tfoot').contents[1].contents:
            if isinstance(total_data, element.Tag):
                tmp.append(total_data.get_text(strip=True))
        total_check = tmp[3].split(' ')[0].split('/')
        if total_check[0] == total_check[1]:
            tmp[3] = 'Done'
        else:
            tmp[3] = 'Ongoing'
        tmp[6] = 0.0
        tmp.append(total_check)
        tmp.append(hint_dict['Total'][0]) #hints done
        tmp.append(hint_dict['Total'][1]) #hints requested
        tmp.append(hint_dict['Total'][2]) #hints total
        await add_totalinfo_to_dict(player_data_dict, tmp, timestamp)

        return True, player_data_dict, tracker_url, task_index, time_spend
    except:
        return False, dict(), tracker_url, task_index, time_spend
        raise (ValueError(f"Room at '{tracker_url}' does not exist anymore"))

async def crawling_process(unfinished_seeds, old_player_data_per_url, old_total_data_per_url):
    timer = time.time()
    db = psycopg2.connect(**db_login)
    cursor = db.cursor()
    unfinished_crawl_tasks = []
    async with httpx.AsyncClient() as client:
        for crawl_index, url_tuple in enumerate(unfinished_seeds):
            unfinished_crawl_tasks.append(crawl_tracker_from_html(
                crawl_index, client, url_tuple[0]))  # time consuming for large rooms
        ### return True/False, player_data_dict, tracker_url, task_index, time_spend

        # success, capture = await crawl_tracker_from_api(crawl_index, client, url_tuple[0])
        # print(f"created all crawling tasks in {time.time() - timer} seconds")

        # results = await gather(*unfinished_crawl_tasks)
        for future in asyncio.as_completed(unfinished_crawl_tasks):
            result = await future
            success, player_dict, url, task_index, time_spend = result
            _, last_updated, title, checks_done = unfinished_seeds[task_index]
            if success:
                # print(f"start pushing {task_index}")
                await main_url_fetch(task_index, url, last_updated, title, checks_done,
                                     old_player_data_per_url[url], old_total_data_per_url[url], player_dict)
            # else:
            #     cursor.execute(f"UPDATE Trackers SET finished = 'x', end_time = "
            #                    f"'{datetime.now(pytz_timezone('Europe/Berlin'))}' WHERE url = '{url}';")
            #     db.commit()
        print(f"all crawling tasks processed in {time.time() - timer} seconds")

async def push_to_db(task_index, db_connector, db_cursor, tracker_url:str, has_title:bool, old_player_data:list,
                     old_total_data:list, capture) -> int:
    '''

    :param db_connector:
    :param db_cursor:
    :param tracker_url: URL for the AP-Multitracker Page to crawl the information
    :param has_title:
    :param old_player_data:
    :param old_total_data:
    :return:
    '''
    # print(f"start push to db for {tracker_url} at index {task_index}")
    # print("time taken to capture: ", time.time() - timer)
    timer = time.time()
    # db_cursor.execute(f"SELECT * FROM Stats_Players JOIN (SELECT max(timestamp) AS time, number, FROM Stats_Players WHERE url = "
    #                   f"'{tracker_url}' GROUP BY number) AS Ts ON Stats_Players.timestamp = Ts.time AND Stats_Players.number = "
    #                   f"Ts.number AND Stats_Players.url = Ts.url")
    # db_cursor.execute(f"SELECT * FROM Stats_Players LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
    #                   f"url AS ts_url FROM Stats_Players WHERE url = '{tracker_url}' GROUP BY number, url) AS Ts ON "
    #                   f"Stats_Players.timestamp = Ts.time AND Stats_Players.number = Ts.ts_number AND Stats_Players.url = Ts.ts_url WHERE "
    #                   f"Stats_Players.url = '{tracker_url}' AND Stats_Players.timestamp = Ts.time AND Stats_Players.number = Ts.ts_number")
    #
    # old_player_data = db_cursor.fetchall()
    #print(f"time taken to fetch old data: {time.time() - timer}")
    old_player_data_dict = {}
    # for row in old_player_data:
    await add_old_playerinfo_to_dict(old_player_data_dict, old_player_data)
    # print(len(capture))
    # db_cursor.execute(f"SELECT * FROM Stats_Total LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
    #                   f"url AS ts_url FROM Stats_Total WHERE url = '{tracker_url}' GROUP BY number, url) AS Ts ON "
    #                   f"Stats_Total.timestamp = Ts.time AND Stats_Total.number = Ts.ts_number AND Stats_Total.url = "
    #                   f"Ts.ts_url WHERE Stats_Total.url = '{tracker_url}' AND Stats_Total.timestamp = Ts.time AND "
    #                   f"Stats_Total.number = Ts.ts_number")
    # old_total_data = db_cursor.fetchall()
    # for row in old_total_data:
    await add_old_totalinfo_to_dict(old_player_data_dict, old_total_data)
    # timer = time.time()
    for index in old_player_data_dict.keys():
        if index == 0:
            delete = (old_player_data_dict[index]['games_done'] == capture[index]["games_done"] and
                      old_player_data_dict[index]['checks_done'] == capture[index]["checks_done"] and
                      old_player_data_dict[index]['connection_status'] == capture[index]["connection_status"])
        else:
            delete = (old_player_data_dict[index]['checks_done'] == capture[index]["checks_done"] and
                      old_player_data_dict[index]['connection_status'] == capture[index]["connection_status"])
        if delete:
            del capture[index]

    # print(f"time taken to compare old data to new data: {time.time() - timer}")
    # print(capture)
    # print(len(capture))
    if capture:
        push_total = False
        push_player = False
        timer = time.time()
        query = ('INSERT INTO Stats (timestamp, url, number, name, game_name, checks_done, checks_total, percentage, '
                 'hints_found, hints_requested, hints_total, connection_status) VALUES ')
        query_total = ('INSERT INTO Stats_total (timestamp, url, number, name, game_name, games_done, '
                       'games_total, checks_done, checks_total, percentage, hints_found, hints_requested, hints_total, '
                       'connection_status) VALUES ')
        query_list = []
        query_total_list = []
        for index, data, in capture.items():
            if old_player_data_dict and (data['timestamp'] - old_player_data_dict[index]['timestamp']).seconds > 300:
            # if old_player_data_dict and (data['timestamp'] - datetime.fromtimestamp(old_player_data_dict[index][
            #                                                                             'timestamp'],
            #                                                                             pytz_timezone(
            #                                                                             'Europe/Berlin'))).seconds
            #                                                                             > 0:
                if data['name'] == "Total":  # total
                    query_total_list.append(
                        f"(TIMESTAMP '{data['timestamp'] - timedelta(seconds=30)}', '{tracker_url}',"
                        f" {old_player_data_dict[index]['number']}, '{old_player_data_dict[index]['name']}', "
                        f"'{old_player_data_dict[index]['game_name']}', "
                        f" {old_player_data_dict[index]['games_done']}, "
                        f" {old_player_data_dict[index]['games_total']},"
                        f" {old_player_data_dict[index]['checks_done']}, "
                        f" {old_player_data_dict[index]['checks_total']}, "
                        f" {old_player_data_dict[index]['percentage']}, "
                        f" {old_player_data_dict[index]['hints_found']}, "
                        f" {old_player_data_dict[index]['hints_requested']}, "
                        f" {old_player_data_dict[index]['hints_total']}, "
                        f"'{old_player_data_dict[index]['connection_status']}')")
                else:  # players
                    query_list.append(
                        f"(TIMESTAMP '{data['timestamp'] - timedelta(seconds=30)}', '{tracker_url}',"
                        f" {old_player_data_dict[index]['number']}, "
                        f"'{old_player_data_dict[index]['name']}', "
                        f"'{old_player_data_dict[index]['game_name']}', "
                        f"{old_player_data_dict[index]['checks_done']}, "
                        f"{old_player_data_dict[index]['checks_total']},"
                        f" {old_player_data_dict[index]['percentage']}, "
                        f"{old_player_data_dict[index]['hints_found']}, "
                        f"{old_player_data_dict[index]['hints_requested']}, "
                        f"{old_player_data_dict[index]['hints_total']}, "
                        f"'{old_player_data_dict[index]['connection_status']}')")
            if data['name'] == "Total": # total
                query_total_list.append(
                    f"(TIMESTAMP '{data['timestamp']}', "
                    f"'{tracker_url}', "
                    f"{data['number']}, "
                    f"'{data['name']}', "
                    f"'{data['game_name']}', "
                    f"{data['games_done']}, "
                    f"{data['games_total']},"
                    f"{data['checks_done']}, "
                    f"{data['checks_total']},"
                    f"{data['percentage']}, "
                    f"{data['hints_found']}, "
                    f"{data['hints_requested']}, "
                    f"{data['hints_total']}, "
                    f"'{data['connection_status']}')")
                push_total = True
            else: # players
                query_list.append(f"(TIMESTAMP '{data['timestamp']}', "
                                f"'{tracker_url}', "
                                f"{data['number']}, "
                                f"'{data['name']}', "
                                f"'{data['game_name']}', "
                                f"{data['checks_done']}, "
                                f"{data['checks_total']}, "
                                f"{data['percentage']}, "
                                f"{data['hints_found']}, "
                                f"{data['hints_requested']}, "
                                f"{data['hints_total']}, "
                                f"'{data['connection_status']}')")
                push_player = True

        if push_total:
            db_cursor.execute(query_total + ", ".join(query_total_list))
        if push_player:
            db_cursor.execute(query + ", ".join(query_list))

        # print("time taken for database push: ", time.time() - timer, "items pushed:", len(capture))

        if 0 in capture.keys() and (capture[0]["checks_done"] == capture[0]["checks_total"] and not has_title
                                    or capture[0]["connection_status"] == "Done"):
            db_cursor.execute(f"UPDATE Trackers SET finished = 'x', end_time = "
                              f"'{datetime.now(pytz_timezone('Europe/Berlin'))}' WHERE url = '{tracker_url}';")
            print(f"Seed with Tracker at {tracker_url} at index {task_index} has finished")

    db_connector.commit()
    return len(capture)
    # else:
        #db_cursor.execute(f"UPDATE Trackers SET finished = 'x', end_time = "
        #                  f"'{datetime.now(pytz_timezone('Europe/Berlin'))}' WHERE url = '{tracker_url}';")
        #db_connector.commit()
        # return len(capture)



def create_table_if_needed(db_connector, db_cursor):
    db_cursor.execute("CREATE TABLE Trackers(tracker_index BIGSERIAL PRIMARY KEY, url TEXT , finished TEXT, "
                      "start_time TIMESTAMP WITH TIME ZONE, end_time TIMESTAMP WITH TIME ZONE, title TEXT);")
    db_cursor.execute("CREATE TABLE Players(player_index BIGSERIAL PRIMARY KEY, playernumber INTEGER, "
                      "room_url BIGINT references Trackers(tracker_index), basename TEXT);")
    db_cursor.execute("CREATE TABLE Stats_Players(url_index BIGINT references Trackers(tracker_index), "
                      "timestamp TIMESTAMP WITH TIME ZONE, number BIGINT references Players(player_index), "
                      "name TEXT, game_name TEXT, checks_done INTEGER, checks_total INTEGER, percentage REAL, "
                      "hints_found INTEGER, hints_requested INTEGER, hints_total INTEGER, connection_status TEXT);")
    db_cursor.execute("CREATE TABLE Stats_Total(url_index BIGINT references Trackers(tracker_index), "
                      "timestamp TIMESTAMP WITH TIME ZONE, number BIGINT references Players(player_index), name TEXT, "
                      "game_name TEXT, games_done INTEGER, games_total (INTEGER, checks_done) INTEGER, checks_total INTEGER, "
                      "percentage REAL, hints_found INTEGER, hints_requested INTEGER, hints_total INTEGER, "
                      "connection_status TEXT);")
    db_connector.commit()

async def new_url_handling(new_url:str, existing_trackers):

    if "/room/" in new_url:
        room_info = await fetch_tracker_from_room(new_url)
        new_url = f"{new_url.split('/room/')[0]}{room_info[1].get('href')}"
    if (new_url.rstrip(),) in existing_trackers:
        print(f"{new_url} already in database")
        return
    if "/tracker/" in new_url:
        db = psycopg2.connect(**db_login)
        cursor = db.cursor()
        cursor.execute(f"INSERT INTO Trackers(url, start_time, last_updated) VALUES ('{new_url.rstrip()}', "
                       f"TIMESTAMPTZ '{datetime.now(pytz_timezone('Europe/Berlin'))}', TIMESTAMPTZ '{datetime.now(pytz_timezone('Europe/Berlin'))}');")
        print(f"added {new_url.rstrip()} to database")
        await crawling_process([[new_url.rstrip(), datetime.now(pytz_timezone('Europe/Berlin')), "",
                                 0]], {new_url.rstrip():[]},{new_url.rstrip():[]})
        db.commit()
        db.close()
    else:
        print("no valid tracking link found")
    # players = get_players(new_url)
    # db = psycopg2.connect(**db_login)
    # cursor = db.cursor()
    # query = "INSERT INTO Players(playernumber, room_url, basename) VALUES"
    # for player in players:
    #     query = query + f"({player['player']},'{new_url}','{player['alias']}'),"
    #
    # cursor.execute(query[:-1])
    # print(f"added {len(players)} Player to database")
    # db.commit()
    # db.close()

async def main_url_fetch(index, url, last_updated, title, checks_done, old_player_data, old_total_data, capture):
    # print(f"starting task: {index}")
    # timer = time.time()
    db = psycopg2.connect(**db_login)
    cursor = db.cursor()
    # url, last_updated, title, checks_done = url_tuple
    # print(checks_done)
    checks_done = checks_done or 0
    if title is not None:
        has_title = True
    else:
        has_title = False
    # check_last_updated_query = f"SELECT last_updated FROM Trackers WHERE url = '{url}';"
    # cursor.execute(check_last_updated_query)
    # last_updated = url[1]
    if ((datetime.now(pytz_timezone('Europe/Berlin')) - timedelta(days=7)) > last_updated) or (checks_done == 0 and (datetime.now(pytz_timezone('Europe/Berlin')) - timedelta(days=1)) > last_updated ):
        cursor.execute(f"UPDATE trackers SET finished = 'x' WHERE url = '{url}'")
        print(f"set URL: {url} to finished/paused.")
        db.commit()
        res = 0
        # raise BaseException
    else:
        res = await push_to_db(index, db, cursor, url, has_title, old_player_data, old_total_data, capture)
        # print(res)
    if res > 0:
        update_last_updated_query = f"UPDATE trackers SET last_updated = TIMESTAMPTZ '{datetime.now(pytz_timezone('Europe/Berlin'))}' WHERE url = '{url}'"
        cursor.execute(update_last_updated_query)
        db.commit()
    db.close()
    # print("time taken for main url fetch: ", time.time() - timer)

def main():
    db = psycopg2.connect(**db_login)
    # db = sqlite3.connect("AP-Crawler.db")
    cursor = db.cursor()
    try:
        create_table_if_needed(db, cursor)
    except psycopg2.errors.DuplicateTable:
        print("tables already present")
        # pass
    db.close()
    while True:
        db = psycopg2.connect(**db_login)
        # db = sqlite3.connect("AP-Crawler.db")
        cursor = db.cursor()
        cursor.execute("Select URL FROM Trackers")
        existing_trackers = cursor.fetchall()
        with open(f'{os.path.curdir}/new_trackers.txt', 'r') as new_trackers:
            new_tracker_urls = new_trackers.readlines()
            new_tracker_urls = set(new_tracker_urls)
            # new_url_tasks = []
            for i, new_url in enumerate(new_tracker_urls):
                print(f"create task for new url {i} {new_url}")
                just_start_async([new_url_handling, new_url, existing_trackers])
                # new_url_tasks.append(create_task(new_url_handling(new_url, existing_trackers)))
            print("finished creating all tasks")
            # new_url_results = await gather(*new_url_tasks)
            print("all new urls processed")
            db.commit()
        if len(new_tracker_urls) > 0:
            with open(f'{os.path.curdir}/new_trackers.txt', 'w') as new_trackers:
                new_trackers.write("")
        timer = time.time()

        get_unfinished_seeds_query = "SELECT Trackers.URL, last_updated, title, stats_total.checks_done FROM Trackers LEFT OUTER JOIN (Select url, max(checks_done) as checks_done, max(connection_status) as connection_status from stats_total WHERE not connection_status = 'Done' GROUP BY url, checks_total) as stats_total on trackers.url = stats_total.url WHERE COALESCE(finished, '') = '';"
        cursor.execute(get_unfinished_seeds_query)
        unfinished_seeds = cursor.fetchall()
        ongoing_seeds = len(unfinished_seeds)
        if ongoing_seeds == 0:
            db.commit()
            print("connection closed")
            print("sleeping 10 minutes")
            time.sleep(600)
            continue
        print(f"crawling {ongoing_seeds} Tracker{'s' if ongoing_seeds > 1 else ''}.")
        url_list = ", ".join(f"'{seed[0]}'" for seed in unfinished_seeds)
        old_player_data_per_url = {seed[0]:[] for seed in unfinished_seeds}
        old_total_data_per_url = {seed[0]:[] for seed in unfinished_seeds}
        cursor.execute(f"SELECT * FROM stats LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
                       f"url AS ts_url FROM stats WHERE url in ({url_list}) GROUP BY number, url) AS Ts ON "
                       f"stats.timestamp = Ts.time AND stats.number = Ts.ts_number AND stats.url = Ts.ts_url WHERE "
                       f"stats.url in ({url_list}) AND stats.timestamp = Ts.time AND stats.number = Ts.ts_number")
        combined_old_player_data = cursor.fetchall()
        for old_player_data in combined_old_player_data:
            old_player_data_per_url[old_player_data[0]].append(old_player_data)
        cursor.execute(f"SELECT * FROM Stats_Total LEFT JOIN (SELECT max(timestamp) AS time, number AS ts_number, "
                          f"url AS ts_url FROM Stats_Total WHERE url in ({url_list}) GROUP BY number, url) AS Ts ON "
                          f"Stats_Total.timestamp = Ts.time AND Stats_Total.number = Ts.ts_number AND Stats_Total.url = "
                          f"Ts.ts_url WHERE Stats_Total.url in ({url_list}) AND Stats_Total.timestamp = Ts.time AND "
                          f"Stats_Total.number = Ts.ts_number")
        combined_old_total_data = cursor.fetchall()
        for old_total_data in combined_old_total_data:
            old_total_data_per_url[old_total_data[0]].append(old_total_data)
        unfinished_seeds_tasks = []
        unfinished_crawl_tasks = []
        def chunk(list, size):
            for i in range(0, len(list), size):
                yield list[i:i + size]
        segments = list(chunk(unfinished_seeds, ongoing_seeds//process_count))
        #
        arg_list = []
        with Pool(process_count) as pool:
            for i in range(0,process_count):
                arg_list.append([crawling_process, segments[i], old_player_data_per_url, old_total_data_per_url])

            p = pool.map_async(just_start_async, arg_list)
            # just_start_async(*arg_list)
            p.wait()
                # p.start()

            # async with httpx.AsyncClient() as client:
            #     for crawl_index, url_tuple in enumerate(unfinished_seeds):
            #         unfinished_crawl_tasks.append(crawl_tracker_from_html(
            #             crawl_index, client, url_tuple[0]))  # time consuming for large rooms
            #     ### return True/False, player_data_dict, tracker_url, task_index, time_spend
            #
            #     # success, capture = await crawl_tracker_from_api(crawl_index, client, url_tuple[0])
            #     # print(f"created all crawling tasks in {time.time() - timer} seconds")
            #
            #     # results = await gather(*unfinished_crawl_tasks)
            #     for future in asyncio.as_completed(unfinished_crawl_tasks):
            #         result = await future
            #         success, player_dict, url, task_index, time_spend = result
            #         _, last_updated, title, checks_done = unfinished_seeds[task_index]
            #         if success:
            #             print(f"start pushing {task_index}")
            #             await main_url_fetch(task_index, url, last_updated, title, checks_done,
            #                          old_player_data_per_url[url], old_total_data_per_url[url], player_dict)
            #         else:
            #             cursor.execute(f"UPDATE Trackers SET finished = 'x', end_time = "
            #                              f"'{datetime.now(pytz_timezone('Europe/Berlin'))}' WHERE url = '{url}';")
            #             db.commit()
            #     print(f"all crawling tasks processed in {time.time() - timer} seconds")



            # print(f"time taken to capture: {time.time() - timer}")
            # for i, url_tuple in enumerate(unfinished_seeds):
            #     #print(f"create task for unfinished seed {i}")
            #     url, last_updated, title, checks_done = url_tuple
            #     if results[i][0]:
            #         unfinished_seeds_tasks.append(main_url_fetch(i, url, last_updated, title, checks_done,
            #                          old_player_data_per_url[url],  old_total_data_per_url[url], results[i][1]))
            # print(f"finished creating all {task_index} tasks")
            # results = await gather(*unfinished_seeds_tasks)
        print("all unfinished seeds processed")

        print("time taken for total: ", time.time() - timer)
        sleep_time = (int(60 - (time.time() - timer)) + 1)
        print(f"sleeping for {sleep_time} seconds")
        time.sleep(sleep_time) if sleep_time > 0 else None
    print("Program ended")

def just_start_async(args):
    func, *arg = args
    # print(func, *arg)
    run(func(*arg))

if __name__ == "__main__":
    main()

# https://docs.google.com/spreadsheets/d/16dS6P6IV7a1jN9QzUkySEPSbqXUw4rb0-yYtqcdKk0Y/ copy of big async sheet
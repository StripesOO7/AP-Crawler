import os.path
import time
from datetime import datetime
import sqlite3

from requests import request
from bs4 import BeautifulSoup as bs
from bs4 import element

'''
CREATE TABLE Trackers(url TEXT PRIMARY KEY , finished TEXT);
CREATE TABLE Stats(url TEXT, timestamp REAL, number INTEGER, name TEXT, game_name TEXT, checks_done INTEGER, 
checks_total INTEGER, percentage REAL, connection_status TEXT) 
'''


def add_playerinfo_to_dict(player_dict, info_list, timestamp):
    # print(info_list)
    info_list[4] = info_list[4].split('/')
    player_dict[info_list[0]] = {
        'number': int(info_list[0]),
        'name': info_list[1].replace("'", "''"),
        'game_name': info_list[2].replace("'", "''"),
        'connection_status': info_list[3],
        'checks_done': int(info_list[4][0]),
        'checks_total': int(info_list[4][1]),
        'percentage': float(info_list[5]),
        'timestamp': timestamp
    }


def add_old_playerinfo_to_dict(player_dict, info_list):
    # print(info_list)
    player_dict[info_list[2]] = {
        'number': int(info_list[2]),
        'name': info_list[3].replace("'", "''"),
        'game_name': info_list[4].replace("'", "''"),
        'connection_status': info_list[8],
        'checks_done': int(info_list[5]),
        'checks_total': int(info_list[6]),
        'percentage': float(info_list[7]),
        'timestamp': info_list[1]
    }


def crawl_tracker(tracker_url: str) -> dict[str, any]:
    tracker_page = request('get', tracker_url)

    tracker_html = bs(tracker_page.text, 'html.parser')

    timestamp = datetime.timestamp(datetime.now())
    player_data_dict = {}
    for player in tracker_html.find('tbody').contents:
        tmp = []
        for player_data in player.contents:
            if isinstance(player_data, element.Tag):
                tmp.append(player_data.get_text(strip=True))
        add_playerinfo_to_dict(player_data_dict, tmp, timestamp)

    tmp = ['0']
    for total_data in tracker_html.find('tfoot').contents[1].contents:
        if isinstance(total_data, element.Tag):
            tmp.append(total_data.get_text(strip=True))
    total_check = tmp[3].split(' ')[0].split('/')
    if total_check[0] == total_check[1]:
        tmp[3] = 'Done'
    else:
        tmp[3] = 'Ongoing'
    add_playerinfo_to_dict(player_data_dict, tmp, timestamp)

    return player_data_dict


def push_to_db(db_connector, db_cursor, tracker_url):
    timer = time.time()
    capture = crawl_tracker(tracker_url)
    print("time taken to capture: ", time.time() - timer)
    timer = time.time()
    old_player_data = db_cursor.execute(f"SELECT * FROM Stats JOIN (SELECT max(timestamp) AS time, number, "
                                        f"url FROM Stats WHERE url = '{tracker_url}' GROUP BY number) AS Ts ON "
                                        "Stats.timestamp = Ts.time AND Stats.number = Ts.number AND Stats.url = "
                                        "Ts.url").fetchall()
    print(f"time taken to fetch old data: {time.time() - timer}")
    old_player_data_dict = {}
    for row in old_player_data:
        add_old_playerinfo_to_dict(old_player_data_dict, row)
    # print(len(capture))
    timer = time.time()
    for index, _ in enumerate(old_player_data):
        if old_player_data_dict[index]['checks_done'] == capture[f'{index}']["checks_done"] and old_player_data_dict[
            index]['connection_status'] == capture[f'{index}']["connection_status"]:
            del capture[f'{index}']
    print(f"time taken to compare old data to new data: {time.time() - timer}")
    # print(capture)
    # print(len(capture))
    if capture:
        timer = time.time()
        query = ('INSERT INTO Stats (timestamp, url, number, name, game_name, checks_done, checks_total, percentage, '
                 'connection_status) VALUES ')
        for index, data, in capture.items():
            query = query + (f"({data['timestamp']}, '{tracker_url}', {data['number']}, '{data['name']}', "
                             f"'{data['game_name']}', {data['checks_done']}, {data['checks_total']},"
                             f" {data['percentage']}, '{data['connection_status']}'),")
        db_cursor.execute(query[:-1])

        print("time taken for database push: ", time.time() - timer, "items pushed:", len(capture))

        if '0' in capture.keys() and capture['0']["checks_done"] == capture['0']["checks_total"]:
            db_cursor.execute(f"UPDATE Trackers SET finished = 'x' WHERE url = '{tracker_url}';")
            print(f"Seed with Tracker at {tracker_url} has finished")

    db_connector.commit()
    return


def create_table_if_needed(db_connector, db_cursor):
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Trackers(url TEXT PRIMARY KEY , finished TEXT);")
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Stats(url TEXT, timestamp REAL, number INTEGER, name TEXT, "
                      "game_name TEXT, checks_done INTEGER, checks_total INTEGER, percentage REAL, connection_status TEXT) ")
    db_connector.commit()


if __name__ == "__main__":
    db = sqlite3.connect("AP-Crawler.db")
    cursor = db.cursor()
    create_table_if_needed(db, cursor)
    while True:

        with open(f'{os.path.curdir}/new_trackers.txt', 'r') as new_trackers:
            new_tracker_urls = new_trackers.readlines()
            for new_url in new_tracker_urls:
                cursor.execute(f"INSERT INTO Trackers VALUES ('{new_url}', '');")
                print(f"added {new_url} to database")
            db.commit()
        if len(new_tracker_urls) > 0:
            with open(f'{os.path.curdir}/new_trackers.txt', 'w') as new_trackers:
                new_trackers.write("")

        timer = time.time()

        get_unfinished_seeds_querey = "SELECT URL FROM Trackers WHERE NOT finished = 'x';"
        unfinished_seeds = cursor.execute(get_unfinished_seeds_querey).fetchall()
        ongoing_seeds = len(unfinished_seeds)
        if ongoing_seeds == 0:
            break
        print(f"crawling {len(unfinished_seeds)} Tracker(s).")
        for url in unfinished_seeds:
            try:
                push_to_db(db, cursor, url[0])
            except:
                print(f"Error und push_to_db for URL {url[0]}")
                db.close()
                db = sqlite3.connect("AP-Crawler.db")
                cursor = db.cursor()

        print("time taken for total: ", time.time() - timer)
        sleep_time = (int(60 - (time.time() - timer)) + 1)
        print(f"sleeping for {sleep_time} seconds")
        time.sleep(sleep_time if sleep_time > 0 else 0)
    db.close()
    print("connection closed")
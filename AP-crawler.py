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
checks_total INTEGER, percentage REAL) 
'''


def add_playerinfo_to_dict(player_dict, info_list, timestamp):
    print(info_list)
    info_list[4] = info_list[4].split('/')
    player_dict[info_list[0]] = {
        'number': int(info_list[0]),
        'name': info_list[1].replace("'", "''"),
        'game_name': info_list[2].replace("'", "''"),
        'checks_done': int(info_list[4][0]),
        'checks_total': int(info_list[4][1]),
        'percentage': float(info_list[5]),
        'timestamp': timestamp
    }

def crawl_tracker(tracker_url:str) -> dict[str, any]:
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
    add_playerinfo_to_dict(player_data_dict, tmp, timestamp)

    return player_data_dict


def push_to_db(db_connector, db_cursor, tracker_url):
    timer = time.time()
    capture = crawl_tracker(tracker_url)
    print("time taken to capture: ", time.time() - timer)

    timer = time.time()
    querey = ('INSERT INTO Stats (timestamp, url, number, name, game_name, checks_done, checks_total, percentage) VALUES ')
    for index, data, in capture.items():
        querey = querey + (f"({data['timestamp']}, '{tracker_url}', {data['number']}, '{data['name']}', "
                           f"'{data['game_name']}', {data['checks_done']}, {data['checks_total']},"
                           f" {data['percentage']}),")
    db_cursor.execute(querey[:-1])

    print("time taken for database push: ", time.time() - timer)


    if capture['0']["checks_done"] == capture['0']["checks_total"]:
        db_cursor.execute(f"UPDATE Trackers SET finished = 'x' WHERE url = '{tracker_url}';")
        print(f"Seed with Tracker at {tracker_url} has finished")

    db_connector.commit()
    return

def create_table_if_needed(db_connector, db_cursor):
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Trackers(url TEXT PRIMARY KEY , finished TEXT);")
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Stats(url TEXT, timestamp REAL, number INTEGER, name TEXT, "
                      "game_name TEXT, checks_done INTEGER, checks_total INTEGER, percentage REAL) ")
    db.commit()


if __name__ == "__main__":
    db = sqlite3.connect("AP-Crawler.db")
    cursor = db.cursor()
    create_table_if_needed(db, cursor)
    while True:
        timer = time.time()

        get_unfinished_seeds_querey = "SELECT URL FROM Trackers WHERE NOT finished = 'x';"
        unfinished_seeds = cursor.execute(get_unfinished_seeds_querey).fetchall()
        ongoing_seeds = len(unfinished_seeds)
        if ongoing_seeds == 0:
            break

        for url in unfinished_seeds:
            push_to_db(db, cursor, url[0])
        print("time taken for total: ", time.time() - timer)
        with open(f'{os.path.curdir}/new_trackers.txt', 'r') as new_trackers:
            for new_url in new_trackers.readlines():
                cursor.execute(f"INSERT INTO Trackers VALUES ('{new_url}', '');")
                print(f"added {new_url} to database")
            db.commit()
        with open(f'{os.path.curdir}/new_trackers.txt', 'w') as new_trackers:
            new_trackers.write("")

        print(f"sleeping for {int(60-(time.time() - timer))+1} seconds")
        time.sleep(int(60-(time.time() - timer))+1)
    db.close()
    print("connection closed")

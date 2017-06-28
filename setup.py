#!/usr/bin/python3
import sqlite3
import csv

platforms = set()
maps = set()
gamemodes = set()
objectives = set()
roles = set()
operators = set()
items = set()
ctus = set()
skillranks = set()

def main():
    db = sqlite3.connect('data/data.sqlite3');
    create_tables(db)
    collect_values(db)
    db.commit()
    db.close()

def create_tables(db):
    c = db.cursor()

    # Platforms
    c.execute('CREATE TABLE platform (platform_id INT PRIMARY KEY, name TEXT)')

    # Operators
    c.execute('CREATE TABLE ctu (ctu_id INT PRIMARY_KEY, name TEXT)')
    c.execute('CREATE TABLE role (role_id INT PRIMARY KEY, name TEXT)')
    c.execute('''CREATE TABLE operator (
              operator_id INT PRIMARY KEY, name TEXT, ctu_id INT, role_id INT,
              FOREIGN KEY (ctu_id) REFERENCES ctu(ctu_id),
              FOREIGN KEY (role_id) REFERENCES role(role_id)
              )''')

    # Maps
    c.execute('CREATE TABLE map (map_id INT PRIMARY_KEY, name TEXT)')
    c.execute('CREATE TABLE gamemode (gamemode_id INT PRIMARY_KEY, name TEXT)')
    c.execute('''CREATE TABLE objective (
              objective_id INT PRIMARY KEY, map_id INT, gamemode_id INT, name TEXT,
              FOREIGN KEY (map_id) REFERENCES map(map_id),
              FOREIGN KEY (gamemode_id) REFERENCES gamemode(gamemode_id)
              )''')

    # Items
    c.execute('CREATE TABLE item (item_id INT PRIMARY KEY, name TEXT)')

    # Ranks
    c.execute('CREATE TABLE skillrank (skillrank_id INT PRIMARY KEY, name TEXT)')

    # Statistics
    c.execute('''CREATE TABLE stat_objective (
              stat_id INT PRIMARY KEY, platform_id INT, date TEXT, objective_id INT, operator_id INT, wins INT, kills INT, deaths INT, picks INT, skillrank_id INT,
              FOREIGN KEY (platform_id) REFERENCES platform(platform_id),
              FOREIGN KEY (objective_id) REFERENCES objective(objective_id),
              FOREIGN KEY (operator_id) REFERENCES operator(operator_id),
              FOREIGN KEY (skillrank_id) REFERENCES skillrank(skillrank_id)
              )''')
    c.execute('''CREATE TABLE stat_loadout (
              stat_id INT PRIMARY KEY, platform_id INT, date TEXT, operator_id INT, primaryweapon_id INT, sidearm_id INT, gadget_id INT, wins INT, kills INT, deaths INT, picks INT,
              FOREIGN KEY (platform_id) REFERENCES platform(platform_id),
              FOREIGN KEY (operator_id) REFERENCES operator(operator_id),
              FOREIGN KEY (primaryweapon_id) REFERENCES item(item_id),
              FOREIGN KEY (sidearm_id) REFERENCES item(item_id),
              FOREIGN KEY (gadget_id) REFERENCES item(item_id)
              )''')
    db.commit()

def collect_values(db):
    collect_objectives(db)

def collect_objectives(db):
    c = db.cursor()

    with open('data/objectives.csv', newline='', encoding='latin-1') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        header = next(reader)
        cols = get_col_ids(header)
        stat_id = 1
        for row in reader:
            platform = row[cols['platform']]
            if platform not in platforms:
                add_platform(db, platform)

            gamemode = row[cols['gamemode']]
            if gamemode not in gamemodes:
                add_gamemode(db, gamemode)

            mapname = row[cols['mapname']]
            if mapname not in maps:
                add_map(db, mapname)

            objective_location = row[cols['objectivelocation']]
            objective = '{};{};{}'.format(mapname, gamemode, objective_location)
            if objective not in objectives:
                add_objective(db, objective_location, mapname, gamemode)

            role = row[cols['role']]
            if role not in roles:
                add_role(db, role)

            operator = row[cols['operator']]
            if operator not in operators:
                add_operator(db, operator, role)
            ctu, operator = operator.split('-')

            skillrank = row[cols['skillrank']]
            if skillrank not in skillranks:
                add_skillrank(db, skillrank)

            dateid = row[cols['dateid']]
            date = '{}-{}-{}'.format(dateid[0:4], dateid[4:6], dateid[6:8])

            wins = row[cols['nbwins']]
            kills = row[cols['nbkills']]
            deaths = row[cols['nbdeaths']]
            picks = row[cols['nbpicks']]

            values = (stat_id, date, wins, kills, deaths, picks, platform, objective_location, mapname, gamemode, operator, ctu, skillrank)
            sql = '''
            INSERT INTO stat_objective (stat_id, date, wins, kills, deaths, picks, platform_id, objective_id, operator_id, skillrank_id)
            SELECT
                ? AS stat_id,
                ? AS date,
                ? AS wins,
                ? AS kills,
                ? AS deaths,
                ? AS picks,
                (
                    SELECT
                        p.platform_id
                    FROM platform p
                    WHERE p.name = ?
                ) AS platform_id,
                (
                    SELECT
                        o.objective_id
                    FROM objective o
                    JOIN map m ON m.map_id = o.map_id
                    JOIN gamemode gm ON gm.gamemode_id = o.gamemode_id
                    WHERE o.name = ?
                      AND m.name = ?
                      AND gm.name = ?
                ) AS objective_id,
                (
                    SELECT
                        o.operator_id
                    FROM operator o
                    JOIN ctu c ON c.ctu_id = o.ctu_id
                    WHERE o.name = ?
                      AND c.name = ?
                ) AS operator_id,
                (
                    SELECT
                        s.skillrank_id
                    FROM skillrank s
                    WHERE s.name = ?
                ) AS skillrank_id
            '''
            c.execute(sql, values)
            stat_id += 1

def add_platform(db, name):
    c = db.cursor()
    c.execute('INSERT INTO platform (platform_id, name) VALUES (?, ?)', (len(platforms) + 1, name))
    platforms.add(name)

def add_gamemode(db, name):
    c = db.cursor()
    c.execute('INSERT INTO gamemode (gamemode_id, name) VALUES (?, ?)', (len(gamemodes) + 1, name))
    gamemodes.add(name)

def add_map(db, name):
    c = db.cursor()
    c.execute('INSERT INTO map (map_id, name) VALUES (?, ?)', (len(maps) + 1, name))
    maps.add(name)

def add_objective(db, objective, mapname, gamemode):
    c = db.cursor()
    values = (len(objectives) + 1, objective, mapname, gamemode)
    c.execute('''INSERT INTO objective (objective_id, map_id, gamemode_id, name)
              SELECT
                ? AS objective_id,
                m.map_id AS map_id,
                gm.gamemode_id AS gamemode_id,
                ? AS name
              FROM map m, gamemode gm
              WHERE m.name = ? AND gm.name = ?''', values)
    objectives.add('{};{};{}'.format(mapname, gamemode, objective))

def add_role(db, name):
    c = db.cursor()
    c.execute('INSERT INTO role (role_id, name) VALUES (?, ?)', (len(roles) + 1, name))
    roles.add(name)

def add_operator(db, operator, role):
    c = db.cursor()
    ctu, name = operator.split('-')
    if ctu not in ctus:
        add_ctu(db, ctu)

    values = (len(operators) + 1, name, ctu, role)
    c.execute('''INSERT INTO operator (operator_id, name, ctu_id, role_id)
              SELECT
                ? AS operator_id,
                ? AS name,
                c.ctu_id AS ctu_id,
                r.role_id AS role_id
              FROM ctu c, role r
              WHERE c.name = ? AND r.name = ?''', values)
    operators.add(operator)

def add_ctu(db, name):
    c = db.cursor()
    c.execute('INSERT INTO ctu (ctu_id, name) VALUES (?, ?)', (len(ctus) + 1, name))
    ctus.add(name)

def add_skillrank(db, name):
    c = db.cursor()
    c.execute('INSERT INTO skillrank (skillrank_id, name) VALUES (?, ?)', (len(skillranks) + 1, name))
    skillranks.add(name)

def get_col_ids(row):
    ids = {}
    i = 0
    for col in row:
        ids[col] = i
        i += 1

    return ids

if __name__ == '__main__':
    main()

#!/usr/bin/python3
import sqlite3
import csv

# These sets track which pieces of information are already in the database
platforms = set()
maps = set()
gamemodes = set()
objectives = set()
roles = set()
operators = set()
items = set()
ctus = set()
skillranks = set()

import_type = 'objectives'

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

    # Indexes
    c.execute('CREATE INDEX idx_platform_name ON platform(name)')
    c.execute('CREATE INDEX idx_ctu_name ON ctu(name)')
    c.execute('CREATE INDEX idx_role_name ON role(name)')
    c.execute('CREATE INDEX idx_map_name ON map(name)')
    c.execute('CREATE INDEX idx_gamemode_name ON gamemode(name)')
    c.execute('CREATE INDEX idx_objective_name ON objective(name)')
    c.execute('CREATE INDEX idx_skillrank_name ON skillrank(name)')

    db.commit()

def collect_values(db):
    collect_from_file(db, 'data/objectives.csv')

def collect_from_file(db, filename):
    c = db.cursor()

    with open(filename, newline='', encoding='latin-1') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        # Get the first line and get an index for every column name
        header = next(reader)
        cols = get_col_ids(header)

        stat_id = 1 # Primary key in the stat table
        for arr in reader:
            row = get_row_dict(arr, cols)

            if import_type == 'objectives':
                collect_objective_data(db, row, stat_id)

            stat_id += 1

def collect_objective_data(db, row, stat_id):
    c = db.cursor()

    add_platform(db, row['platform'])
    add_gamemode(db, row['gamemode'])
    add_map(db, row['mapname'])
    add_role(db, row['role'])
    add_ctu(db, row['ctu'])
    add_operator(db, row['operator'], row['ctu'], row['role'])
    add_skillrank(db, row['skillrank'])
    add_objective(db, row['objectivelocation'], row['mapname'], row['gamemode'])

    values = (stat_id, row['date'], row['nbwins'], row['nbkills'], row['nbdeaths'], row['nbpicks'], row['platform'], row['objectivelocation'], row['mapname'], row['gamemode'], row['operator'], row['ctu'], row['skillrank'])
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

# The add_* functions insert a single piece of data in its corresponding table.
# It then adds it to the set so that it appears only once.

def add_platform(db, platform):
    if platform not in platforms:
        c = db.cursor()
        c.execute('INSERT INTO platform (platform_id, name) VALUES (?, ?)', (len(platforms) + 1, platform))
        platforms.add(platform)

def add_gamemode(db, gamemode):
    if gamemode not in gamemodes:
        c = db.cursor()
        c.execute('INSERT INTO gamemode (gamemode_id, name) VALUES (?, ?)', (len(gamemodes) + 1, gamemode))
        gamemodes.add(gamemode)

def add_map(db, mapname):
    if mapname not in maps:
        c = db.cursor()
        c.execute('INSERT INTO map (map_id, name) VALUES (?, ?)', (len(maps) + 1, mapname))
        maps.add(mapname)

def add_objective(db, objectivelocation, mapname, gamemode):
    s = '{};{};{}'.format(objectivelocation, mapname, gamemode)

    if s not in objectives:
        c = db.cursor()
        values = (len(objectives) + 1, objectivelocation, mapname, gamemode)
        c.execute('''
        INSERT INTO objective (objective_id, map_id, gamemode_id, name)
            SELECT
                ? AS objective_id,
                m.map_id AS map_id,
                gm.gamemode_id AS gamemode_id,
                ? AS name
            FROM map m, gamemode gm
            WHERE m.name = ? AND gm.name = ?
        ''', values)
        objectives.add(s)

def add_role(db, role):
    if role not in roles:
        c = db.cursor()
        c.execute('INSERT INTO role (role_id, name) VALUES (?, ?)', (len(roles) + 1, role))
        roles.add(role)

def add_operator(db, operator, ctu, role):
    s = '{};{};{}'.format(operator, ctu, role)

    if s not in operators:
        c = db.cursor()
        values = (len(operators) + 1, operator, ctu, role)
        c.execute('''
        INSERT INTO operator (operator_id, name, ctu_id, role_id)
              SELECT
                ? AS operator_id,
                ? AS name,
                c.ctu_id AS ctu_id,
                r.role_id AS role_id
            FROM ctu c, role r
            WHERE c.name = ? AND r.name = ?
        ''', values)
        operators.add(s)

def add_ctu(db, ctu):
    if ctu not in ctus:
        c = db.cursor()
        c.execute('INSERT INTO ctu (ctu_id, name) VALUES (?, ?)', (len(ctus) + 1, ctu))
        ctus.add(ctu)

def add_skillrank(db, skillrank):
    if skillrank not in skillranks:
        c = db.cursor()
        c.execute('INSERT INTO skillrank (skillrank_id, name) VALUES (?, ?)', (len(skillranks) + 1, skillrank))
        skillranks.add(skillrank)

# Returns a dict with column names as indexes and numeric indexes as values.
# Here is an example:
# { 'platform': 0, 'dateid': 1, ... }
def get_col_ids(row):
    ids = {}
    i = 0
    for col in row:
        ids[col] = i
        i += 1

    return ids

# Returns a dict with column names as indexes and the CSV values as values
# Here is an example:
# { 'platform': 'PC', 'dateid': '20170510', ... }
def get_row_dict(arr, cols):
    row = {}
    for key in cols:
        row[key] = arr[cols[key]]

    if 'operator' in row:
        row['ctu'], row['operator'] = row['operator'].split('-')

    if 'dateid' in row:
        row['date'] = '{}-{}-{}'.format(row['dateid'][0:4], row['dateid'][4:6], row['dateid'][6:8])

    return row

if __name__ == '__main__':
    main()

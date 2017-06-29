#!/usr/bin/python3
import sqlite3
import csv
import argparse
import os

# These sets track which pieces of information are already in the database
platforms = set()
maps = set()
gamemodes = set()
objectives = set()
roles = set()
operators = set()
ctus = set()
skillranks = set()
matches = set()
rounds = set()
endroundreasons = set()
weapontypes = set()
weapons = set()
attachments = set()
sight_attachments = set()
grip_attachments = set()
underbarrel_attachments = set()
barrel_attachments = set()
gadgets = set()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Import a CSV file into an SQLite3 database')
    parser.add_argument('filename', metavar='FILENAME', type=str, help='A CSV file provided by Ubisoft')
    parser.add_argument('-o', dest='output', metavar='OUTFILE', help='The name of the SQLite3 db file')
    parser.add_argument('-f', '--force', dest='force', action='store_const', const=True, default=False, help='Overwrite an existing database file')
    args = parser.parse_args()

    if not os.path.isfile(args.filename):
        print('File not found: "{}"'.format(args.filename))
        return

    if args.output is None:
        args.output = os.path.splitext(args.filename)[0] + '.sqlite'

    if os.path.isfile(args.output) and args.force == False:
        print('"{}" already exists. Use -f to overwrite.'.format(args.output))
        return
    elif os.path.isfile(args.output) and args.force == True:
        os.remove(args.output)

    db = sqlite3.connect(args.output);
    create_tables(db)
    collect_values(db, args.filename)
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

    # Ranks
    c.execute('CREATE TABLE skillrank (skillrank_id INT PRIMARY KEY, name TEXT)')

    # Matches and rounds
    c.execute('''
    CREATE TABLE match (
        match_id INT PRIMARY KEY,
        map_id INT,
        gamemode_id INT,
        date TEXT,
        FOREIGN KEY (map_id) REFERENCES map(map_id),
        FOREIGN KEY (gamemode_id) REFERENCES gamemode(gamemode_id)
    )
    ''')

    c.execute('''
    CREATE TABLE endroundreason (
        endroundreason_id INT PRIMARY KEY,
        name TEXT
    )
    ''')

    c.execute('''
    CREATE TABLE round (
        round_id INT PRIMARY KEY,
        round_num INT,
        match_id INT,
        objective_id INT,
        winrole_id INT,
        endroundreason_id INT,
        duration INT,
        FOREIGN KEY (match_id) REFERENCES match(match_id),
        FOREIGN KEY (objective_id) REFERENCES objective(objective_id),
        FOREIGN KEY (winrole_id) REFERENCES role(role_id),
        FOREIGN KEY (endroundreason_id) REFERENCES endroundreason(endroundreason_id)
    )
    ''')

    # Weapons and items
    c.execute('''
    CREATE TABLE weapontype (
        weapontype_id INT PRIMARY KEY,
        name TEXT
    )
    ''')
    c.execute('''
    CREATE TABLE weapon (
        weapon_id INT PRIMARY KEY,
        weapontype_id INT,
        name TEXT,
        FOREIGN KEY (weapontype_id) REFERENCES weapontype(weapontype_id)
    )
    ''')
    c.execute('''
    CREATE TABLE attachment (
        attachment_id INT PRIMARY KEY,
        name TEXT
    )
    ''')
    c.execute('''
    CREATE TABLE attachment_sight (
        attachment_id INT PRIMARY KEY,
        FOREIGN KEY (attachment_id) REFERENCES attachment(attachment_id)
    )
    ''')
    c.execute('''
    CREATE TABLE attachment_grip (
        attachment_id INT PRIMARY KEY,
        FOREIGN KEY (attachment_id) REFERENCES attachment(attachment_id)
    )
    ''')
    c.execute('''
    CREATE TABLE attachment_underbarrel (
        attachment_id INT PRIMARY KEY,
        FOREIGN KEY (attachment_id) REFERENCES attachment(attachment_id)
    )
    ''')
    c.execute('''
    CREATE TABLE attachment_barrel (
        attachment_id INT PRIMARY KEY,
        FOREIGN KEY (attachment_id) REFERENCES attachment(attachment_id)
    )
    ''')
    c.execute('''
    CREATE TABLE gadget (
        gadget_id INT PRIMARY KEY,
        name TEXT
    )
    ''')

    # Statistics

    # Indexes
    c.execute('CREATE INDEX idx_platform_name ON platform(name)')
    c.execute('CREATE INDEX idx_ctu_name ON ctu(name)')
    c.execute('CREATE INDEX idx_role_name ON role(name)')
    c.execute('CREATE INDEX idx_map_name ON map(name)')
    c.execute('CREATE INDEX idx_gamemode_name ON gamemode(name)')
    c.execute('CREATE INDEX idx_objective_name ON objective(name)')
    c.execute('CREATE INDEX idx_skillrank_name ON skillrank(name)')
    c.execute('CREATE INDEX idx_weapontype_name ON weapontype(name)')
    c.execute('CREATE INDEX idx_weapon_name ON weapon(name)')
    c.execute('CREATE INDEX idx_attachment_name ON attachment(name)')
    c.execute('CREATE INDEX idx_gadget_name ON gadget(name)')

    db.commit()

def collect_values(db, filename):
    collect_from_file(db, filename)

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
            collect_from_row(db, row, stat_id)
            stat_id += 1

def collect_from_row(db, row, stat_id):
    add_platform(db, row['platform'])
    add_gamemode(db, row['gamemode'])
    add_map(db, row['mapname'])
    add_objective(db, row['objectivelocation'], row['mapname'], row['gamemode'])
    add_match(db, row['matchid'], row['date'], row['mapname'], row['gamemode'])
    add_endroundreason(db, row['endroundreason'])
    add_role(db, row['winrole'])
    add_round(db, row['roundnumber'], row['matchid'], row['objectivelocation'], row['mapname'], row['gamemode'], row['winrole'], row['endroundreason'], row['roundduration'])
    add_skillrank(db, row['skillrank'])
    add_role(db, row['role'])
    add_ctu(db, row['ctu'])
    add_operator(db, row['operator'], row['ctu'], row['role'])
    add_weapontype(db, row['primaryweapontype'])
    add_weapon(db, row['primaryweapon'], row['primaryweapontype'])
    add_attachment_sight(db, row['primarysight'])
    add_attachment_grip(db, row['primarygrip'])
    add_attachment_underbarrel(db, row['primaryunderbarrel'])
    add_attachment_barrel(db, row['primarybarrel'])
    add_weapontype(db, row['secondaryweapontype'])
    add_weapon(db, row['secondaryweapon'], row['secondaryweapontype'])
    add_attachment_sight(db, row['secondarysight'])
    add_attachment_grip(db, row['secondarygrip'])
    add_attachment_underbarrel(db, row['secondaryunderbarrel'])
    add_attachment_barrel(db, row['secondarybarrel'])
    add_gadget(db, row['secondarygadget'])

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

def add_weapontype(db, weapontype):
    if weapontype not in weapontypes:
        c = db.cursor()
        c.execute('INSERT INTO weapontype (weapontype_id, name) VALUES (?, ?)', (len(weapontypes) + 1, weapontype))
        weapontypes.add(weapontype)

def add_weapon(db, weapon, weapontype):
    if weapon not in weapons:
        c = db.cursor()
        values = (len(weapons) + 1, weapon, weapontype)
        c.execute('''
        INSERT INTO weapon (weapon_id, name, weapontype_id)
            SELECT
                ? AS weapon_id,
                ? AS name,
                t.weapontype_id AS weapontype_id
            FROM weapontype t
            WHERE t.name = ?
        ''', values)
        weapons.add(weapon)

def add_attachment(db, attachment):
    if attachment == 'None':
        return

    if attachment not in attachments:
        c = db.cursor()
        c.execute('INSERT INTO attachment (attachment_id, name) VALUES (?, ?)', (len(attachments) + 1, attachment))
        attachments.add(attachment)

def add_attachment_sight(db, attachment):
    if attachment == 'None':
        return

    if attachment not in sight_attachments:
        add_attachment(db, attachment)
        c = db.cursor()
        c.execute('''
        INSERT INTO attachment_sight (attachment_id)
            SELECT
                a.attachment_id
            FROM attachment a
            WHERE a.name = ?
        ''', (attachment,))
        sight_attachments.add(attachment)

def add_attachment_grip(db, attachment):
    if attachment == 'None':
        return

    if attachment not in grip_attachments:
        add_attachment(db, attachment)
        c = db.cursor()
        c.execute('''
        INSERT INTO attachment_grip (attachment_id)
            SELECT
                a.attachment_id
            FROM attachment a
            WHERE a.name = ?
        ''', (attachment,))
        grip_attachments.add(attachment)

def add_attachment_underbarrel(db, attachment):
    if attachment == 'None':
        return

    if attachment not in underbarrel_attachments:
        add_attachment(db, attachment)
        c = db.cursor()
        c.execute('''
        INSERT INTO attachment_underbarrel (attachment_id)
            SELECT
                a.attachment_id
            FROM attachment a
            WHERE a.name = ?
        ''', (attachment,))
        underbarrel_attachments.add(attachment)

def add_attachment_barrel(db, attachment):
    if attachment == 'None':
        return

    if attachment not in barrel_attachments:
        add_attachment(db, attachment)
        c = db.cursor()
        c.execute('''
        INSERT INTO attachment_barrel (attachment_id)
            SELECT
                a.attachment_id
            FROM attachment a
            WHERE a.name = ?
        ''', (attachment,))
        barrel_attachments.add(attachment)

def add_gadget(db, gadget):
    if gadget not in gadgets:
        c = db.cursor()
        c.execute('INSERT INTO gadget (gadget_id, name) VALUES (?, ?)', (len(gadgets) + 1, gadget))
        gadgets.add(gadget)

def add_match(db, matchid, date, mapname, gamemode):
    if matchid not in matches:
        c = db.cursor()
        values = (matchid, date, mapname, gamemode)
        c.execute('''
        INSERT INTO match (match_id, date, map_id, gamemode_id)
            SELECT
                ? AS match_id,
                ? AS date,
                (
                    SELECT
                        m.map_id AS map_id
                    FROM map m
                    WHERE m.name = ?
                ) AS map_id,
                (
                    SELECT
                        gm.gamemode_id AS gamemode_id
                    FROM gamemode gm
                    WHERE gm.name = ?
                ) AS gamemode_id
        ''', values)
        matches.add(matchid)

def add_round(db, round_num, matchid, objective, mapname, gamemode, winrole, endroundreason, duration):
    s = '{};{}'.format(matchid, round_num)

    if s not in rounds:
        c = db.cursor()
        values = (len(rounds) + 1, round_num, matchid, duration, mapname, gamemode, objective, winrole, endroundreason)
        c.execute('''
        INSERT INTO round (round_id, round_num, match_id, duration, objective_id, winrole_id, endroundreason_id)
            SELECT
                ? AS round_id,
                ? AS round_num,
                ? AS match_id,
                ? AS duration,
                (
                    SELECT
                        o.objective_id
                    FROM objective o
                    JOIN map m ON m.map_id = o.map_id
                    JOIN gamemode gm ON gm.gamemode_id = o.gamemode_id
                    WHERE m.name = ?
                      AND gm.name = ?
                      AND o.name = ?
                ) AS objective_id,
                (
                    SELECT
                        r.role_id
                    FROM role r
                    WHERE r.name = ?
                ) AS winrole_id,
                (
                    SELECT
                        r.endroundreason_id
                    FROM endroundreason r
                    WHERE r.name = ?
                ) AS endroundreason_id
        ''', values)
        rounds.add(s)

def add_endroundreason(db, endroundreason):
    if endroundreason not in endroundreasons:
        c = db.cursor()
        c.execute('INSERT INTO endroundreason (endroundreason_id, name) VALUES (?, ?)', (len(endroundreasons) + 1, endroundreason))
        endroundreasons.add(endroundreason)

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

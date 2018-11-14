import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


def init_db():
    # connect to database
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # drop existing tables
    statement = "DROP TABLE IF EXISTS Bars"
    cur.execute(statement)
    statement = "DROP TABLE IF EXISTS Countries"
    cur.execute(statement)

    conn.commit()

    # create Bars table
    statement = '''
    CREATE TABLE Bars (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Company TEXT,
    SpecificBeanBarName TEXT,
    REF TEXT,
    ReviewDate TEXT,
    CocoaPercent REAL,
    CompanyLocation TEXT,
    CompanyLocationId INTEGER REFERENCES Countries(Id),
    Rating REAL,
    BeanType TEXT,
    BroadBeanOrigin TEXT,
    BroadBeanOriginId INTEGER REFERENCES Countries(Id)
    );
    '''
    # UPDATE Bars SET CompanyLocationId = CompanyLocation
    cur.execute(statement)

    # create Countries table
    statement = '''
    CREATE TABLE Countries (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Alpha2 TEXT,
    Alpha3 TEXT,
    EnglishName TEXT,
    Region TEXT,
    Subregion TEXT,
    Population INTEGER,
    Area REAL
    );
    '''
    cur.execute(statement)

    conn.commit()
    conn.close()

def insert_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # get data from countries json file
    f = open(COUNTRIESJSON, 'r', encoding='utf8')
    contents = f.read()
    contents_list = json.loads(contents) # this is a list of dictionaries
    f.close()

    columns = ["alpha2Code", "alpha3Code", "name", "region", "subregion", "population", "area"]

    for country in contents_list:
        keys = tuple(country[c] for c in columns) # creates a tuple with the data for each country
        statement = '''
        INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)
        VALUES (?,?,?,?,?,?,?)
        '''
        cur.execute(statement, keys)
        conn.commit()

        # read bars data in from csv
    bars_tuples = []

    with open(BARSCSV) as bars_data: # Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin
        csv_reader = csv.reader(bars_data)
        for row in csv_reader:
            # row[5] - CompanyLocation
            statement = '''
            SELECT Id
            FROM Countries
            WHERE EnglishName = \"{}\"
            '''.format(row[5])
            cur.execute(statement)
            result = cur.fetchone()
            try:
                id = result[0]
            except:
                id = None
            row.append(id)

            # row[8] - BroadBeanOrigin
            statement = '''
            SELECT Id
            FROM Countries
            WHERE EnglishName = \"{}\"
            '''.format(row[8])
            cur.execute(statement)
            result = cur.fetchone()
            try:
                id = result[0]
            except:
                id = None
            row.append(id)
            tup = tuple(row)
            bars_tuples.append(tup)

    for bar in bars_tuples[1:]:
        statement = '''
        INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, Rating, BeanType, BroadBeanOrigin, CompanyLocationId, BroadBeanOriginId)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(statement, bar)
        conn.commit()

    conn.commit()
    conn.close()

init_db()
insert_data()

# Part 2: Implement logic to process user commands
def process_command(command):

    words = command.split()

    if len(words) > 0:
        main_command = words[0]
    else:
        main_command = None

    if main_command == "bars":
        pass

    elif main_command == "companies":
        pass

    elif main_command== "countries":
        pass

    elif main_command == "regions":
        pass

    else:
        pass


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

# # Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()

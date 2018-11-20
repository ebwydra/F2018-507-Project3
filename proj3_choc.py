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


def reload_data():
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
    CompanyLocationId INTEGER REFERENCES Countries(Id),
    Rating REAL,
    BeanType TEXT,
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
            row.pop(5)
            row.pop(7)
            # print(row)
            new_cocoa_percent = row[4].strip("%")
            row.pop(4)
            row.append(new_cocoa_percent)
            tup = tuple(row)
            # print(tup)
            bars_tuples.append(tup)

    for bar in bars_tuples[1:]:
        statement = '''
        INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, Rating, BeanType, CompanyLocationId, BroadBeanOriginId, CocoaPercent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(statement, bar)
        conn.commit()

    conn.commit()
    conn.close()

# reload_data()

# Part 2: Implement logic to process user commands

'''
bars
Description: Lists chocolate bars, according the specified parameters.
Parameters:
sellcountry=<alpha2> | sourcecountry=<alpha2> | sellregion=<name> | sourceregion=<name> [default: none]
Description: Specifies a country or region within which to limit the results, and also specifies whether to limit by the seller (or manufacturer) or by the bean origin source.
ratings | cocoa [default: ratings]
Description: Specifies whether to sort by rating or cocoa percentage
top=<limit> | bottom=<limit> [default: top=10]
Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
'''

def bars_function(a=None, b="ratings", c="top=10"):

    if a != None:
        a_spl = a.split("=")
        param = a_spl[0]
        val = a_spl[1]

        if param == "sellcountry":
            which_table = "CompanyLocation"
            countries_col = "Alpha2"

        elif param == "sourcecountry":
            which_table = "OriginLocation"
            countries_col = "Alpha2"

        elif param == "sellregion":
            which_table = "CompanyLocation"
            countries_col = "Region"

        elif param == "sourceregion":
            which_table = "OriginLocation"
            countries_col = "Region"

        else:
            print("Parameter 'a' not recognized.")
            return None

    if b == "ratings":
        order_col = "Rating"
    elif b == "cocoa":
        order_col = "CocoaPercent"
    else:
        print("Parameter 'b' not recognized.")
        return None

    c_spl = c.split("=")
    if c_spl[0] == "top":
        order = "DESC"
        n = c_spl[1]
    elif c_spl[0] == "bottom":
        order = "ASC"
        n = c_spl[1]
    else:
        print("Parameter 'c' not recognized.")
        return None

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    SELECT Bars.SpecificBeanBarName, Bars.Company, CompanyLocation.EnglishName, Bars.Rating, Bars.CocoaPercent, OriginLocation.EnglishName
    FROM Bars
    LEFT JOIN Countries as CompanyLocation
    ON Bars.CompanyLocationId = CompanyLocation.Id
    LEFT JOIN Countries as OriginLocation
    ON Bars.BroadBeanOriginId = OriginLocation.Id
    '''
    
    if a != None:
        statement += '''
        WHERE {}.{} LIKE \"{}\"
        '''.format(which_table, countries_col, val)
    
    statement += '''
    ORDER BY Bars.{} {}
    LIMIT {}
    '''.format(order_col, order, n)

    cur.execute(statement)
    result = cur.fetchall()
    # print("")
    # for r in result:
    #     print(r)
    return result


# print(len(bars_function(b="cocoa")))

'''
companies
Description: Lists chocolate bars sellers according to the specified parameters. Only companies that sell more than 4 kinds of bars are listed in results.
Parameters:
country=<alpha2> | region=<name> [default: none]
Description: Specifies a country or region within which to limit the results.
ratings | cocoa | bars_sold [default: ratings]
Description: Specifies whether to sort by rating, cocoa percentage, or the number of different types of bars sold
top=<limit> | bottom=<limit> [default: top=10]
Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
'''

def companies_function(a=None, b="ratings", c="top=10"):

    if a != None:
        a_spl = a.split("=")
        param = a_spl[0]
        val = a_spl[1]

        if param == "country":
            countries_col = "Alpha2"

        elif param == "region":
            countries_col = "Region"

        else:
            print("Parameter 'a' not recognized.")
            return None

    if b == "ratings":
        last_select = "AVG(Bars.Rating)"
    elif b == "cocoa":
        last_select = "AVG(Bars.CocoaPercent)"
    elif b == "bars_sold":
        last_select = "COUNT(*)"
    else:
        print("Parameter 'b' not recognized.")
        return None

    c_spl = c.split("=")
    if c_spl[0] == "top":
        order = "DESC"
        n = c_spl[1]
    elif c_spl[0] == "bottom":
        order = "ASC"
        n = c_spl[1]
    else:
        print("Parameter 'c' not recognized.")
        return None

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    SELECT Bars.Company, Countries.EnglishName, {}
    FROM Bars
    LEFT JOIN Countries
    ON Bars.CompanyLocationId = Countries.Id
    '''.format(last_select)

    if a != None:
        statement += '''
        WHERE Countries.{} LIKE \"{}\"
        '''.format(countries_col, val)

    statement += '''
    GROUP BY Bars.Company
    HAVING COUNT(*) > 4
    ORDER BY {} {}
    LIMIT {}
    '''.format(last_select, order, n)

    cur.execute(statement)
    result = cur.fetchall()

    # for r in result:
    #     print(r)
    return result

# companies_function(a="region=Europe",b="cocoa")

'''
countries
Description: Lists countries according to specified parameters. Only countries that sell/source more than 4 kinds of bars are listed in results.
Parameters:
region=<name> [default: none]
Description: Specifies a region within which to limit the results.
sellers | sources [default: sellers]
Description: Specifies whether to select countries based sellers or bean sources.
ratings | cocoa | bars_sold [default: ratings]
Description: Specifies whether to sort by rating, cocoa percentage, or the number of different types of bars sold
top=<limit> | bottom=<limit> [default: top=10]
Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
'''

def countries_function(a=None, b="sellers", c="ratings", d="top=10"):
    
    if a!= None:
        a_spl = a.split("=")
        param = a_spl[0]
        val = a_spl[1]

        if param == "region":
            countries_col = "Region"
        else:
            print("Parameter 'a' not recognized.")
            return None

    if b == "sellers":
        join_col = "CompanyLocationId"
    elif b == "sources":
        join_col = "BroadBeanOriginId"
    else:
        print("Parameter 'b' not recognized.")
        return None

    if c == "ratings":
        last_select = "AVG(Bars.Rating)"
    elif c == "cocoa":
        last_select = "AVG(Bars.CocoaPercent)"
    elif c == "bars_sold":
        last_select = "COUNT(*)"
    else:
        print("Parameter 'c' not recognized.")
        return None


    d_spl = d.split("=")
    if d_spl[0] == "top":
        order = "DESC"
        n = d_spl[1]
    elif d_spl[0] == "bottom":
        order = "ASC"
        n = d_spl[1]
    else:
        print("Parameter 'd' not recognized.")
        return None

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    SELECT Countries.EnglishName, Countries.Region, {}
    FROM Bars
    LEFT JOIN Countries
    ON Bars.{} = Countries.Id
    '''.format(last_select, join_col)

    if a != None:
        statement += '''
        WHERE Countries.{} LIKE \"{}\"
        '''.format(countries_col, val)

    statement += '''
    GROUP BY Bars.{}
    HAVING COUNT(*) > 4
    ORDER BY {} {}
    LIMIT {}
    '''.format(join_col, last_select, order, n)

    cur.execute(statement)
    result = cur.fetchall()

    # for r in result:
    #     print(r)
    return result

# countries_function(b="sellers", c="bars_sold", d="top=3")

def regions_function(b="sellers", c="ratings", d="top=10"):

    if b == "sellers":
        join_col = "CompanyLocationId"
    elif b == "sources":
        join_col = "BroadBeanOriginId"
    else:
        print("Parameter 'b' not recognized.")
        return None

    if c == "ratings":
        last_select = "AVG(Bars.Rating)"
    elif c == "cocoa":
        last_select = "AVG(Bars.CocoaPercent)"
    elif c == "bars_sold":
        last_select = "COUNT(*)"
    else:
        print("Parameter 'c' not recognized.")
        return None

    d_spl = d.split("=")
    if d_spl[0] == "top":
        order = "DESC"
    elif d_spl[0] == "bottom":
        order = "ASC"
    else:
        print("Parameter 'c' not recognized.")
        return None
    if len(d_spl) == 2:
        n = d_spl[1]
    else:
        print("Parameter 'd' not recognized.")
        return None

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    SELECT Countries.Region, {}
    FROM Bars
    LEFT JOIN Countries
    ON Bars.{} = Countries.Id
    GROUP BY Countries.Region
    HAVING COUNT(*) > 4
    ORDER BY {} {}
    LIMIT {}
    '''.format(last_select, join_col, last_select, order, n)

    cur.execute(statement)
    result = cur.fetchall()

    for r in result:
        print(r)
    return result

print()
regions_function(b="sources",c="bars_sold")
print()

def process_command(command):
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

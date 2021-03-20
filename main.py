import requests
import methods

# API Address
url = "https://yahoo-finance-low-latency.p.rapidapi.com/v8/finance/chart/AAPL"

# If we wanted, we loop through a few different companies with string formatting like others below.
querystring = {"symbols":"AAPL,MSFT"}

# headers provided by RapidAPI, needed to authenticate who we are.
headers = {
    'x-rapidapi-key': "4055d59597msh18aab29381bc41ep112c52jsnb7ed8606b690",
    'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
}

# Set up the initial SQL statement with the table, columns and then values.
# The unformatted string allows us to enter values later.
sql = "INSERT INTO rherlihy (timestamp, open, volume, high, close, low ) VALUES ({}, {}, {}, {}, {}, {});"

# We are checking if an existing timestamp exists so there are no duplicates.
getSQL = "SELECT * from rherlihy WHERE timestamp = {};"

# Connect to database. We can execute different statements to create new tables/databases as required.
connection = methods.create_db_connection("localhost", "root", "root1234", 'test')

# Get response from API.
response = requests.request("GET", url, headers=headers, params=querystring).json()

timestamp = response["chart"]['result'][0]['timestamp']
allValues = response["chart"]['result'][0]['indicators']['quote'][0]

open = allValues['open']
volume = allValues['volume']
high = allValues['high']
close = allValues['close']
low = allValues['low']

for (a, b, c, d, e, f) in zip(timestamp,open, volume, high, close, low):
    # Check to see if the timestamp already exists.
    select_sql = getSQL.format(a)
    check_timestamps = methods.execute_select_query(connection, select_sql)
    # if it doesn't exist, then put in the data.
    if(check_timestamps == 'None'):
        # Enter each line of data into the statement.
        fullSQL = sql.format(a, b, c, d, e, f)
        # Add data to database.
        methods.execute_query(connection, fullSQL)

connection.close()

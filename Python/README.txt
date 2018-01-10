Based on Python 3.5

Dependencies include: pandas, tweepy

1. STREAMING-Keywords.py 
	- this allows the connection to the streaming API, filtered via specific keywords. 
2. _MySQL_CategorizeNSave.py
	- takes the data from the twitter API and normalizes it into the relevant tables in the MySQL db
3. _DB_connection.py
	- the main Input/Output with the database for all python scripts. Takes prepared statements, saves, or extracts data as needed
4. _Twitter_downloader.py
	- uses the SEARCH (REST) API to query specific statuses based on the tweet id

Note, scripts marked as beggining with an underscore are supporting scripts

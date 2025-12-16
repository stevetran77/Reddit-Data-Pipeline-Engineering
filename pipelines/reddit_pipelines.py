def reddit_pipeline (file_name: str, subreddit: str, time_filter = 'day',limit=None):
    #Connecting to Reddit Instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    #Extraction
    #Transformation
    #Loadingtocsv
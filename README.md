# A TikTok ETL DAG

## Abstract
This script iterates through the various categories in TikTok's explore page and makes a request of 30 videos from each category to TikTok's API. 
The request's response is a JSON object containing the post's metadata, some of the user data, video & audio metadata, and stats like views count, shares count, saved count, hashtags, etc.

## Extract Task
Selenium creates a headless webdriver and iterates through a dictionary that contains the categories information (category_name, category_id), making a request for 30 videos of each category,
then beautifulsoup4 parses the request's response (JSON) and loads it into a dictionary. For each element (post/video) in the response's dictionary, the script extracts 
user data (username, follower/following count, etc), post data (likes count, views count, shares count, etc), post metadata (duration, hashtags, date, etc) and stores it in
multiple temp CSV files.

## Transform task
The transformation happens inside the extract task, each element's (post/video) date is transformed from epoch to datetime and split into day, month, year, day of week and quarter.

## Load Task
The data is read from the temp CSV files and loaded into a postgres database

from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow.utils.dates import days_ago
from airflow.models.dag import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonVirtualenvOperator

default_args = {
    'owner': 'Ivan Hernandez Rocha',
    'email': 'softsolutions4@outlook.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 4, 28)
}

def extract():
    import json
    import time
    import pandas as pd
    from datetime import datetime
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from bs4 import BeautifulSoup

    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1080, 800) # set the size of the window

    categories = {
        119 : 'Singing & Dancing',
        104 : 'Comedy',
        112 : 'Sports',
        100 : 'Anime & Comics',
        107 : 'Relationship',
        101 : 'Shows',
        110 : 'Lipsync',
        105 : 'Daily Life',
        102 : 'Beauty Care',
        103 : 'Games',
        114 : 'Society',
        109 : 'Outfit',
        115 : 'Cars',
        111 : 'Food',
        113 : 'Animals',
        106 : 'Family',
        108 : 'Drama',
        117 : 'Fitness & Health',
        116 : 'Education',
        118 : 'Technology' 
    } 

    user_data = {
        'user_id': [],
        'user_name': [],
        'user_nickname': [],
        'verified': [],
        'signature': [],
        'avatar': [],
        'videos_count': [],
        'likes_count': [],
        'follower_count': [],
        'following_count': [],
        'hearts_count': [],
    }

    date_data = {
        'date_id': [],
        '_date': [],
        'day_of_week': [],
        'month': [],
        'year': [],
        'quarter': []
    }

    audio_data = {
        'audio_id': [],
        'title': [],
        'author': [],
        'duration': [],
        'album': [],
        'cover': [],
        'play_url': [],    
    }

    postFacts_data = {
        'post_id': [],
        'user_id': [],
        'date_id': [],
        'views_count': [],
        'likes_count': [],
        'shares_count': [],
        'saved_count': [],
        'comments_count': [],
        'duration': [],
        'follower_count_at_post_date': []
    }

    postDim_data = {
        'post_id': [],
        'user_id': [],
        'date_id': [],
        'category_id': [],
        'desc': [],
        'hashtag_list': [],
        'audio_id': [],
    }

    for k, v in categories.items():
        driver.get(f"https://www.tiktok.com/api/explore/item_list/?WebIdLastTime=1715545594&aid=1988&app_language=es&app_name=tiktok_web&browser_language=es-ES&browser_name=Mozilla&browser_online=true&browser_platform=Win32&browser_version=5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F124.0.0.0%20Safari%2F537.36&categoryType={k}&channel=tiktok_web&cookie_enabled=true&count=30&device_id=7368212115955598854&device_platform=web_pc&focus_state=true&from_page=&history_len=2&is_fullscreen=false&is_page_visible=true&language=es&odinId=7368211536445326342&os=windows&priority_region=&referer=&region=MX&screen_height=864&screen_width=1536&tz_name=America%2FMexico_City&webcast_language=es")
        time.sleep(4)
        page = driver.page_source
        soup = BeautifulSoup(page, 'lxml')
        web_data = soup.find('pre').contents[0]
        data_dict = json.loads(web_data)
        
        for item in data_dict['itemList']:
            hashtagsList = []
            if 'textExtra' in item:
                for i in item['textExtra']:
                    hashtagsList.append(i['hashtagName'])
            else:
                hashtagsList.append(None)
            
            if 'album' in item['music']:
                audio_data['album'].append(item['music']['album'])
            else:
                audio_data['album'].append(None)
                
            if 'playUrl' in item['music']:
                audio_data['play_url'].append(item['music']['playUrl'])      
            else:
                audio_data['play_url'].append(None)
            
            if 'authorName' in item['music']:
                audio_data['author'].append(item['music']['authorName'])      
            else:
                audio_data['author'].append(None)  

            if 'duration' in item['music']:
                audio_data['duration'].append(item['music']['duration'])    
            else:
                audio_data['duration'].append(None)  
            
            audio_data['audio_id'].append(item['music']['id'])
            audio_data['title'].append(item['music']['title'])
            audio_data['cover'].append(item['music']['coverLarge'])
            
            user_data['user_id'].append(item['author']['id'])
            user_data['user_name'].append(item['author']['uniqueId'])
            user_data['user_nickname'].append(item['author']['nickname'])
            user_data['verified'].append(item['author']['verified'])
            user_data['videos_count'].append(item['authorStats']['videoCount'])
            user_data['follower_count'].append(item['authorStats']['followerCount'])
            user_data['following_count'].append(item['authorStats']['followingCount'])
            user_data['avatar'].append(item['author']['avatarLarger'])
            user_data['likes_count'].append(item['authorStats']['diggCount'])
            user_data['hearts_count'].append(item['authorStats']['heartCount'])
            user_data['signature'].append(item['author']['signature'])
            
            postDim_data['post_id'].append(item['id'])
            postDim_data['user_id'].append(item['author']['id'])
            postDim_data['date_id'].append(item['id'])
            postDim_data['category_id'].append(k)
            postDim_data['desc'].append(item['desc'])
            postDim_data['hashtag_list'].append(hashtagsList)
            postDim_data['audio_id'].append(item['music']['id'])
            
            postFacts_data['post_id'].append(item['id'])
            postFacts_data['user_id'].append(item['author']['id'])
            postFacts_data['date_id'].append(item['id'])
            postFacts_data['views_count'].append(item['stats']['playCount'])
            postFacts_data['likes_count'].append(item['stats']['diggCount'])
            postFacts_data['shares_count'].append(item['stats']['shareCount'])
            postFacts_data['saved_count'].append(item['stats']['collectCount'])
            postFacts_data['comments_count'].append(item['stats']['commentCount'])
            postFacts_data['duration'].append(item['video']['duration'])
            postFacts_data['follower_count_at_post_date'].append(item['authorStats']['followerCount'])
            
            date = datetime.fromtimestamp(item['createTime'])
            if date.month <= 3:
                date_data['quarter'].append('Q1')
            elif date.month > 3 and date.month <= 6:
                date_data['quarter'].append('Q2')
            elif date.month > 6 and date.month <= 9:
                date_data['quarter'].append('Q3')
            elif date.month > 9 and date.month <= 12:
                date_data['quarter'].append('Q4')
            
            date_data['date_id'].append(item['id'])
            date_data['_date'].append(date.strftime('%a %d %b %Y, %I:%M%p'))
            date_data['day_of_week'].append(date.strftime('%a'))
            date_data['month'].append(date.month)
            date_data['year'].append(date.year)
    
    df_user = pd.DataFrame(user_data)
    df_audio = pd.DataFrame(audio_data)
    df_date = pd.DataFrame(date_data)
    df_postFacts = pd.DataFrame(postFacts_data)
    df_postDim = pd.DataFrame(postDim_data)

    df_user.to_csv('user_data.csv', index=False)
    df_audio.to_csv('audio_data.csv', index=False)
    df_date.to_csv('date_data.csv', index=False)
    df_postFacts.to_csv('postFacts_data.csv', index=False)
    df_postDim.to_csv('postDim_data.csv', index=False)

    print("Finished extract")

def load():
    import os
    import pandas as pd
    from sqlalchemy import create_engine

    def postgres_upsert(table, conn, keys, data_iter):
        from sqlalchemy.dialects.postgresql import insert

        data = [dict(zip(keys, row)) for row in data_iter]

        insert_statement = insert(table.table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(  
            constraint=f"{table.table.name}_pkey",
            set_={c.key: c for c in insert_statement.excluded},
        )
        conn.execute(upsert_statement)

    df_user = pd.read_csv('user_data.csv')
    df_audio = pd.read_csv('audio_data.csv')
    df_date = pd.read_csv('date_data.csv')
    df_postFacts = pd.read_csv('postFacts_data.csv')
    df_postDim = pd.read_csv('postDim_data.csv')

    # Database connection details
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    ENDPOINT = ''  # replace with your endpoint
    USER = ''  # replace with your username
    PASSWORD = ''  # replace with your password
    PORT = 5432  # replace with your port
    DATABASE = ''  # replace with your database name

    # Create the connection string
    connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

    # Create the database engine
    engine = create_engine(connection_string)

    # Write the DataFrame to a PostgreSQL table
    df_user = df_user.drop_duplicates(subset=['user_id'])
    df_user.to_sql('UserDim', engine, if_exists='append', index=False, method=postgres_upsert)

    df_date = df_date.drop_duplicates(subset=['date_id'])
    df_date.to_sql('DateDim', engine, if_exists='append', index=False, method=postgres_upsert)

    df_audio = df_audio.drop_duplicates(subset=['audio_id'])
    df_audio.to_sql('AudioDim', engine, if_exists='append', index=False, method=postgres_upsert)

    df_postFacts = df_postFacts.drop_duplicates(subset=['post_id'])
    df_postFacts.to_sql('PostFacts', engine, if_exists='append', index=False, method=postgres_upsert)

    df_postDim = df_postDim.drop_duplicates(subset=['post_id'])
    df_postDim.to_sql('PostDim', engine, if_exists='append', index=False, method=postgres_upsert)
    
    os.remove('user_data.csv')
    os.remove('audio_data.csv')
    os.remove('date_data.csv')
    os.remove('postFacts_data.csv')
    os.remove('postDim_data.csv')

    print("Finished load")

with DAG(
    "tiktok_dag",
    default_args=default_args,
    description='A tiktok data pipeline',
    schedule_interval=timedelta(days=2)

) as dag:
    
    extract_task= PythonVirtualenvOperator(
        task_id='tiktok_extract',
        python_callable=extract,
        provide_context=True,
        requirements=["pandas==2.2.2", "selenium==4.21.0", "beautifulsoup4==4.12.3", "lxml==5.2.2"]
    )

    load_task=PythonVirtualenvOperator(
        task_id='tiktok_load',
        python_callable=load,
        provide_context=True,
        requirements=["pandas==2.2.2", "SQLAlchemy==2.0.30", "psycopg2-binary==2.9.9"]
    )

    extract_task >> load_task

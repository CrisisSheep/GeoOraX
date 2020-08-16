from facebook_scraper import get_posts
import json
from pathlib import Path
import pytz
import time
from datetime import datetime
import logging
from botocore.exceptions import ClientError
import boto3
from elasticsearch import Elasticsearch
import os

# scraper config setup:
root_folder = Path(Path.cwd() / 'GeoOra')
mapping_folder = Path(Path.cwd() / 'GeoOra' / 'facebook_scraper' / 'mapping/')

with open(root_folder / 'facebook_scraper' / 'config.json', 'r') as f:
    config = json.load(f)

with open(root_folder / 'facebook_scraper' / 'credentials.json', 'r') as f:
    credentials = json.load(f)

facebook_config = config['facebook']
facebook_pages = facebook_config['pages']
facebook_groups = facebook_config['groups']

#facebook_groups = facebook_config['groups']
facebook_dict_for_json = {} 
facebook_page_config_array = []
current_facebook_page_config = []
facebook_group_config_array = []
current_facebook_group_config = []
facebook_page_posts = []
facebook_group_posts = []

#comprehend init
comprehend = boto3.client(service_name='comprehend', region_name='ap-southeast-2')

#Elasticsearch init
elastic = Elasticsearch(hosts=[credentials['elastic.url']], http_auth=(credentials['elastic.user'], credentials['elastic.password']), request_timeout=60)

tz = pytz.timezone("Pacific/Auckland")
date_now_local = datetime.now(tz)

post_id_dict = {}

def getComprehendAnalysis(text):
     if type(text) == str and len(text) > 5 and len(text) < 5000:
          result = comprehend.detect_sentiment(Text=text, LanguageCode='en')
          if result['ResponseMetadata']['HTTPStatusCode'] == 200:
               return {
                    "Sentiment": result['Sentiment'],
                    "SentimentScore": result['SentimentScore']
               }
          else:
               return None
     else:
          return None

def createElasticMapping(index):
     if not elastic.indices.exists(index):
          with open(mapping_folder / 'page_post.json', 'r') as f:
               mapping = json.load(f)

          # create an index with the mapping passed to the 'body' parameter
          response = elastic.indices.create(
               index=index,
               body=mapping,
               ignore=400
          )
          # print out the response:
          print ('response:', response)

          if 'acknowledged' in response:
               if response['acknowledged'] == True:
                    print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])

          # catch API error response
          elif 'error' in response:
               print ("ERROR:", response['error']['root_cause'])
               print ("TYPE:", response['error']['type'])

def savePagePost(post):
    result = elastic.index(index='facebook_post', body=post, id=post['post_id'])

#createElasticMapping('page_post')

for page_config in facebook_pages:
     existing_post_ids = set(page_config['post_ids']) #used to check if post data has already been scraped
     new_post_ids = set() #to be compared with existing_post_ids
     try:

          for post in get_posts(page_config['id'], pages=facebook_config['max_limit'], timeout=60):
               #We want to check that each post has not been previously recorded

               # ensuring that each post has a timestamp associated with it
               try:
                    print(post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() + ' (' + post['post_id'] + ')' or None)
               except:
                    continue

               #post id list for the current page
               if len(new_post_ids) == 0:
                    new_post_ids.add(post["post_id"])

               #details to be added to the array of posts
               fb_post = {
                    "post_id": post["post_id"],
                    "text": post['text'],
                    "post_text": post['post_text'],
                    "shared_text": post['shared_text'],
                    "timestamp": post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() or None,
                    "image": post['image'],
                    "video": post['video'],
                    "video_thumbnail":  post["video_thumbnail"],
                    "likes": post["likes"],
                    "comprehend": getComprehendAnalysis(post['text']),
                    "reactions": post.get('reactions') or None,
                    "group_id": page_config["id"],               
                    "group_name": page_config["name"],
                    "group_region": page_config["region"],
                    "group_city": page_config["city"],
                    "group_suburb": page_config["suburb"]
               }        
               facebook_page_posts.append(fb_post)
               savePagePost(fb_post)
               # Code for optimising periodic post id updates:
               if post["post_id"] in existing_post_ids:
                    break
          
          current_facebook_page_config ={
               "name": page_config["name"],
               "id": page_config["id"],
               "region": page_config["region"],
               "city": page_config["city"],
               "suburb": page_config["suburb"],
               "posts": facebook_page_posts
          }

          #when all post ids have been added to new_post_ids set, we add the values to a dictionary
          post_id_dict[page_config['id']] = list(new_post_ids)
          
          facebook_page_config_array.append(current_facebook_page_config) 

          filename = str(page_config['id']) + '.json'
          filename = filename.lower()
          print(filename) #json filename for page data

          #creating json file for current facebook page
          with open(root_folder / 'facebook_scraper' / 'data' / filename, 'w') as outfile:
               tmp_dictionary = {
                    "pages": current_facebook_page_config
               }
               json.dump(tmp_dictionary, outfile)
               
     except Exception as e:
          print(e)

#updating config
for page_config in facebook_config['pages']:
     if page_config['id'] in post_id_dict:
          page_config['post_ids'] = post_id_dict[page_config['id']]

#writing new config details to config json file     
with open(root_folder / 'facebook_scraper' / 'config.json', 'w') as outfile:
     json.dump(config, outfile)

for group_config in facebook_groups:
     existing_post_ids = set(group_config['post_ids']) #used to check if post data has already been scraped
     new_post_ids = set() #to be compared with existing_post_ids
     try:

          for post in get_posts(group=group_config['id'], pages=facebook_config['max_limit'], timeout=60):
               #We want to check that each post has not been previously recorded
               if post['post_id'] is None:
                    continue
               # ensuring that each post has a timestamp associated with it
               try:
                    strTime = post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat()
                    print(post.get('time').astimezone(pytz.timezone('Pacific/Auckland')).replace(microsecond=0).isoformat() + ' (' + post['post_id'] + ')' or None)
               except:
                    strTime = date_now_local.replace(microsecond=0).isoformat() 
                    print(date_now_local.replace(microsecond=0).isoformat() + ' (' + post['post_id'] + ')')

               #post id list for the current page
               if len(new_post_ids) == 0:
                    new_post_ids.add(post["post_id"])

               #details to be added to the array of posts
               fb_post = {
                    "post_id": post["post_id"],
                    "text": post['text'],
                    "post_text": post['post_text'],
                    "shared_text": post['shared_text'],
                    "timestamp": strTime,
                    "image": post['image'],
                    "video": post['video'],
                    "video_thumbnail":  post["video_thumbnail"],
                    "likes": post["likes"],
                    "comprehend": getComprehendAnalysis(post['text']),
                    "reactions": post.get('reactions') or None,
                    "group_id": group_config["id"],               
                    "group_name": group_config["name"],
                    "group_region": group_config["region"],
                    "group_city": group_config["city"],
                    "group_suburb": group_config["suburb"]
               }        
               facebook_group_posts.append(fb_post)
               savePagePost(fb_post)
               # Code for optimising periodic post id updates:
               if post["post_id"] in existing_post_ids:
                    break
          
          current_facebook_group_config ={
               "name": group_config["name"],
               "id": group_config["id"],
               "region": group_config["region"],
               "city": group_config["city"],
               "suburb": group_config["suburb"],
               "posts": facebook_group_posts
          }

          #when all post ids have been added to new_post_ids set, we add the values to a dictionary
          post_id_dict[group_config['id']] = list(new_post_ids)
          
          facebook_group_config_array.append(current_facebook_group_config) 

          filename = str(group_config['id']) + '.json'
          filename = filename.lower()
          print(filename) #json filename for page data

          #creating json file for current facebook page
          with open(root_folder / 'facebook_scraper' / 'data' / filename, 'w') as outfile:
               tmp_dictionary = {
                    "groups": current_facebook_group_config
               }
               json.dump(tmp_dictionary, outfile)
               
     except Exception as e:
          print(e)

#setting up dictionary with all page information, to be used for main json file
facebook_dict_for_json = {
     "facebook": {
          "pages": facebook_page_config_array,
          "groups": facebook_group_config_array
     }
}

#when all the pages have been collected, we add every post id back into the config file..

#updating config
for page_config in facebook_config['groups']:
     if page_config['id'] in post_id_dict:
          page_config['post_ids'] = post_id_dict[page_config['id']]

#writing new config details to config json file     
with open(root_folder / 'facebook_scraper' / 'config.json', 'w') as outfile:
     json.dump(config, outfile)

# #writing main page/post data json file
# with open(root_folder / 'facebook_scraper' / 'facebook_posts.json', 'w') as outfile:
#      json.dump(facebook_dict_for_json, outfile)

print('Scraping finished.')

#CODE FOR UPLOADING FILE TO S3 BUCKET
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(str(root_folder / 'facebook_scraper' / 'data' / file_name), bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def generateIndex():
     file_array = []
     s3_client = boto3.client('s3')
     for key in s3_client.list_objects(Bucket='geoora')['Contents']:
          file_array.append(key['Key'])
     
     file_index = {
          "file_index": file_array
     }

     #writing main page/post data json file
     with open(root_folder / 'facebook_scraper' / 'data' / 'index.json', 'w') as outfile:
          json.dump(file_index, outfile)
     
     upload_file('index.json', 'geoora')


for filename in os.listdir(root_folder / 'facebook_scraper' / 'data'):
     upload_file(filename, "geoora")

generateIndex()
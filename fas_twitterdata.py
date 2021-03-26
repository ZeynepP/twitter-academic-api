# -*- coding: utf-8 -*-
"""FAS_TwitterData.ipynb

## FULL ARCHIVE SEARCH TWITTER API
"""

import csv
import requests
import json
import time

bearer_token = "XXXX"
HEADERS = {"Authorization": "Bearer {}".format(bearer_token)}
URL = "https://api.twitter.com/2/tweets/search/all"


def get_params(params, next_token=None):
    tweet_params = {'max_results': 500}


    if 'tweet_fields' in params:
        tweet_params['tweet.fields'] = params['tweet_fields']
    if 'user_fields' in params:
        tweet_params['user.fields'] = params['user_fields']
    if 'place_fields' in params:
        tweet_params['place.fields'] = params['place_fields']
    if 'expansions' in params:
        tweet_params['expansions'] = params['expansions']
    if 'start_time' in params:
        tweet_params['start_time'] = params['start_time']
    if 'end_time' in params:
        tweet_params['end_time'] = params['end_time']
    if 'query' in params:
        tweet_params['query'] = params['query']

    if next_token is not None:
        tweet_params['next_token'] = next_token

    return tweet_params



def connect_to_endpoint(url, params):

    response = requests.get(url, params=params, headers=HEADERS)
    print(response.url)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print(response.headers.get('x-rate-limit-reset'))
        remainder = float(response.headers.get('x-rate-limit-reset')) - time.time()
        print("Sleeping for seconds", remainder)
        if (remainder > 0):
            time.sleep(remainder)
            connect_to_endpoint(url, params)
    else :
        raise Exception(response.status_code, response.text)



def convert_csv(tweet, users, media):
    csv_data ={}

    csv_data["id"] = tweet["id"]
    csv_data["created_at"] = tweet["created_at"]
    csv_data["text"] = tweet["text"].replace(";", " ").replace("\n", "").replace('"', "").replace('\'', "")

    csv_data["author_id"]=tweet["author_id"]
    user = users[tweet["author_id"]]
    csv_data["author_name"] = user["name"]
    csv_data["author_username"] = user["username"]
    csv_data["source"] = tweet["source"]
    csv_data["possibly_sensitive"] = tweet["possibly_sensitive"]
    csv_data["lang"] = tweet["lang"]

    try:
        murls = []
        mtypes = []
        media_keys = tweet['attachments']["media_keys"]
        for m in media_keys :
          murls.append(media[m]["url"])
          mtypes.append(media[m]["type"])
    except:
        murls = []
        mtypes = []

    csv_data['media_urls'] = " , ".join(murls)
    csv_data['media_types'] = " , ".join(mtypes)

    csv_data["retweet_count"] = tweet["public_metrics"]["retweet_count"]
    csv_data["reply_count"]=tweet["public_metrics"]["reply_count"]
    csv_data["like_count"] = tweet["public_metrics"]["like_count"]
    csv_data["quote_count"] = tweet["public_metrics"]["quote_count"]

    try :
        urls = tweet["entities"]["urls"]
        surls = []
        if len(urls )> 0:
            for url in urls :
                surls.append(url["expanded_url"])
    except:
        surls = []
    csv_data['urls'] = " , ".join(surls)

    try :
        mentions = tweet["entities"]["mentions"]
        smentions = []
        if len(mentions )> 0:
            for url in mentions :
                smentions.append(url["username"])
    except:
        smentions  = []
    csv_data["mentions"] = " , ".join(smentions)
    try:
        hashtags = tweet["entities"]["hashtags"]
        shashtags = []
        if len(hashtags) > 0:
            for url in hashtags:
                shashtags.append( url["tag"])
    except:
        shashtags  = []

    csv_data["hashtags"] = " , ".join(shashtags)

    try:
        ref = tweet["referenced_tweets"]
        rt = []
        rt_id =[]
        if len(ref) > 0:
          for r in ref:
            rt.append( r["type"])
            rt_id.append(ref[0]["id"])
    except:
        rt = ["Tweet"]
        rt_id=["0"]

    csv_data["retweet"] =  " , ".join(rt)
    csv_data["retweet_id"] =  " , ".join(rt_id)

    return csv_data

def parse_and_write(json_response, header, f):
    users ={}
    media = {}
    try:
      for m in json_response["includes"]["media"]:
        media[m["media_key"]] = m
    except:
      print("no media found in bunch tweet")

    for u in json_response["includes"]["users"]:
      users[u["id"]] = u

    for tweet in json_response['data']:
        #print(tweet)
        csv_data = convert_csv(tweet, users, media)
        writer = csv.DictWriter(f, fieldnames=csv_data, delimiter=';' ,  escapechar='\r')
        if header:
            writer.writeheader()
            header = False
        writer.writerow(csv_data)
    print("last id of batch", tweet["id"])
    return header

def get_data(params , output, next_token=None, keepJson=False):

   
    count = 0
    flag = True
    header = True

    tweet_params = get_params(params, next_token)
    f = open(output, "w", encoding='utf-8',  newline="")
    if keepJson:
        fjson = open(output + ".json", "w", encoding='utf-8', newline="")
    while flag:
        # Replace the count below with the number of Tweets you want to stop at.
        # Note: running without the count check will result in getting more Tweets
        # that will count towards the Tweet cap

        try:
            json_response = connect_to_endpoint(URL, tweet_params)
            result_count = json_response['meta']['result_count']
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
            else :
                next_token = None #in case from params

            print(next_token)
            if next_token is not None :
                tweet_params["next_token"] = next_token
            else:
                if "next_token" in tweet_params:
                    del tweet_params["next_token"]
            if result_count is not None and result_count > 0 and next_token is not None:
                if keepJson:
                    json.dump(json_response, fjson)
                header = parse_and_write(json_response,header,f)
                count += result_count
                print(count)


            else:
                flag = False

                if result_count is not None and result_count > 0 :
                    if keepJson:
                        json.dump(json_response, fjson)
                    header = parse_and_write(json_response,header,f)
                    count += result_count
                    print(count)
        except Exception as ex:
            print(ex, next_token)
    print("Total Tweet IDs saved: {}".format(count))
    f.close()

params = {
    "start_time": "2011-01-01T00:00:00Z",
 #   "end_time": "2021-03-19T00:00:00Z",
    "query": "From:TwitterDev  is:retweet",
    "tweet_fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,withheld",
    #  "user.fields" : "created_at,id,location,name,profile_image_url,protected,public_metrics,url,username,verified,withheld"
    "expansions" : "author_id,attachments.media_keys",
    "media_fields" : "url"
  }

output ="./twitterdev.csv" #
next_token =None # to restart from the token if a problem occurs after getting some data.
get_data(params, output, next_token )
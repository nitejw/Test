from __future__ import print_function
import tweepy, json, pymongo, time, datetime, multiprocessing, threading
from tweepy import OAuthHandler
from threading import Timer

# connection
mongoClient = pymongo.MongoClient("mongodb://localhost:27017/")
# use database
db = mongoClient["k"]
# collections (tables) being used
collectionMerged = db['Merged']
collectionRest = db['restTweet']
collectionStream = db['streamTweet']

###################################################################################################################################

# api key 1 / auth
consumer_key = 'NOB8m1F3gXdVMD4jNbSaVY6Xy'
consumer_secret = 'yMoDLjGf2iYDQ7ahB0MnlFsNCFs833PkKxeL26BAf2i2zOWCbd'
access_token = '1179015889133297664-7lJQWUNVPIldoS8qRQIrdvVKyvJy9Y'
access_secret = 'CHgHZXavZoGa7GhsGRpib1zQkQCgc3AxlATGrxiiLtQGw'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# api key 2 / auth
consumer_key2 = 'Aa2xCSrHpamFM0rYpYqqsKYyi'
consumer_secret2 = 'vmD3DrhYe7TzLDk5lNm3Q4rIqg5V5UZb4olPDOSy3Fpk0LXGDu'
access_token2 = '1179015889133297664-edUMvqmpvNtnnq8lQXHk6l1Cq3trwq'
access_secret2 = 'HTNGtshltgrVvEmsguMakRxqGh7edMuzUKsfpzuThgdEX'

auth2 = OAuthHandler(consumer_key2, consumer_secret2)
auth2.set_access_token(access_token2, access_secret2)
 
api2 = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# api key 3 / auth
consumer_key3 = 'KUOAzEwL6DorTYPn3zuevck7q'
consumer_secret3 = 'lEl4cTnOmH2HiL8VluD3vtvB3Hh3pc9EG2YCwIRD9Cqo8uNIVG'
access_token3 = '1179015889133297664-131UQebj6b7idGATePb9APzSVw0aZP'
access_secret3 = 'FHvNi594rveszUy40eUTI47VshUUA6h6UINO5Tw4LIgFs'

auth3 = OAuthHandler(consumer_key3, consumer_secret3)
auth3.set_access_token(access_token3, access_secret3)
 
api3 = tweepy.API(auth3, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# api trend key / auth
consumer_key4 = 'ow5ZXTwwGUOny4BOxDtjoG37i'
consumer_secret4 = '0Kp0o1uCUz9KNv1vs80vexOsbUOmZ66IsxtuIwJ6O12T74iQ6J'
access_token4 = '1179015889133297664-tbybiZdA4joQEKJADwabL3NUNbzqz7'
access_secret4 = 'vWj946tW0DvoVN1otVWHElGOLDlfAvNmeqro5ww1ts43H'

auth4 = OAuthHandler(consumer_key4, consumer_secret4)
auth4.set_access_token(access_token4, access_secret4)
 
api4 = tweepy.API(auth4, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#the time to run (1 hr)
runtime = 60*60

#TREND FIRST##################################################################################################################################

#list to store the trending word
trendlist = []

# store the name and volume from the trending API
trendVolume = {}
count = 0

# woeid = london;uk
for trendWords in api4.trends_place(44418):
        for i, value in trendWords.items():
            if i == "trends":
                for i in value:
                    if(i['tweet_volume'] != None):
                        trendVolume[i["name"]] = i["tweet_volume"]

# sort and store top 5 trends
sortVolume = sorted(trendVolume, key = lambda i: trendVolume[i], reverse=True)
for sortTrend in sortVolume:
    if count < 4:
        trendlist.append(sortTrend)
        count += 1

#Streaming API###################################################################################################################################

# bounding location 4 params (around london)
bbox = [-0.933838,51.190705,0.689392,51.794348]

# Streaming API code
class SAPI(tweepy.StreamListener):
    def on_connect(self):
        print("Connection to Twitter established - STREAM")

    def on_data(self, data):
            try:
                all_data = json.loads(data)
                # Check that the data have country code GB (UK) then store into database
                if all_data['place']['country_code'] == "GB":
                    collectionStream.insert(all_data)
                    collectionMerged.insert(all_data)
            except Exception as e:
                print(e)

    def on_error(self, status_code):
        print("Error: " + repr(status_code))
        return False

def streamer():    
    myStreamListener = SAPI(api = api)
    mySAPI = tweepy.Stream(auth=auth, listener=myStreamListener)
    mySAPI.filter(locations=bbox, languages=['en'])

def streamer2():    
    myStreamListener = SAPI(api = api2)
    mySAPI = tweepy.Stream(auth=auth2, listener=myStreamListener)
    mySAPI.filter(locations=bbox, languages=['en'])
    


#Rest API########################################################################################################################################

# geocode (lot lan radius)
geocode = "51.51768,-0.11362,50mi"

# Rest API code
def RestAPI():
    print("Connection to Twitter established - REST")
    rTweet = tweepy.Cursor(api3.search, geocode=geocode, q=' OR '.join(trendlist), lang='en').items()

    while (True):
        try:
            for tweet in rTweet:
                dataJson = tweet._json
                collectionRest.insert(dataJson)
                collectionMerged.insert(dataJson)
        # Addresing rate limit for rest, once rate limit reach, it will sleep until the API is able to collect again
        except tweepy.TweepError:
            continue
        except StopIteration:
            break

#################################################################################################################################################

# Display
def terminateProcess(stream, rest, stream2):
    stream.terminate()
    rest.terminate()
    stream2.terminate()

    # get the current UTC time (endUntil) and one hour before the current UTC time (startSince)
    startSince = (datetime.datetime.utcnow() + datetime.timedelta(hours=-1)).strftime("%a %b %d %H:%M:%S %z%Y")
    endUntil = datetime.datetime.utcnow().strftime("%a %b %d %H:%M:%S %z%Y")
    # startSince = "Mon Nov 05 11:30:00 +0000 2018"
    # endUntil = "Mon Nov 05 12:30:00 +0000 2018"

    # total tweet
    tweetSum = collectionMerged.count()
    restSum = collectionRest.find({}).count()
    print("Total Tweets: " + str(tweetSum))

    # count not duplicated tweet
    distinctCount = len(collectionMerged.distinct('id_str', {'created_at': {'$lt': endUntil, '$gte': startSince}}))
    # geolocation tweet
    geo_tagged = (collectionRest.find({"geo": {'$ne': None}, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count()) + (collectionStream.find({"geo": {'$ne': None}, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count())
    # quoted tweet
    quote = (collectionRest.find({"is_quote_status": True, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count()) + (collectionStream.find({"is_quote_status": True, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count())
    # retweeted tweet
    retweet = (collectionRest.find({'retweeted_status.id': {'$ne': None}, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count()) + (collectionStream.find({'retweeted_status.id': {'$ne': None}, 'created_at': {'$lt': endUntil, '$gte': startSince}}).count())
    # multimedia tweet
    multimedia = len(collectionRest.distinct('extended_entities.media.media_url', {'created_at': {'$lt': endUntil, '$gte': startSince}})) + len(collectionStream.distinct('extended_entities.media.media_url', {'created_at': {'$lt': endUntil, '$gte': startSince}}))


    # output + converting int to str
    print("Trend: " + str(trendlist))
    print("Total Rest Tweets collected: " + str(restSum))
    print("Total Stream Tweets collected: " + str(tweetSum-restSum))
    print("Total Geo-tagged data from London: " + str(geo_tagged))
    print("Unique Tweets: " + str(distinctCount))
    print("Duplicate Tweets: " + str(tweetSum - distinctCount))
    print("Total Quotes: " + str(quote))
    print("Total Retweets: " + str(retweet))
    print("Total Multimedia: " + str(multimedia))


# main
if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')
    # each thread & run
    stream = multiprocessing.Process(target=streamer)
    stream.start()
    rest = multiprocessing.Process(target=RestAPI)
    rest.start()
    stream2 = multiprocessing.Process(target=streamer2)
    stream2.start()

    # terminateProcess ends the thread and display output
    endTime = Timer(runtime, terminateProcess, args=[stream, rest, stream2])
    endTime.start()

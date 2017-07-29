import sys, json, datetime, codecs, locale
import tweepy
import MySQLdb as mdb


def forceStr(inp):
  if not (type(inp) == str or type(inp) == unicode):
    inp = unicode(inp) # unicode(inp, "utf-8", errors='replace')
  return inp.encode('utf-8')

# Twitter handshake stuff
consumer_key = "<twitter consumer key>"
consumer_secret = "<twitter consumer secret>"
access_key = "<twitter access key>"
access_secret = "<twitter access secret>"

# Connect to the API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# Connect to MySQL and create table if necessary
con = mdb.connect('localhost', 'crawler', '<local db password>', 'tweets')
cur = con.cursor()

# Create the tweets table if it doesn't exist.
cur.execute("""
  CREATE TABLE IF NOT EXISTS tweets(tweet text CHARACTER SET utf8 COLLATE utf8_general_ci, entities text CHARACTER SET utf8 COLLATE utf8_general_ci, 
    internal_id bigint NOT NULL AUTO_INCREMENT, tweet_id bigint NOT NULL, is_retweet tinyint(1), num_retweets int, 
    retweet_tweet_id bigint, retweeted_user varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci, 
    retweeted_user_id bigint, geotagged tinyint(1), lat float(10), 
    lng float(10), username varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci, userid int, 
    num_followers int, num_friends int, pubtime timestamp,
    PRIMARY KEY (internal_id)) ENGINE=MyISAM CHARACTER SET utf8 COLLATE utf8_general_ci;""")
try:
  cur.execute("ALTER TABLE tweets ADD INDEX geoloc (geotagged,lat,lng);")
  cur.execute("ALTER TABLE tweets ADD INDEX isretweet (is_retweet);")
  cur.execute("ALTER TABLE tweets ADD INDEX published (pubtime);")
except:
  print "INDEXES already created"

# these are the parameters to collect from Twitter
keywords = ['keyword1', 'keyword2']
usernames = []
boxes = []

class CustomStreamListener(tweepy.StreamListener):
  def on_status(self, status):
    insertObj = {
      'tweet': u'"' + status.text.replace("\"", "\\\"") + u'"',
      'entities': u'"' + json.dumps(status.entities).replace("\"", "\\\"") + u'"',
      'tweet_id': status.id,
      'is_retweet': int(status.retweet != None),
      'num_retweets': status.retweet_count,
      'retweet_tweet_id': -1,
      'retweeted_user': u'',
      'retweeted_user_id': -1,
      'geotagged': int(status.coordinates != None),
      'lat': 0.,
      'lng': 0.,
      'username': u'"' + status.author.screen_name + u'"',
      'userid': status.author.id,
      'num_followers': status.author.followers_count,
      'num_friends': status.author.friends_count,
      'jobID': int(JOB_ID),
      'pubtime': "DATE_FORMAT('" + datetime.datetime.strftime(status.created_at, '%Y-%m-%d %H:%M:%S') + "', '%Y-%m-%d %H:%i:%s')"
    }
    if insertObj['is_retweet']:
      insertObj['retweet_tweet_id'] = status.retweet.im_self.id
      insertObj['retweeted_user'] = u'"' + status.retweet.im_self.author.screen_name + u'"' 
      insertObj['retweeted_user_id'] = status.retweet.im_self.author.id
    if insertObj['geotagged'] == 1:
      print " === GEO:", insertObj['tweet_id'], type(status.coordinates), status.coordinates
      insertObj['lat'] = status.coordinates['coordinates'][1]
      insertObj['lng'] = status.coordinates['coordinates'][0]

    print "PUBTIME: ", insertObj['pubtime']

    fields = ",".join([x for x, y in insertObj.items()])
    values = ",".join([forceStr(y) for x, y in insertObj.items()])
    sql = 'INSERT INTO tweets(%s) VALUES(%s);' % (fields, values)
    try:
      cur.execute(sql)
    except:
      print "FAILED SQL. FAILED JSON:", sql

  def on_error(self, status_code):
    print >> sys.stderr, 'Encountered error with status code:', status_code
    return True # Don't kill the stream

  def on_timeout(self):
    print >> sys.stderr, 'Timeout...'
    return True # Don't kill the stream

# begin crawling the data
sapi = tweepy.streaming.Stream(auth, CustomStreamListener())
print keywords
print usernames
print boxes
sapi.filter(track=keywords, follow=usernames, locations=boxes)

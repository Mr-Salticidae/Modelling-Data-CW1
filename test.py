from ctypes import sizeof
import json
from pathlib import Path
import sqlite3

p = Path('./ad-impressions.js')
content = json.loads(p.read_text(encoding='utf-8')[33:])
impressions = content[0]['ad']['adsUserData']['adImpressions']['impressions']
print(impressions[0].keys())
print(impressions[0]['deviceInfo'])
print(impressions[0]['displayLocation'])
print(impressions[0]['promotedTweetInfo'])
print(impressions[0]['advertiserInfo'])
print(impressions[0]['matchedTargetingCriteria'])
print(impressions[0]['impressionTime'])


conn = sqlite3.connect('./test.db')
cur = conn.cursor()

impressionId = 0
targetingId = 0

impressionId += 1

# Insert deviceInfo
deviceInfo = impressions[0]['deviceInfo']
osType = deviceInfo['osType']
deviceId = deviceInfo['deviceId']
deviceType = deviceInfo['deviceType']
deviceId += deviceType
deviceData = (osType, deviceId, deviceType)
cur.execute('INSERT INTO deviceInfo VALUES(?, ?, ?)', deviceData)

# Insert promotedTweetInfo
promotedTweetInfo = impressions[0]['promotedTweetInfo']
tweetId = promotedTweetInfo['tweetId']
tweetText = promotedTweetInfo['tweetText']
urls = str(promotedTweetInfo['urls'])
mediaUrls = str(promotedTweetInfo['mediaUrls'])
TweetData = (tweetId, tweetText, urls, mediaUrls)
cur.execute('INSERT INTO promotedTweetInfo VALUES(?, ?, ?, ?)', TweetData)

# Insert advertiserInfo
advertiserInfo = impressions[0]['advertiserInfo']
advertiserName = advertiserInfo['advertiserName']
screenName = advertiserInfo['screenName']
advertiserData = (advertiserName, screenName)
cur.execute('INSERT INTO advertiserInfo VALUES(?, ?)', advertiserData)

# Insert TargetingCriteria and matchedTargetingCriteria
TargetingCriteria = impressions[0]['matchedTargetingCriteria']
for target in TargetingCriteria:
    targetingId += 1
    targetingType = target['targetingType']
    targetingValue = target['targetingValue']
    targetingData = (targetingId, targetingType, targetingValue)
    cur.execute('INSERT INTO TargetingCriteria VALUES(?, ?, ?)', targetingData)
    matchedTargetingData = (impressionId, targetingId)
    cur.execute('INSERT INTO matchedTargetingCriteria VALUES(?, ?)', matchedTargetingData)

# Insert impressions
displayLocation = impressions[0]['displayLocation']
impressionTime = impressions[0]['impressionTime']
impressionData = (impressionId, deviceId, displayLocation, tweetId, impressionTime, advertiserName)
cur.execute('INSERT INTO impressions VALUES(?, ?, ?, ?, ?, ?)', impressionData)

conn.commit()
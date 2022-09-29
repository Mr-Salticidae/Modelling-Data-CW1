import json
import sqlite3
import argparse
from pathlib import Path
# Needs to be at least Python version 3.6

# DO NOT IMPORT ANY OTHER MODULES!

# Please make sure you don't add any "print" statements in your final version.
# Only existing prints should write to the terminal!
# If you "print" to help with development or debugging, make sure to remove them
# before you submit!


# YOU DO NOT NEED TO MODIFY THIS FUNCTION
# If you break this function, you likely break the whole script, so
# it's best not to touch it.
def load_json_from_js(p):
    """Takes a path to Twitter ad impression data and returns parsed JSON.
    
    Note that the Twitter files are *not* valid JSON but a Javascript file
    with a blog of JSON assigned to a Javascript variable, so some 
    preprocessing is needed.""" 
  
    # Note that this is a horrid hack. It's *fragile* i.e., if Twitter changes it's
    # variable name (currently "window.YTD.ad_engagements.part0 =") this will break.
    # It also requires loading the entire string into memory before parsing. If we're
    # running this on user machines on their own data this is probably fine, but if 
    # we're running it on a server the fact that we have the entire string AND the entire
    # parsed JSON structure in memory will add up.
    
    # If we use the standard json module, then there's no advantage to *not* doing this
    # if we want to json.load the file...it brings the string into memory anyway.
    #     https://pythonspeed.com/articles/json-memory-streaming/
    # We'd need to handle buffering ourselves or explore existing streaming solutions 
    # like:
    #     https://pypi.org/project/json-stream/
    # But then we'll have to play some tricks to avoid the junk at the beginning.
    #
    # Also, the weird, pointless, top level objects might break streaming. So we might
    # need to do a LOT of preprocessing.
    
    # ... further investigation of json-stream suggests it can handle the junk ok!
    #     https://github.com/daggaz/json-stream 
    return json.loads(p.read_text(encoding='utf-8')[33:])


# Don't touch this function!
def populate_db(adsjson, db):
    """Takes a blob of Twitter ad impression data and pushes it into our database.
    
    Note that this is responsible for avoiding redundant entries. Furthermore,
    it should be robust to errors and always get as much data in as it can.
    """ 
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    try:
        json2db(adsjson, cur)
    except:
        # We'd prefer no exceptions reached this level.
        print("There was a problem with the loader. This shouldn't happen")
    conn.commit()
    conn.close()

def populate_device_info(impression, cur):
    try:
        device_info = impression['deviceInfo']
        device_id = device_info['deviceId']
    except:
        return None
    
    device_type = device_info['deviceType']
    device_id += device_type
    os_type = device_info['osType']
    device_data = (os_type, device_id, device_type)
    
    try:
        cur.execute('INSERT INTO deviceInfo VALUES(?, ?, ?)', device_data)
    except:
        pass
    
    return device_id

def populate_promoted_tweet_info(impression, cur):
    try:
        promoted_tweet_info = impression['promotedTweetInfo']
        tweet_id = promoted_tweet_info['tweetId']
    except:
        return None
    
    tweet_text = promoted_tweet_info['tweetText']
    urls = str(promoted_tweet_info['urls'])
    media_urls = str(promoted_tweet_info['mediaUrls'])
    tweet_data = (tweet_id, tweet_text, urls, media_urls)
    
    try:
        cur.execute('INSERT INTO promotedTweetInfo VALUES(?, ?, ?, ?)', tweet_data)
    except:
        pass
    
    return tweet_id

def populate_advertiser_info(impression, cur):
    try:
        advertiser_info = impression['advertiserInfo']
        advertiser_name = advertiser_info['advertiserName']
    except:
        return None
    
    screen_name = advertiser_info['screenName']
    
    try:
        advertiser_data = (advertiser_name, str([screen_name]))
        cur.execute('INSERT INTO advertiserInfo VALUES(?, ?)', advertiser_data)
    except:
        cur.execute('SELECT screenName FROM advertiserInfo WHERE advertiserName = ?', (advertiser_name,))
        existing_screen_names = cur.fetchone()[0]
        existing_screen_names = eval(existing_screen_names)
        if screen_name not in existing_screen_names:
            existing_screen_names.append(screen_name)
            advertiser_data = (str(existing_screen_names), advertiser_name)
            cur.execute('UPDATE advertiserInfo SET screenName = ? WHERE advertiserName = ?', advertiser_data)
    
    return advertiser_name

def populate_impressions(impression, impression_id, device_id, tweet_id, advertiser_name, cur):
    display_location = impression['displayLocation']
    impression_time = impression['impressionTime']
    impression_data = (impression_id, device_id, display_location, tweet_id, impression_time, advertiser_name)
    cur.execute('INSERT INTO impressions VALUES(?, ?, ?, ?, ?, ?)', impression_data)

def populate_targeting_criteria_and_matched_targeting_criteria(impression, targeting_id, impression_id, cur):
    TargetingCriteria = impression['matchedTargetingCriteria']
    for target in TargetingCriteria:
        targeting_type = target['targetingType']
        try:
            targeting_value = target['targetingValue']
        except:
            targeting_value = ''
            
        # Check whether targeting type and targeting value already exist
        cur.execute('SELECT * FROM TargetingCriteria WHERE targetingType = ? AND targetingValue = ?', (targeting_type, targeting_value))
        res = cur.fetchone()
        if res is not None:
            current_targeting_id = res[0]
        else:
            targeting_id += 1
            current_targeting_id = targeting_id
            targeting_data = (targeting_id, targeting_type, targeting_value)
            cur.execute('INSERT INTO TargetingCriteria VALUES(?, ?, ?)', targeting_data)
        
        matched_targeting_data = (impression_id, current_targeting_id)
        cur.execute('INSERT INTO matchedTargetingCriteria VALUES(?, ?)', matched_targeting_data)
        
    return targeting_id

def json2db(adsjson, cur):
    """Processes the JSON and INSERTs it into the db via the cursor, cur"""
    
    # THIS IS WHAT YOU SHOULD MODIFY!
    # Feel free to add helper functions...you don't *need* to make a giant
    # hard to test function...indeed, that will come up in code review!
    impression_id = 0
    targeting_id = 0
    for impressions in adsjson:
        impressions = impressions['ad']['adsUserData']['adImpressions']['impressions']
        for impression in impressions:
            impression_id += 1
            
            # If device id or tweet id or advertiser name is missing, throw it away
            device_id = populate_device_info(impression, cur)
            if device_id is None:
                continue
            tweet_id = populate_promoted_tweet_info(impression, cur)
            if tweet_id is None:
                continue
            advertiser_name = populate_advertiser_info(impression, cur)
            if advertiser_name is None:
                continue
            populate_impressions(impression, impression_id, device_id, tweet_id, advertiser_name, cur)
            targeting_id = populate_targeting_criteria_and_matched_targeting_criteria(impression, targeting_id, impression_id, cur)
            


# DO NOT MODIFY ANYTHING BELOW!
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load JSON from Twitter's ad-impressions.js into our database.")
    parser.add_argument('--source',  
                        type=Path,
                        default=Path('./ad-impressions.js'),
                        help='path to source  file')    
    parser.add_argument('--output', 
                        type=Path,
                        default=Path('./twitterads.db'),
                        help='path to output DB')    
    args = parser.parse_args()
    
    print('Loading JSON.')
    ads_json = load_json_from_js(args.source)
    print('Populating database.')    
    populate_db(ads_json, args.output)
    print('Done')
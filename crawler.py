import tweepy
import secrets
import pandas as pd

# name = 'train'


def crawl(name, CONSUMER_KEY=secrets.CONSUMER_KEY, CONSUMER_SECRET=secrets.CONSUMER_SECRET,
          ACCESS_KEY=secrets.ACCESS_KEY, ACCESS_SECRET=secrets.ACCESS_SECRET):
    df = pd.read_csv(name + ".tsv", sep="\t")
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True)

    tweets_by_API = []
    wrong_ones = []
    # input_data = input_data[:20]
    idx = 0
    print(df.columns)
    verfied_list = []
    retweet_count_list = []
    followers_count_list = []
    url_count_list = []
    photo_count_list = []
    video_count_list = []
    Id_list = []
    label_list = []
    text_list = []
    truncated_list = []
    listed_count_list = []
    statuses_count_list = []
    user_created_at_list = []
    hashtag_count_list = []
    friends_count_list = []
    user_url_list = []
    for i in range(df.shape[0]):
        if idx % 20 == 0:
            print('[I] number of ids processed:', idx)
        try:
            json = api.get_status(df['Id'][i], tweet_mode='extended')
            verfied = json['user']['verified']
            followers_count = json['user']['followers_count']
            retweet_count = json['retweet_count']
            truncated = json["truncated"]
            listed_count = json["user"]["listed_count"]
            statuses_count = json["user"]["statuses_count"]
            user_created_at = json["user"]["created_at"][-4:]
            friends_count = json['user']['friends_count']
            user_url = 0
            if 'url' in json['user']['entities']:
                user_url = 1
            url_count = 0
            if 'urls' in json['entities']:
                url_count = len(json['entities']['urls'])
            hashtag_count = 0
            if 'urls' in json['entities']:
                hashtag_count = len(json['entities']['hashtags'])
            photo_count = 0
            video_count = 0

            if 'media' in json['entities']:
                # print(len(json['entities']['media']))
                for i in range(len(json['entities']['media'])):
                    if json['entities']['media'][i]['type'] == 'photo':
                        photo_count += 1
                    # elif json['entities']['media'][i]['type'] == 'video':
                    #     video_count += 1
                    else:
                        print(json['entities']['media'][i]['type'])
                        exit()
            verfied_list.append(verfied)
            retweet_count_list.append(retweet_count)
            followers_count_list.append(followers_count)
            url_count_list.append(url_count)
            photo_count_list.append(photo_count)
            video_count_list.append(video_count)
            Id_list.append(df['Id'][i])
            label_list.append(df['Label'][i])
            text_list.append(df['Text'][i])
            truncated_list.append(truncated)
            listed_count_list.append(listed_count)
            statuses_count_list.append(statuses_count)
            user_created_at_list.append(user_created_at)
            hashtag_count_list.append(hashtag_count)
            friends_count_list.append(friends_count)
            user_url_list.append(user_url)
            print(verfied, retweet_count, followers_count, url_count, photo_count, truncated, listed_count,
                  statuses_count, user_created_at, hashtag_count, friends_count,user_url)
            # tweets_by_API.append(json)
        except tweepy.TweepError as e:
            print(e)
            # wrong_ones.append([i, e])
        idx += 1

    new_df = pd.DataFrame({
        'ID': Id_list,
        'Text': text_list,
        'verfied': verfied_list,
        'retweet_count': retweet_count_list,
        'followers_count': followers_count_list,
        'url_count': url_count_list,
        'photo_count': photo_count_list,
        # 'video_count': video_count_list,
        'truncated': truncated_list,
        'listed_count': listed_count_list,
        'statuses_count': statuses_count_list,
        'user_created_at': user_created_at_list,
        'hashtag_count': hashtag_count_list,
        'friends_count': friends_count_list,
        'user_url': user_url_list,
        'Label': label_list

    })
    new_df.to_csv(name + '_meta.tsv', sep="\t")


crawl('valid')

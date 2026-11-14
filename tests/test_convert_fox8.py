from scripts.convert_fox8 import tweet_to_event

USER = {"id_str": "42", "screen_name": "acct42", "created_at": "Sat Jun 23 10:57:13 +0000 2012",
        "followers_count": 98, "friends_count": 118, "statuses_count": 125,
        "default_profile_image": True}


def tweet(**extra):
    base = {"id_str": "1000", "created_at": "Fri Apr 21 14:47:30 +0000 2023",
            "text": "hello world", "entities": {"hashtags": []}, "user": USER,
            "in_reply_to_status_id_str": None}
    base.update(extra)
    return base


def test_retweet_points_at_original_and_measures_latency():
    original = {"id_str": "900", "created_at": "Fri Apr 21 14:47:00 +0000 2023",
                "user": {"id_str": "7"}, "entities": {"hashtags": [{"text": "Crypto"}]}}
    event = tweet_to_event(tweet(text="RT @x: big news…", retweeted_status=original), "bot", "fox8")
    assert event["event_type"] == "retweet"
    assert event["tweet_id"] == "900"
    assert event["author_id"] == "7"
    assert event["reply_latency_seconds"] == 30
    assert event["tweet_text"].endswith(" #Crypto")  # hashtag recovered from the original
    assert event["is_bot_truth"] == 1


def test_reply_points_at_parent_without_latency():
    event = tweet_to_event(tweet(in_reply_to_status_id_str="555", in_reply_to_user_id_str="9"),
                           "human", "gilani-17")
    assert (event["event_type"], event["tweet_id"], event["author_id"]) == ("reply", "555", "9")
    assert event["reply_latency_seconds"] == ""
    assert event["is_bot_truth"] == 0


def test_post_maps_profile_fields():
    event = tweet_to_event(tweet(), "bot", "fox8")
    assert event["event_type"] == "post" and event["tweet_id"] == "1000"
    assert event["timestamp"] == "2023-04-21T14:47:30Z"
    assert event["actor_created_at"] == "2012-06-23T10:57:13Z"
    assert (event["actor_followers"], event["actor_following"], event["actor_default_avatar"]) == (98, 118, 1)


def test_hashtag_already_in_text_is_not_duplicated():
    event = tweet_to_event(tweet(text="love #NFT", entities={"hashtags": [{"text": "NFT"}]}), "bot", "fox8")
    assert event["tweet_text"] == "love #NFT"

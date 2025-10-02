#implementing all the operations related to web scraping and using bright data service
from dotenv import load_dotenv #imports environment variables
import os
import requests
from urllib.parse import quote_plus #turns normal string into a string we can include in a query parameter
from snapshot_operations import download_snapshot, poll_snapshot_status

load_dotenv()

dataset_id = "gd_lvz8ah06191smkebj4"

#Building the api request for Bright Data
def _make_api_request(url, **kwargs):
    api_key = os.getenv("BRIGHTDATA_API_KEY")
    #need to send headers to tell Bright Data who we are
    headers = {
        "Authorization": f"Bearer {api_key}",
        #The format of our requests' body is JSON
        "Content-Type": "application/json"
    }
    try:
        #passes some data to Bright Data
        response = requests.post(url, headers=headers, **kwargs)
        #Raising an exception if we dont get an okay status
        response.raise_for_status()
        #Otherwise, we return the response.json()
        return response.json()
    #handling network request error
    except requests.RequestException as e:
        print(f"API Request Error: {e}")
        return None
    #handling all other errors
    except Exception as e:
        print(f"Unknown error: {e}")
        return None

#function will be written dynamically so it can be used multiple times, depending on search engine
def serp_search(query, engine = "google"):
    if engine == "google":
       base_url = "https://www.google.com/search"
    elif engine == "bing":
        base_url = "https://www.bing.com/search"
    else:
        raise ValueError(f"Unknown engine {engine}")

    url = "https://api.brightdata.com/request"

    #how we send the search request which will hit the search engine and return us back the information
    payload = {
        "zone": "ai_agent",
        "url": f"{base_url}?q={quote_plus(query)}&brd_json=1",
        "format": "raw",
    }

    full_response = _make_api_request(url, json=payload)
    if not full_response:
        return None

    extracted_data = {
        "knowledge": full_response.get("knowledge", {}),
        "organic": full_response.get("organic", []),
    }
    #returns pretty quickly since bright data did the indexing of the data
    return extracted_data

def trigger_and_download_snapshot(trigger_url, params, data, operation_name = "operation"):
    #Make API Request to bright data, get the snapshot information, and then well pull that snapshot until its ready and download
    trigger_result = _make_api_request(trigger_url, params = params, json=data)
    #Covers case if we dont get anything, then we cant pull it
    if not trigger_result:
        return None
    snapshot_id = trigger_result.get("snapshot_id")
    #If we have no snapshot, we return None
    if not snapshot_id:
        return None
    #Pulling the snapshot
    if not poll_snapshot_status(snapshot_id):
        return None
    raw_data = trigger_result.get("raw_data")
    return raw_data

def reddit_search_api(keyword, date = "All time", sort_by = "Hot", num_of_posts = 90):

    trigger_url = "https://api.brightdata,com/datasets/v3/trigger"

    #“params”/“inputs” = search recipe you give Bright Data.

    params = {
        "dataset_id": "gd_lvz8ah06191smkebj4",
        "include_errors":"true",
        "type":"discover_new",
        "discover_by": "keyword"
    }

    data = [
        {
            "keyword": keyword,
            "date": date,
            "sort_by": sort_by,
            "num_of_posts": num_of_posts,
        }
    ]
    #Taking all the data that was returned to us.
    raw_data = trigger_and_download_snapshot(trigger_url, params, data, operation_name = "reddit")

    if not raw_data:
        return None

    parsed_data = []
    #Each post has a bunch of data assigned to it, so we want to parse through it and only get the info we care about
    for post in raw_data:
        #parsing data to get only the information I care about
        parsed_post = {
            "title": post.get("title"),
            "url": post.get("url"),
        }
        parsed_data.append(parsed_post)
    return {"parsed_posts": parsed_data, "total_found": len(parsed_data)}

#getting the urls from the posts and parsing through them
def reddit_post_retrieval(urls, days_back = 10, load_all_replies = False, comment_limit = None ):
    #Checking if weve been passed a url, if not we exit the function
    if not urls:
        return None
    trigger_url = "https://api.brightdata,com/datasets/v3/trigger"
    params = {
        #Gets us comments from reddit
        "dataset_id": "gd_lvz8ah06191smkebj4", #may be inactive due to Brightdata trial ending
        "include_errors":"true",
    }
    #Creating entries for all urls, obtaining comments for all urls with the provided parameters
    data = [
        {
            "url": url,
            "days_back": days_back,
            "load_all_replies": load_all_replies,
            "comment_limit": comment_limit,

        }
    ]
    raw_data = trigger_and_download_snapshot(trigger_url, params, data, operation_name="reddit comments")
    if not raw_data:
        return None

    parsed_comments = []

    for comment in raw_data:
        parsed_comment = {
            "comment_id": comment.get("comment_id"),
            "content": comment.get("comment"),
            "date": comment.get("date_posted"),
        }

        parsed_comments.append(parsed_comment)
        return {"comments": parsed_comments, "total_retrieved": len(parsed_comments)}
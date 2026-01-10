import os
import time
from datetime import datetime
from xhamster_api import Client

try:
    from pymongo import MongoClient
    from bson import ObjectId
    HAS_MONGODB = True
except ImportError:
    HAS_MONGODB = False
    print("Warning: pymongo not installed. MongoDB functionality will be disabled.")
    print("Install with: pip install pymongo")


def prepare_mongo_data(data):
    """
    Prepare video data for MongoDB insertion:
    - Remove all _id fields (MongoDB will auto-generate)
    - Set Category, SubCategory, SubSubCategory, CreatedBy to null (not ObjectId)
    - Convert $date extended JSON to datetime
    """
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            # Skip _id fields completely
            if key == "_id":
                continue
            
            # Set Category, SubCategory, SubSubCategory, CreatedBy to null
            if key in ["Category", "SubCategory", "SubSubCategory", "CreatedBy"]:
                result[key] = None
            # Handle $date format
            elif isinstance(value, dict) and "$date" in value:
                date_str = value["$date"]
                # Parse ISO format date string
                try:
                    # Replace Z with +00:00 for UTC timezone
                    if date_str.endswith("Z"):
                        date_str = date_str[:-1] + "+00:00"
                    elif "+" not in date_str and "-" in date_str[-6:]:
                        # Already has timezone
                        pass
                    result[key] = datetime.fromisoformat(date_str)
                except Exception as e:
                    # Fallback: try parsing without timezone
                    try:
                        date_str_clean = date_str.replace("Z", "").split(".")[0]
                        result[key] = datetime.fromisoformat(date_str_clean)
                    except:
                        # Last resort: use current time
                        result[key] = datetime.now()
            # Skip $oid format (we don't want ObjectId, we want null for these fields)
            elif isinstance(value, dict) and "$oid" in value:
                # Skip this - we've already handled Category, SubCategory, etc above
                # For any other $oid, set to null
                result[key] = None
            # Recursively process nested structures
            else:
                result[key] = prepare_mongo_data(value)
        return result
    elif isinstance(data, list):
        return [prepare_mongo_data(item) for item in data]
    else:
        return data


def process_video_url(video_url, source_collection, dest_collection, source_doc_id, xhamster_client):
    """
    Process a single video URL:
    1. Fetch video data from xhamster API
    2. Create structured video data
    3. Save to destination MongoDB
    4. Update source document with success: true
    """
    try:
        print(f"\n{'='*60}")
        print(f"Processing: {video_url}")
        print(f"{'='*60}")
        
        # Fetch video data from xhamster API
        video_object = xhamster_client.get_video(video_url)
        
        print(f"Title: {video_object.title}")
        print(f"Thumbnail: {video_object.thumbnail}")
        print(f"Pornstars: {video_object.pornstars}")
        
        # Get M3U8 base URL
        m3u8_url = video_object.m3u8_base_url
        print(f"M3U8 URL: {m3u8_url}")
        
        # Prepare video data structure
        video_data = {
            "_id": {
                "$oid": "6947d16422a1f7321a78c6d2"
            },
            "Title": video_object.title,
            "Description": None,
            "PendingQualities": [],
            "Category": {
                "$oid": "6947c195bc9e939536a9291a"
            },
            "SubCategory": {
                "$oid": "6947c1a4bc9e939536a9291f"
            },
            "SubSubCategory": {
                "$oid": "6947c1b4bc9e939536a92925"
            },
            "IsPremium": False,
            "Tags": [],
            "MetaTitle": video_object.title,
            "MetaDescription": None,
            "MetaKeywords": [],
            "Status": "active",
            "IsTrending": False,
            "IsFeatured": False,
            "AgeRestriction": "NC-17",
            "BlockedCountries": [],
            "IsDMCA": False,
            "Views": 0,
            "Likes": 0,
            "LikedBy": [],
            "Comments": 0,
            "Rating": 0,
            "ReleaseDate": {
                "$date": "2025-12-21T00:00:00.000Z"
            },
            "Year": 2025,
            "Genre": [],
            "Cast": video_object.pornstars if video_object.pornstars else [],
            "Director": None,
            "CreatedBy": {
                "$oid": "694249d95e4c61d9c0859bef"
            },
            "Videos": [
                {
                    "Quality": "best",
                    "Url": m3u8_url,
                    "FileSize": None,
                    "IsOriginal": True,
                    "_id": {
                        "$oid": "6947d17a22a1f7321a78c70e"
                    }
                },
                {
                    "Quality": "1080p",
                    "Url": m3u8_url,
                    "FileSize": None,
                    "IsOriginal": False,
                    "_id": {
                        "$oid": "6947d17a22a1f7321a78c70f"
                    }
                },
                {
                    "Quality": "720p",
                    "Url": m3u8_url,
                    "FileSize": None,
                    "IsOriginal": False,
                    "_id": {
                        "$oid": "6947d17a22a1f7321a78c710"
                    }
                },
                {
                    "Quality": "480p",
                    "Url": m3u8_url,
                    "FileSize": None,
                    "IsOriginal": False,
                    "_id": {
                        "$oid": "6947d17a22a1f7321a78c711"
                    }
                }
            ],
            "Subtitles": [],
            "createdAt": {
                "$date": "2025-12-21T10:52:20.815Z"
            },
            "updatedAt": {
                "$date": "2026-01-10T05:42:07.512Z"
            },
            "Slug": video_object.title.lower().replace(" ", "-"),
            "__v": 1,
            "Thumbnail": video_object.thumbnail,
            "Poster": video_object.thumbnail
        }
        
        # Prepare data for MongoDB (remove _id fields and convert extended JSON)
        mongo_data = prepare_mongo_data(video_data)
        
        # Insert into destination MongoDB
        result = dest_collection.insert_one(mongo_data)
        print(f"✓ Video data saved to MongoDB with _id: {result.inserted_id}")
        
        # Update source document with success: true
        source_collection.update_one(
            {"_id": source_doc_id},
            {"$set": {"success": True}}
        )
        print(f"✓ Source document updated with success: true")
        
        return True
        
    except Exception as e:
        print(f"✗ Error processing video: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function to process videos from source MongoDB"""
    if not HAS_MONGODB:
        print("✗ MongoDB not available. Install pymongo to enable database storage.")
        return
    
    # MongoDB connection details
    source_mongodb_uri = "mongodb+srv://indiandigitalservice01:V2aQpeBve8H54rRW@sarkariresult.womtf2i.mongodb.net/xxxdata"
    source_collection_name = "video_urls"
    
    dest_mongodb_uri = "mongodb+srv://globespotmlm:globespotmlm@cluster0.qwwrkho.mongodb.net/aryatechmovies"
    dest_collection_name = "movies"
    
    try:
        # Connect to source MongoDB
        print("Connecting to source MongoDB...")
        source_client = MongoClient(source_mongodb_uri, serverSelectionTimeoutMS=5000)
        source_client.admin.command('ping')
        source_db_name = source_mongodb_uri.split('/')[-1].split('?')[0] if '/' in source_mongodb_uri else 'xxxdata'
        source_db = source_client[source_db_name]
        source_collection = source_db[source_collection_name]
        print(f"✓ Connected to source MongoDB: {source_db_name}/{source_collection_name}")
        
        # Connect to destination MongoDB
        print("Connecting to destination MongoDB...")
        dest_client = MongoClient(dest_mongodb_uri, serverSelectionTimeoutMS=5000)
        dest_client.admin.command('ping')
        dest_db_name = dest_mongodb_uri.split('/')[-1].split('?')[0] if '/' in dest_mongodb_uri else 'aryatechmovies'
        dest_db = dest_client[dest_db_name]
        dest_collection = dest_db[dest_collection_name]
        print(f"✓ Connected to destination MongoDB: {dest_db_name}/{dest_collection_name}")
        
        # Initialize xhamster client
        xhamster_client = Client()
        
        # Get all video URLs that haven't been processed (success != true or doesn't exist)
        query = {
            "$or": [
                {"success": {"$ne": True}},
                {"success": {"$exists": False}}
            ]
        }
        
        video_urls = list(source_collection.find(query))
        total_videos = len(video_urls)
        
        print(f"\n{'='*60}")
        print(f"Found {total_videos} videos to process")
        print(f"{'='*60}\n")
        
        if total_videos == 0:
            print("No videos to process. All videos are already processed.")
            source_client.close()
            dest_client.close()
            return
        
        # Process each video
        processed = 0
        failed = 0
        
        for idx, video_doc in enumerate(video_urls, 1):
            video_url = video_doc.get("url")
            doc_id = video_doc.get("_id")
            
            if not video_url:
                print(f"\n[{idx}/{total_videos}] Skipping document {doc_id}: No URL found")
                continue
            
            print(f"\n[{idx}/{total_videos}] Processing video...")
            
            success = process_video_url(
                video_url=video_url,
                source_collection=source_collection,
                dest_collection=dest_collection,
                source_doc_id=doc_id,
                xhamster_client=xhamster_client
            )
            
            if success:
                processed += 1
            else:
                failed += 1
            
            # Wait 10 seconds before processing next video (except for the last one)
            if idx < total_videos:
                print(f"\nWaiting 10 seconds before processing next video...")
                time.sleep(10)
        
        # Close connections
        source_client.close()
        dest_client.close()
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"Processing Complete!")
        print(f"Total: {total_videos}")
        print(f"Successfully processed: {processed}")
        print(f"Failed: {failed}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
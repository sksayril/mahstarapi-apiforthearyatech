import os
from xhamster_api import Client

# Initialize a Client object
client = Client()

# Fetch a video
video_url = "https://xhamster.com/videos/the-house-owner-was-naked-and-drying-his-dirt-the-broom-man-was-watching-everything-xhWVogf"
video_object = client.get_video(video_url)

# Get current folder for saving videos
current_folder = os.getcwd()
output_folder = os.path.join(current_folder, "downloaded_videos")

# Create output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Print all Video object information
print("=== Video Information ===")
print(f"Title: {video_object.title}")
print(f"URL: {video_object.url}")
print(f"Thumbnail: {video_object.thumbnail}")
print(f"Pornstars: {video_object.pornstars}")
print("========================\n")

# Get M3U8 base URL (the video link)
try:
    m3u8_url = video_object.m3u8_base_url
    print("=== Video Links ===")
    print(f"M3U8 Base URL: {m3u8_url}")
    print("========================\n")
    
    # Get video segments by quality
    print("=== Available Qualities ===")
    segments_best = video_object.get_segments(quality="best")
    segments_480p = video_object.get_segments(quality="480p")
    segments_720p = video_object.get_segments(quality="720p")
    segments_1080p = video_object.get_segments(quality="1080p")
    
    print(f"Best Quality Segments: {len(segments_best) if segments_best else 0} segments")
    print(f"480p Segments: {len(segments_480p) if segments_480p else 0} segments")
    print(f"720p Segments: {len(segments_720p) if segments_720p else 0} segments")
    print(f"1080p Segments: {len(segments_1080p) if segments_1080p else 0} segments")
    print("========================\n")
    
    # Save video information and links to file
    info_file = os.path.join(current_folder, "video_info.txt")
    with open(info_file, "w", encoding="utf-8") as f:
        f.write("=== Video Information ===\n")
        f.write(f"Title: {video_object.title}\n")
        f.write(f"URL: {video_object.url}\n")
        f.write(f"Thumbnail: {video_object.thumbnail}\n")
        f.write(f"Pornstars: {video_object.pornstars}\n\n")
        f.write("=== Video Links ===\n")
        f.write(f"M3U8 Base URL: {m3u8_url}\n\n")
        f.write("=== Available Qualities ===\n")
        f.write(f"Best Quality Segments: {len(segments_best) if segments_best else 0} segments\n")
        f.write(f"480p Segments: {len(segments_480p) if segments_480p else 0} segments\n")
        f.write(f"720p Segments: {len(segments_720p) if segments_720p else 0} segments\n")
        f.write(f"1080p Segments: {len(segments_1080p) if segments_1080p else 0} segments\n")
    
    print(f"✓ Video information saved to: {info_file}")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
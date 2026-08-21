Web architecture:
We push commits to this github repo.
A render.com instance is linked to this repo and periodically updates (but if you push something and want it checked urgently tell me and I'll manually redeploy it). Procfile and requirements.txt are setup functions for render.com so don't touch these please!
The URL (civl3704.joeyhain.org) points to the default URL (civl3704.onrender.com). Don't worry about that stuff.
The render.com instance has a .env that defines TFNSW_API_KEY and TFNSW_GTFS_RT_URL.
When you make changes to app.py (or if you want to test another .py) DO NOT hardcode your own API key into the script! Use API_KEY = os.getenv("TFNSW_API_KEY"). For testing on your local machine keep your API key in a .env file located in the same directory you run the script.
Also note TFNSW_GTFS_RT_URL is the trip update API, NOT the position API. The position API URL is hardcoded into app.py.
As at 21/8/26 the app.py is structured:
load_dotenv()
API_KEY = os.getenv("TFNSW_API_KEY")
FEED_URL = os.getenv("TFNSW_GTFS_RT_URL", "https://api.transport.nsw.gov.au/v1/gtfs/realtime/buses")
SCHEDULE_URL = os.getenv("TFNSW_GTFS_SCHEDULE_URL", "https://api.transport.nsw.gov.au/v1/gtfs/schedule/buses") # this does not get used but it's in here just in case. you don't need this part in other scripts.

# Deliberately hardcoded — NOT sourced from .env's TFNSW_GTFS_RT_URL, which
# is the trip-update feed above. Vehicle positions are a separate GTFS-RT
# product on the TfNSW developer portal with their own subscription.
VEHICLE_POS_URL = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses"
You can copy this into your own scripts and as long as the api key is in the .env it will work.

When you push a new .py please tell me as this might break the render.com instance.

Feel free to change the formatting on this readme and add your own stuff. We need to submit the repository in the final submission so explaining what things do will get paid marks.

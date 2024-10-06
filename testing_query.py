import weaviate
import weaviate.classes.query as wq
import os
from dotenv import load_dotenv

'''
Connection to Weaviate DB
'''
load_dotenv()
print(os.getenv("OPENAI_APIKEY"))
headers = {
    "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
}
client = weaviate.connect_to_local(headers=headers)

# Get the collection
stories = client.collections.get("stories_temp_3")

# Perform query
response = stories.query.near_text(
    query="give me a story with empathy which is for leve3",
    limit=5,
    return_metadata=wq.MetadataQuery(distance=True),
    return_properties=["level", "story_id", "story_name"]
)

# Inspect the response
for o in response.objects:
    print(
        o.properties["level"], o.properties["story_id"], o.properties["story_name"]
    )  # Print the title and release year (note the release date is a datetime object)
    print(
        f"Distance to query: {o.metadata.distance:.3f}\n"
    )  # Print the distance of the object from the query

client.close()

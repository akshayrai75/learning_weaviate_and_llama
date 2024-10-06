
import weaviate
import os
from dotenv import load_dotenv
import weaviate.classes.config as wc
import pandas as pd
from weaviate.util import generate_uuid5
from tqdm import tqdm
import re

'''
Connection to Weaviate DB
'''
load_dotenv()
print(os.getenv("OPENAI_APIKEY"))
headers = {
    "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
}

client = weaviate.connect_to_local(headers=headers)

'''
Importing Data
'''
############################################################
# Define the base directory
base_dir = 'stories'

# Initialize an empty list to store the data
data = []

# Walk through the directory structure
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.txt'):
            # Extract level from the folder name
            level = os.path.basename(root)

            # Extract story_id and story_name from the file name
            match = re.match(r'(\d+)(.*)\.txt', file)
            if match:
                story_id = int(match.group(1))
                story_name = match.group(2)

                # Read the content of the file
                with open(os.path.join(root, file), 'r', encoding="utf-8") as f:
                    story = f.read()

                # Create the object
                story_obj = {
                    'level': level,
                    'story_id': story_id,
                    'story_name': story_name,
                    'story': story
                }

                # Append the object to the data list
                data.append(story_obj)

# Create a DataFrame from the data list
df = pd.DataFrame(data)

# Display the DataFrame
# print(df)
############################################################

'''
Create collection
'''
client.collections.create(
    name="stories_temp_3",  # The name of the collection ('MM' for multimodal)
    properties=[
        wc.Property(name="story_name", data_type=wc.DataType.TEXT),
        wc.Property(name="story", data_type=wc.DataType.TEXT),
        wc.Property(name="story_id", data_type=wc.DataType.INT),
        wc.Property(name="level", data_type=wc.DataType.TEXT),
    ],
    # Define & configure the vectorizer module
    vectorizer_config=wc.Configure.Vectorizer.multi2vec_clip(
        text_fields=[wc.Multi2VecField(name="story_name", weight=0.3),
                     wc.Multi2VecField(name="story", weight=0.4),
                     wc.Multi2VecField(name="level", weight=0.3)],  # 10% of the vector is from the title
    ),
    # Define the generative module
    generative_config=wc.Configure.Generative.openai()
)

'''
We create a collection object (with client.collections.get) so we can interact with the collection.
'''
stories = client.collections.get("stories_temp_3")

'''
Enter Context Manager
'''
with stories.batch.fixed_size(50) as batch:
    # Loop through the data
    for i, _story in tqdm(df.iterrows()):
        # Build the object payload
        _story_obj = {
            "story_name": _story["story_name"],
            "story": _story["story"],
            "story_id": _story["story_id"],
            "level": _story["level"]
        }

        # Add object to batch queue
        batch.add_object(
            properties=_story_obj,
            uuid=generate_uuid5(_story["story_id"])
            # references=reference_obj  # You can add references here
        )
        # Batcher automatically sends batches

'''
Error Handling
Because a batch includes multiple objects, it's possible that some objects will fail to import. The batcher saves these 
errors.
'''
# Check for failed objects
if len(stories.batch.failed_objects) > 0:
    print(f"Failed to import {len(stories.batch.failed_objects)} objects")
    for failed in stories.batch.failed_objects:
        print(f"e.g. Failed to import object with error: {failed.message}")

client.close()

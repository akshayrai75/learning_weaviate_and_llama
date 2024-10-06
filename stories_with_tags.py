import weaviate
import os
from dotenv import load_dotenv
import weaviate.classes.config as wc
import pandas as pd
from weaviate.util import generate_uuid5
from tqdm import tqdm
import re
import requests

load_dotenv()

headers = {
    "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
}
client = weaviate.connect_to_local(headers=headers)
base_dir = 'stories'
data = []


# Function to generate tags using the local Llama 2 API
def generate_tags(_story):
    response = requests.post('http://localhost:5000/generate_tags', json={'story': _story})
    tags = response.json()['tags']
    return tags


# Load stories and generate tags
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.txt'):
            level = os.path.basename(root)
            match = re.match(r'(\d+)(.*)\.txt', file)
            if match:
                story_id = int(match.group(1))
                story_name = match.group(2)
                with open(os.path.join(root, file), 'r', encoding="utf-8", errors='replace') as f:
                    story = f.read()
                    story = re.sub(r'\n*', ' ', story)
                story_obj = {
                    'level': level,
                    'story_id': story_id,
                    'story_name': story_name,
                    'story': story,
                    'tags': generate_tags(story)  # Generate tags
                }
                data.append(story_obj)

df = pd.DataFrame(data)

# Create collection in Weaviate
client.collections.create(
    name="stories_temp_6",
    properties=[
        wc.Property(name="story_name", data_type=wc.DataType.TEXT),
        wc.Property(name="story", data_type=wc.DataType.TEXT),
        wc.Property(name="story_id", data_type=wc.DataType.INT),
        wc.Property(name="level", data_type=wc.DataType.TEXT),
        wc.Property(name="tags", data_type=wc.DataType.TEXT_ARRAY)  # Store tags
    ],
    vectorizer_config=wc.Configure.Vectorizer.multi2vec_clip(
        text_fields=[wc.Multi2VecField(name="story_name", weight=0.3),
                     wc.Multi2VecField(name="story", weight=0.4),
                     wc.Multi2VecField(name="level", weight=0.3)],
    ),
    generative_config=wc.Configure.Generative.custom(
        url='http://localhost:5000/generate_tags'
    )
)

stories = client.collections.get("stories_temp_6")

# Batch import stories with tags
with stories.batch.fixed_size(50) as batch:
    for i, _story in tqdm(df.iterrows()):
        _story_obj = {
            "story_name": _story["story_name"],
            "story": _story["story"],
            "story_id": _story["story_id"],
            "level": _story["level"],
            "tags": _story["tags"]
        }
        batch.add_object(
            properties=_story_obj,
            uuid=generate_uuid5(_story["story_id"])
        )

if len(stories.batch.failed_objects) > 0:
    print(f"Failed to import {len(stories.batch.failed_objects)} objects")
    for failed in stories.batch.failed_objects:
        print(f"e.g. Failed to import object with error: {failed.message}")

client.close()

print(df[['story_name', 'tags']])

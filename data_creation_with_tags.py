import weaviate
import os
from dotenv import load_dotenv
import weaviate.classes.config as wc
import pandas as pd
from weaviate.util import generate_uuid5
from tqdm import tqdm
import re
import openai

load_dotenv()
openai.api_key = os.getenv("OPENAI_APIKEY")

headers = {
    "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
}
client = weaviate.connect_to_local(headers=headers)
base_dir = 'stories'
data = []

# Function to generate tags using OpenAI
def generate_tags(story):
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that generates tags for stories."},
            {"role": "user", "content": f"Generate tags for the following story:\n\n{story}\n\nTags:"}
        ],
        max_tokens=50,
        n=1,
        stop=None,
        temperature=0.5
    )
    tags = response.choices[0].message['content'].strip().split(',')
    return [tag.strip() for tag in tags]

# Load stories and generate tags
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.txt'):
            level = os.path.basename(root)
            match = re.match(r'(\d+)(.*)\.txt', file)
            if match:
                story_id = int(match.group(1))
                story_name = match.group(2)
                with open(os.path.join(root, file), 'r', encoding="utf-8") as f:
                    story = f.read()
                story_obj = {
                    'level': level,
                    'story_id': story_id,
                    'story_name': story_name,
                    'story': story,
                    'tags': generate_tags(story)  # Generate tags
                }
                data.append(story_obj)
                print(story_obj.get('level'), story_obj.get('story_name'), story_obj.get('tags'))

df = pd.DataFrame(data)

# Create collection in Weaviate
client.collections.create(
    name="stories_temp_4",
    properties=[
        wc.Property(name="story_name", data_type=wc.DataType.TEXT),
        wc.Property(name="story", data_type=wc.DataType.TEXT),
        wc.Property(name="story_id", data_type=wc.DataType.INT),
        wc.Property(name="level", data_type=wc.DataType.TEXT),
        wc.Property(name="tags", data_type=wc.DataType.TEXT_ARRAY)  # Store tags
    ],
    vectorizer_config=wc.Configure.Vectorizer.multi2vec_clip(
        text_fields=[wc.Multi2VecField(name="story_name", weight=0.2),
                     wc.Multi2VecField(name="story", weight=0.6),
                     wc.Multi2VecField(name="level", weight=0.2)],
    ),
    generative_config=wc.Configure.Generative.openai()
)

stories = client.collections.get("stories_temp_4")

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

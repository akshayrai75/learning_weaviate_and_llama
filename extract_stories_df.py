import os
import pandas as pd
import re

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
print(df.head())
print(len(df))

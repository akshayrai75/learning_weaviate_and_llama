from flask import Flask, request, jsonify
import subprocess
import re

app = Flask(__name__)


@app.route('/generate_tags', methods=['POST'])
def generate_tags():
    data = request.json
    story = data['story']
    result = subprocess.run(
        [
            'D:/personal/Llama_2/llama.cpp/build/bin/Release/llama-cli',
            '-t', '12', '-ngl', '32', '-m',
            'D:/personal/Llama_2/llama.cpp/models/llama-2-7b.Q4_K_M.gguf',
            '-c', '2048', '--temp', '0.8', '--repeat_penalty', '1.1', '-n', '-1', '-p',
            f"Given the following story, generate around 10 relevant tags that capture the main themes, "
            f"elements, and keywords of the story. These tags will be used to store the story in a database for "
            f"easier retrieval. \n\nStory: {story}\n\nTags: "
        ],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'  # Replace characters that can't be decoded
    )
    print("Full output:", result.stdout)
    # Extract tags from the output
    output = result.stdout.strip()

    tags_pattern = re.compile(r'\d+\.\s*(\w+)')
    tags = tags_pattern.findall(output)

    print("\nTAGS: ", tags)
    return jsonify({'tags': tags})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

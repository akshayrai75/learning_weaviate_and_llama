from llama_cpp import Llama

# Path to your model file
model_path = "D:/personal/Llama_2/llama.cpp/models/llama-2-7b-chat.ggmlv3.q4_K_M.bin"

# Initialize the model
llama = Llama(model_path=model_path)

# Define your prompt
prompt = "Explain the concept of quantum computing."

# Generate a response
response = llama(prompt)

# Print the response
print(response)

import requests
from secret_manager import access_secret

API_URL = 'https://api.openai.com/v1/chat/completions'
HEADERS = {
    'Authorization': access_secret("kitrum-cloud", "chat_gpt")
}


def get_response(chat_history):
    post_data = {
        "model": "gpt-3.5-turbo-16k",
        "temperature": 1.0,
        "messages": chat_history
    }
    api_response = requests.post(API_URL, headers=HEADERS, json=post_data)
    if api_response.status_code == 200:
        return api_response.json().get('choices')[0].get('message').get('content')
    else:
        print(api_response.text)
        return 'ohh. I need to go, please contact me later!'


def get_analytics(conversation_str):
    chat_question = f"What is this conversation about in details?\n\n{conversation_str}"
    chat_history = [{"role": "user", "content": chat_question}]
    bot_reply = get_response(chat_history)
    print(bot_reply)
    return {"result": bot_reply}


get_analytics("Hello")

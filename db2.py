
from google.cloud import bigquery
import os
import datetime
import time
from operator import itemgetter
import json
from bs4 import BeautifulSoup
from secret_manager import access_secret
from google.oauth2 import service_account

kitrum_bq_json = json.loads(access_secret("kitrum-cloud", "kitrum_bq"))
credentials = service_account.Credentials.from_service_account_info(kitrum_bq_json)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def get_data_from_bq(client, query):
    query_job = client.query(query)
    print(f"Requesting Data from BigQuery according to query: {query}")
    bq_data = []
    for row in query_job.result():
        row_dict = {}
        for key in row.keys():
            row_dict[key] = row[key]
        bq_data.append(row_dict)
    return bq_data


def convert_to_search_string(list_of_values, key_name):
    values_list = []
    for element in list_of_values:
        element_id = element[key_name]
        values_list.append(f"'{element_id}'")
    return ", ".join(values_list)


def prettify_text(body_type, body):
    if body_type == 'html':
        body = body.replace(" !important", "").replace("width", "").replace("background-color: #ffffff;", "").replace("background-color:rgb(255,255,255)", "")
        soup = BeautifulSoup(body, "html.parser")
        for div in soup.find_all("div", {'class': 'gmail_signature'}):
            div.decompose()
        for div in soup.find_all("div", {'class': 'gmail_quote'}):
            div.decompose()
        for div in soup.find_all("div", {'class': 'x_gmail_quote'}):
            div.decompose()
        for img in soup.find_all("img"):
            img.decompose()
        for blockquote in soup.select('[type="cite"]'):
            blockquote.decompose()

        # OUTLOOK TESTS
        # is_outlook = True if soup.select('div[class="WordSection1"]') else False
        # if is_outlook:
        #     for div in soup.select('div[class="WordSection1"] div'):
        #         div.decompose()
        # print("Is Outlook: ",is_outlook)

        # OUTLOOK TESTS V2
        div_found = False
        try:
            unordered_list = soup.find("div", {"class": "WordSection1"})
            children = unordered_list.findChildren()
            for child in children:
                if child.name == 'div':
                    div_found = True
                if div_found:
                    child.decompose()
        except:
            pass

        # for img in soup.find_all("img"):
        #     img.decompose()

        return str(soup)
    else:
        return body


def format_messages(thread_id, messages, attachments):
    messages_results = []
    min_date, max_date, first_message = None, None, None

    sorted_messages = sorted(messages, key=itemgetter('date'))

    contents = ""
    for message in sorted_messages:
        message_id = message['messageId']
        message_attachments = attachments[message_id] if message_id in attachments else []

        # CLEAR EXISTED ATTACHMENTS
        if len(message_attachments) > 0:
            for attachment in message_attachments:
                if attachment['name'] in contents:
                    message_attachments.remove(attachment)
        contents += message['bodyText']

        attachment_lis = ""
        for message_attachment in message_attachments:
            attachment_lis += f'<li><a href="{message_attachment["url"]}">{message_attachment["name"]}</a></li>'
        attachments_ul = f'<ul>{attachment_lis}</ul>' if attachment_lis else ''

        message_from = message['from']
        message_to = message['to']
        message_cc = message['cc']
        body_type = message['bodyType']
        message_date = message['date']

        if not min_date or message_date < min_date:
            min_date = message_date
            first_message = message

        if not max_date or message_date > max_date:
            max_date = message_date

        message_body = message['bodyText']

        pretty_text = prettify_text(body_type, message_body)

        messages_results.append({
            "msg_id": message_id,
            "from": message_from,
            "to": message_to.replace(", ", "<br>"),
            "cc": message_cc.replace(", ", "<br>"),
            "date": message_date,
            "text": pretty_text,
            "attachments": attachments_ul
        })

    # sorted_messages = sorted(messages_results, key=itemgetter('date'))

    thread_result = {
        "thread_id": thread_id,
        "subject":  first_message['subject'],
        "start_date": str(min_date),
        "last_message_date": str(max_date),
        "messages": messages_results,
        "messages_count": len(messages_results),
        "thread_owner": first_message['messageOwner']
    }

    return thread_result


def get_records(email_address):
    query = f"""SELECT `threadId` FROM `kitrum-cloud.gmail.messages`
WHERE (`to` like '%{email_address}%'
OR `from` like '%{email_address}%'
OR `cc` like '%{email_address}%')
group by `threadId`"""

    threads_response = get_data_from_bq(client, query)
    threads_str = convert_to_search_string(threads_response, 'threadId')
    if not threads_str:
        return []

    # GET THREAD MESSAGES
    query = f"SELECT * FROM `kitrum-cloud.gmail.messages` WHERE `threadId`in ({threads_str})"

    messages_response = get_data_from_bq(client, query)

    messages_by_thread = {}
    for msg in messages_response:
        thread_id = msg['threadId']
        if thread_id not in messages_by_thread:
            messages_by_thread[thread_id] = [msg]
        else:
            current_val = messages_by_thread[thread_id]
            current_val.append(msg)
            messages_by_thread[thread_id] = current_val

    messages_str = convert_to_search_string(messages_response, 'messageId')

    query = f"SELECT * FROM `kitrum-cloud.gmail.attachments` WHERE `messageId`in ({messages_str})"

    # GET MESSAGE ATTACHMENTS
    attachments_response = get_data_from_bq(client, query)

    attachments_by_message = {}

    for attachment in attachments_response:
        attachment_message_id = attachment['messageId']
        attachment_value = {"name": attachment['name'], "url": attachment['url']}

        if attachment_message_id not in attachments_by_message:
            attachments_by_message[attachment_message_id] = [attachment_value]
        else:
            current_value = attachments_by_message[attachment_message_id]
            already_added = False
            for entry in current_value:
                if entry['url'] == attachment['url']:
                    already_added = True
            if not already_added:
                current_value.append(attachment_value)
                attachments_by_message[attachment_message_id] = current_value

    print(attachments_by_message)
    results = []
    for thread, thread_messages in messages_by_thread.items():
        thread_data = format_messages(thread, thread_messages, attachments_by_message)
        results.append(thread_data)

    sorted_threads = sorted(results, key=itemgetter('last_message_date'), reverse=True)
    return sorted_threads


def get_authorized_users():
    query = "SELECT email FROM `kitrum-cloud.gmail.tool_users`"
    users_response = get_data_from_bq(client, query)
    allowed_users = []
    for user in users_response:
        user_email = user['email']
        allowed_users.append(user_email)
    return {'users': allowed_users}


def get_last_sync():
    query = "SELECT * FROM `kitrum-cloud.gmail.gmail_syncs` ORDER BY sync_datetime DESC limit 1"
    latest_sync = get_data_from_bq(client, query)[0]
    latest_sync['sync_results'] = json.loads(latest_sync['sync_results'])
    return latest_sync


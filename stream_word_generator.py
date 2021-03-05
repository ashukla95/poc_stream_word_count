import base64
import datetime

from apiclient import discovery
from oauth2client.client import GoogleCredentials

WORD_STREAM_TOPIC = input("Please provide a pubsub topic to continue.")
PUBSUB_SCOPES = ['https://www.googleapis.com/auth/pubsub']
NUM_RETRIES = 3
FILENAME = "kinglear.txt"

def create_client():
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    return discovery.build("pubsub", "v1beta2", credentials=credentials)

def publish(client, pubsub_topic, data_line, msg_attributes=None):
    """Publish to the given pubsub topic."""
    data = base64.b64encode(data_line)
    msg_payload = {'data': data}
    if msg_attributes:
        msg_payload['attributes'] = msg_attributes
    body = {'messages': [msg_payload]}
    resp = client.projects().topics().publish(
        topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp

def read_file():
    all_lines = []
    with open(FILENAME) as reader:
        for line in reader:
            all_lines.append(line)
    return all_lines

def start_stream():
    print("creating client for pubsub.")
    client = create_client()
    print("Reading file for streaming.")
    file_lines = read_file()
    print("File read complete. Length: {}".format(len(file_lines)))
    print("publishing all the read lines.")
    try:
        while True:
            for line in file_lines:
                print("sending text to pubsub.")
                publish(client, WORD_STREAM_TOPIC, line.encode("utf-8"), {"timestamp": str(datetime.datetime.now()).encode("utf-8")})
    except Exception as e:
        print("Exception raised: {}".format(e))

if __name__ == "__main__":
    print("starting the word stream.")
    start_stream()




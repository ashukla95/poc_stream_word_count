from google.cloud import pubsub, pubsub_v1
import datetime
import os
import sys

def read_file(file_name):
    all_lines = []
    with open(file_name) as reader:
        for line in reader:
            all_lines.append(line)
    return all_lines

def start_stream(project_id, topic_id):

    PROJECT_ID = project_id #"playground-s-11-691e528b"
    TOPIC_ID = topic_id #"word_ingest"
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"
    FILE_NAME = "kinglear.txt"
    # topic = input("Please provide a topic to continue: ")
    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    pubsub_client = pubsub.PublisherClient(publisher_options=publisher_options)
    topic = pubsub_client.topic_path(PROJECT_ID, TOPIC_ID)
    all_text_lines = read_file(FILE_NAME)
    while True:
        for line in all_text_lines:
            try:
                print("publishing message.")
                future = pubsub_client.publish(topic, line.encode("utf-8"), ordering_key=str(datetime.datetime.now()))
                print("message id: {}".format(future.result()))
            except Exception as e:
                print("EXception is: {}".format(e))
                continue

if __name__ == "__main__":
    project_id = sys.argv[1]
    topic_id = sys.argv[2]
    print("project id: {}, topic id: {}".format(project_id, topic_id))
    start_stream(project_id, topic_id)

from google.cloud import pubsub, pubsub_v1
import datetime
import os

def read_file(file_name):
    all_lines = []
    with open(file_name) as reader:
        for line in reader:
            all_lines.append(line)
    return all_lines

def start_stream():
    
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="key.json"
    FILE_NAME = "kinglear.txt"
    topic = input("Please provide a topic to continue: ")
    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    pubsub_client = pubsub.PublisherClient(publisher_options=publisher_options)
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
    start_stream()



# topics = projects/playground-s-11-84d094ad/topics/word_ingest

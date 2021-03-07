echo "setting topic id for the pubsub."
TOPIC_ID="word_ingest"

echo "creating topic in pubsub with no subscriptions."
gcloud pubsub topics create $TOPIC_ID

echo "cloing the repository."
git clone https://github.com/ashukla95/poc_stream_word_count.git

echo "entering the directory"
cd poc_stream_word_count

echo "activating the virtual environment"
source stream_env/Scripts/activate

echo "getting the project ID."
PROJECT_ID=`gcloud config get-value project`


echo "starting the stream of words to cloud pubsub."
python3 stream_word_generator.py "$PROJECT_ID" "$TOPIC_ID"

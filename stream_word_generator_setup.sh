PWD=`pwd`
/usr/local/bin/virtualenv --python=python3 venv
echo $PWD
activate () {
    . $PWD/venv/bin/activate
}

echo "activating the shell environment."
activate

echo "setting topic id for the pubsub."
TOPIC_ID="word_ingest"

echo "creating topic in pubsub with no subscriptions."
gcloud pubsub topics create $TOPIC_ID

echo "cloing the repository."
git clone https://github.com/ashukla95/poc_stream_word_count.git

echo "entering the directory"
cd poc_stream_word_count

echo "installing the required libraries for pubsub stream."
pip3 install google-cloud-pubsub


echo "getting the project ID."
PROJECT_ID=`gcloud config get-value project`


echo "starting the stream of words to cloud pubsub."
python3 stream_word_generator.py "$PROJECT_ID" "$TOPIC_ID"


# echo "start a dataflow pipeline"
# python3 -m stream_words \
# --runner DataflowRunner \
# --region us-east1 \
# --project "$PROJECT_ID" \
# --project_known "$PROJECT_ID" \
# --staging_location gs://"$PROJECT_ID"/staging \
# --temp_location gs://"$PROJECT_ID"/temp \
# --template_location gs://"$PROJECT_ID"/templates/stream_words \
# --table stream_word_table \
# --dataset test_dataset \
# --input_topic projects/"$PROJECT_ID"/topics/word_ingest
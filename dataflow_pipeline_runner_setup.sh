echo "entering the directory"
cd poc_stream_word_count

PWD=`pwd`
/usr/local/bin/virtualenv --python=python3 venv
echo $PWD
activate () {
    . $PWD/venv/bin/activate
}

echo "activating the shell environment."
activate

echo "install apache beam-gcp related python libraries"
pip3 install "apache-beam[gcp]"

echo "getting the project ID."
PROJECT_ID=`gcloud config get-value project 2> /dev/null`

echo "The project ID is"
echo "$PROJECT_ID"

echo "creating cloud storage bucket
gsutil mb gs://"$PROJECT_ID"

echo "creating bigquery dataset"
bq mk --dataset "$PROJECT_ID":"test_dataset"

echo "start a dataflow pipeline"
python3 -m stream_words \
--runner DataflowRunner \
--region us-east1 \
--project "$PROJECT_ID" \
--staging_location gs://"$PROJECT_ID"/staging \
--temp_location gs://"$PROJECT_ID"/temp \
--template_location gs://"$PROJECT_ID"/templates/stream_words \
--table stream_word_table \
--dataset test_dataset \
--input_topic projects/"$PROJECT_ID"/topics/word_ingest



echo "start a dataflow pipeline"
python3 -m stream_words \
--runner DataflowRunner \
--region us-east1 \
--project $PROJECT_ID \
--staging_location gs://$PROJECT_ID/staging \
--temp_location gs://$PROJECT_ID/temp \
--template_location gs://$PROJECT_ID/templates/stream_words \
--table stream_word_table \
--dataset test_dataset \
--input_topic projects/$PROJECT_ID/topics/word_ingest
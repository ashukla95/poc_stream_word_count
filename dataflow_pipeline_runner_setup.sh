echo "creating python virtual environment for dataflow."
python3 -m venv dataflow_env

echo "activating the virtual environment"
source dataflow_env/bin/activate

echo "install apache beam-gcp related python libraries"
pip3 install "apache-beam[gcp]"

echo "getting the project ID."
PROJECT_ID=`gcloud config get-value project`

echo "creating cloud storage bucket
gsutil mb gs://"$PROJECT_ID"

echo "creating bigquery dataset"
bq mk --dataset "$PROJECT_ID":"$PROJECT_ID"

echo "start a dataflow pipeline"
python3 -m stream_words \
--runner DataflowRunner \
--region us-east1 \
--project "$PROJECT_ID" \
--staging_location gs://"$PROJECT_ID"/staging \
--temp_location gs://"$PROJECT_ID"/temp \
--template_location gs://"$PROJECT_ID"/templates/stream_words \
--table stream_word_table \
--dataset "$PROJECT_ID" \
--input_topic "projects/$PROJECT_ID/topics/word_ingest"

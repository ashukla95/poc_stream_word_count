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
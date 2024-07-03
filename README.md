# data-engineering-coder

# Using it running a simple file

This project aims to get data from spotify and load it in a database.
Data may be of interesting to artist, producers and other players in music industry.

To use this repo, create .env with database and spotify keys in the root.

Run the following command if you don't have a venv created:
python3 -m venv venv

After, please activate the virtual environment running the following command, while being at the root of the project:
source venv/bin/activate

Make sure pip version is 24.0 or higher by running:
pip --version

If it is not, please run:
pip install --upgrade pip

After, install the requirements by running:
pip install -r requirements.txt

To run the script please run:
python3 etl.py

# To use it with Docker and Airflow

Move previous .env file with database and spotify keys to dags folder
Create another .env file with airflow userId value in the root.

Run:
docker compose up --build

Once it is running, go to http://localhost:8080 and log in with:
user: airflow
pass: airflow

Once logged in, please go to DAGs and activate the one called ingestion_data.
Tasks should start and data will be loaded into redshift.

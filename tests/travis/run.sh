#export PYTHONPATH=.
export PYTHONPATH=.;tests

python active_data/app.py --settings=tests/travis/app.json &
sleep 5

export TEST_CONFIG=tests/travis/app.json
#python -m unittest -m unittest discover tests --failfast
python -m unittest -m unittest test_load_and_save_queries.TestLoadAndSaveQueries
exit

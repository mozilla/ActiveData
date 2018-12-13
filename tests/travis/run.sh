export PYTHONPATH=.:vendor

python active_data/app.py --settings=tests/travis/app.json &
sleep 5

export TEST_CONFIG=tests/travis/app.json
python -m unittest discover tests --failfast
#python -m unittest test_load_and_save_queries.TestLoadAndSaveQueries
#python -m unittest tests.test_jx.test_expressions_w_set_ops.TestSetOps.test_left_and_right

exit

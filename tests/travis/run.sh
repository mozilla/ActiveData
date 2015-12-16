export PYTHONPATH=.

python active_data/app.py --settings=tests/travis/app.json &
sleep 5
python -m unittest discover tests --failfast
exit

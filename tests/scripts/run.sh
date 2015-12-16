export PYTHONPATH=.

python active_data/app.py --settings=tests/config/app_travis_settings.json &
sleep 5
python -m unittest discover tests
exit

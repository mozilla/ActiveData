export PYTHONPATH=.

python active_data/app.py --settings=tests/config/app_travis_settings.json &
python -m unittest discover tests
exit

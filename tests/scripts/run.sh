export PYTHONPATH=.

python active_data/app.py --settings=tests/config/app_dev_settings.json &
python -m unittest discover tests
exit

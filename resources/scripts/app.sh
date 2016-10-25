


sudo -i

export HOME=/home/ec2-user
export PYTHONPATH=.
cd ~/temp
python active_data/app.py  --settings=tests/config/app_staging_settings.json
export HOME=/home/ec2-user
export PYTHONPATH=.
cd ~/temp
python active_data/app.py  --settings=tests/config/app_staging_settings.json

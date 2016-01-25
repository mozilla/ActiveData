cd ~/ActiveData/
git checkout master
git stash
git pull origin master


sudo -i
export PYTHONPATH=.
export HOME=/home/ec2-user
cd ~/ActiveData/
python active_data/app.py --settings=resources/config/staging_settings.json &
disown -h
tail -f  results/logs/etl.log

# DO NOT HANG ONTO PROCESS (nohup)
#nohup python27 testlog_etl/etl.py --settings=etl_settings.json &

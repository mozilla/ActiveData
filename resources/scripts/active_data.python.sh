cd /home/ubuntu/ActiveData/
git checkout master
git stash
git pull origin master

export PYTHONPATH=.
python testlog_etl/etl.py --settings=resources/settings/staging_settings.json &
disown -h
tail -f  results/logs/etl.log

# DO NOT HANG ONTO PROCESS (nohup)
#nohup python27 testlog_etl/etl.py --settings=etl_settings.json &

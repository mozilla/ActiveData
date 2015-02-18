cd /home/ubuntu/ActiveData/
git checkout etl
git stash
git pull origin master
git stash apply

sudo cp /home/ubuntu/ActiveData/resources/supervisor/active_data.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart all
sudo supervisorctl

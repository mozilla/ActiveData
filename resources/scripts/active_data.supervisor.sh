cd /home/ubuntu/ActiveData/
sudo git checkout master
sudo git stash clear
sudo git stash
sudo git pull origin master
sudo git stash apply

sudo cp /home/ubuntu/ActiveData/resources/supervisor/active_data.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl restart all
sudo supervisorctl

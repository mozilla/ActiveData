sudo apt-get install -y supervisor

sudo service supervisor start

cd /home/ubuntu
mkdir -p /home/ubuntu/ActiveData/results/logs

sudo cp /home/ubuntu/ActiveData/resources/supervisor/active_data.conf /etc/supervisor/conf.d/

sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl

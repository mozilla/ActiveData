sudo apt-get install -y supervisor

sudo service supervisor start

cd /home/ubuntu
mkdir -p /home/ubuntu/ActiveData/results/logs

sudo cp /home/ubuntu/ActiveData/resources/supervisor/etl.conf /etc/supervisor/conf.d/

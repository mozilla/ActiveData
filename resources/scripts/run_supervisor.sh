
# SUPERVISOR CONFIG
sudo cp ~/ActiveData/resources/config/supervisord.conf /etc/supervisord.conf

# START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/local/bin/supervisord -c /etc/supervisord.conf

# READ CONFIG
sudo supervisorctl reread
sudo supervisorctl update

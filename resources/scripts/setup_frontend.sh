# USE THIS TO INSTALL PYTHON 2.7 ONTO UBUNTU EC2 INSTANCE
# 2.7 IS ALREADY DEFAULT

sudo apt-get update
sudo apt-get -y install python-pip python-dev nginx git-core


#INSTALL gunicorn
sudo pip install gunicorn

#INSTALL nginx
sudo yum install nginx

# IMPORTANT: nginx INSTALL SCREWS UP PERMISSIONS
sudo chown -R ec2-user:ec2-user /var/lib/nginx/





mkdir  ~/temp
cd  ~/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
cd  ~


git clone https://github.com/klahnakoski/ActiveData.git

cd ~/ActiveData/
git checkout master


sudo pip install gunicorn flask
sudo pip install -r requirements.txt


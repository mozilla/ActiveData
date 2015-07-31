# USE THIS TO INSTALL PYTHON 2.7 ONTO UBUNTU EC2 INSTANCE
# 2.7 IS ALREADY DEFAULT

mkdir  ~/temp
cd  ~/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py

cd  ~
sudo apt-get -y install git-core


git clone https://github.com/klahnakoski/ActiveData.git

cd ~/ActiveData/
git checkout master
sudo pip install -r requirements.txt


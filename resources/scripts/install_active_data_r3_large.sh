
# FOR AMAZON AMI ONLY
# ENSURE THE EC2 INSTANCE IS GIVEN A ROLE THAT ALLOWS IT ACCESS TO S3 AND DISCOVERY
# THIS EXAMPLE WORKS, BUT YOU MAY FIND IT TOO PERMISSIVE
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "NotAction": "iam:*",
#       "Resource": "*"
#     }
#   ]
# }


# NOTE: NODE DISCOVERY WILL ONLY WORK IF PORT 9300 IS OPEN BETWEEN THEM

# ORACLE'S JAVA VERISON 8 IS APPARENTLY MUCH FASTER
# YOU MUST AGREE TO ORACLE'S LICENSE TERMS TO USE THIS COMMAND
cd /home/ec2-user/
mkdir temp
cd temp
wget -c --no-cookies --no-check-certificate --header "Cookie: s_cc=true; s_nr=1425654197863; s_sq=%5B%5BB%5D%5D; oraclelicense=accept-securebackup-cookie; gpw_e24=http%3A%2F%2Fwww.oracle.com%2Ftechnetwork%2Fjava%2Fjavase%2Fdownloads%2Fjre8-downloads-2133155.html" "http://download.oracle.com/otn-pub/java/jdk/8u40-b25/jre-8u40-linux-x64.rpm" --output-document="jdk-8u5-linux-x64.rpm"
sudo rpm -i jdk-8u5-linux-x64.rpm
sudo alternatives --install /usr/bin/java java /usr/java/default/bin/java 20000
export JAVA_HOME=/usr/java/default

#CHECK IT IS 1.8
java -version

# INSTALL ELASTICSEARCH
cd /home/ec2-user/
wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz
tar zxfv elasticsearch-1.7.1.tar.gz
sudo mkdir /usr/local/elasticsearch
sudo cp -R elasticsearch-1.7.1/* /usr/local/elasticsearch/


cd /usr/local/elasticsearch/

# BE SURE TO MATCH THE PULGIN WITH ES VERSION
# https://github.com/elasticsearch/elasticsearch-cloud-aws
sudo bin/plugin install elasticsearch/elasticsearch-cloud-aws/2.7.1


#ES HEAD IS WONDERFUL!
sudo bin/plugin install mobz/elasticsearch-head


#INSTALL GIT
sudo yum install -y git-core


#INSTALL PYTHON27
sudo yum -y install python27

rm -fr /home/ec2-user/temp
mkdir  /home/ec2-user/temp
cd /home/ec2-user/temp
wget https://bootstrap.pypa.io/get-pip.py
sudo python27 get-pip.py
sudo ln -s /usr/local/bin/pip /usr/bin/pip

#INSTALL MODIFIED SUPERVISOR
sudo yum install -y libffi-devel
sudo yum install -y openssl-devel
sudo yum groupinstall -y "Development tools"

sudo pip install pyopenssl
sudo pip install ndg-httpsclient
sudo pip install pyasn1
sudo pip install requests
sudo pip install supervisor-plus-cron

cd /usr/bin
sudo ln -s /usr/local/bin/supervisorctl supervisorctl

#INSTALL gunicorn
sudo pip install gunicorn





# CLONE ACTIVEDATA
cd ~
git clone https://github.com/klahnakoski/ActiveData.git

cd ~/ActiveData/
git checkout master
sudo pip install -r requirements.txt

# PLACE ALL CONFIG FILES

# ELASTICSEARCH CONFIG
sudo cp ~/ActiveData/resources/config/elasticsearch.yml /usr/local/elasticsearch/config/elasticsearch.yml

# FOR SOME REASON THE export COMMAND DOES NOT SEEM TO WORK
# THIS SCRIPT SETS THE ES_MIN_MEM/ES_MAX_MEM EXPLICITLY
sudo cp ~/ActiveData/resources/config/elasticsearch.in.sh /usr/local/elasticsearch/bin/elasticsearch.in.sh

# SUPERVISOR CONFIG
sudo cp ~/ActiveData/resources/config/supervisord.conf /etc/supervisord.conf


mkdir ~/logs
cd /
sudo ln -s /home/ec2-user/logs logs


# START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
sudo /usr/local/bin/supervisord -c /etc/supervisord.conf

# READ CONFIG
sudo supervisorctl reread
sudo supervisorctl update

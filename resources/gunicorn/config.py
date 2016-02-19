bind = ["0.0.0.0:80", "0.0.0.0:443"]

backlog = 64
workers = 1
timeout = 24*60*60

keyfile = "/home/ec2-user/.ssh/activedata.allizom.org.key"
certfile = "/home/ec2-user/.ssh/activedata_allizom_org.crt"
ca_certs = "/home/ec2-user/.ssh/DigiCertCA.crt"



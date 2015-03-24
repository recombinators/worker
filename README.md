# landsat_worker
To install on worker:
spool up t2.medium instance

sudo apt-get update

sudo apt-get install python-pip python-numpy python-scipy libgdal-dev libatlas-base-dev gfortran

sudo pip install landsat-util

sudo easy_install -U pip

sudo pip install boto

sudo apt-get install git

git clone https://github.com/bm5w/landsat_worker.git

create ~/.boto file with aws credentials
  [Credentials]
  aws_access_key_id
  aws_secret_access_key
  
cd landsat_worker
sudo python render_1.py

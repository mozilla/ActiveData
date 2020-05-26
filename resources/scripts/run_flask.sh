

cd ~/ActiveData
git checkout master
git clean -nfxd


export PYTHONPATH=.:vendor
python active_data/app.py  --settings=resources/config/simple_settings.json

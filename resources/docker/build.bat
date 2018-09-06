
REM RUN FROM ROOT ActiveData DIRECTORY
docker build --file resources\docker\activedata.dockerfile --build-arg REPO_CHECKOUT=bugzilla-etl --no-cache --tag mozilla/activedata:v2.3rc14 .



docker build --file resources\docker\activedata.dockerfile --no-cache --tag activedata .


REM RUN FROM ROOT ActiveData DIRECTORY
docker build --file resources\docker\activedata.dockerfile --build-arg REPO_TAG=v2.0 --no-cache --tag activedata .



docker build --file resources\docker\activedata.dockerfile --no-cache --tag activedata .

REM -N DO NOT START SHELL
REM -v VERBOSE
REM -L <local_port>:<distant_host>:<distant_port> <putty config>

plink -v -N -L 9201:localhost:9200 "active-data-frontend 6"

#!/usr/bin/env bash
sudo find /data1 -name translog-* -exec rm -rf {} \;
sudo find /data2 -name translog-* -exec rm -rf {} \;
sudo find /data3 -name translog-* -exec rm -rf {} \;
sudo find /data4 -name translog-* -exec rm -rf {} \;

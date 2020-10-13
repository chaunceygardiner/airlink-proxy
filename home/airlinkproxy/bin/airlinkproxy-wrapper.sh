#!/bin/sh

# Copyright (c) 2020 John A Kline
# See the file LICENSE for your full rights.

cd /home/airlinkproxy/bin
nohup /home/airlinkproxy/bin/airlinkproxyd /home/airlinkproxy/airlinkproxy.conf $@ > /dev/null 2>&1 &

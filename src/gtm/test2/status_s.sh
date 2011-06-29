#!/bin/sh

# GTM start script for test

export PATH=/tmp/pgxc/bin:$PATH
export DATA=/tmp/pgxc/data/gtm_standby

# -------------------------------
# starting active...
# -------------------------------
gtm_ctl -D ${DATA} -S gtm_standby status


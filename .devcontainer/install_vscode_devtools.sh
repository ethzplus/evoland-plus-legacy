#!/usr/bin/env bash

# Abort script if any command exits with non-zero status. Not foolproof.
set -e 

CRAN=${CRAN:-"https://cloud.r-project.org/"} # posit mirror proxy for optimal selection
NCPUS=${NCPUS:-"-1"}

echo "options(repos = c(CRAN = '${CRAN}'), download.file.method = 'libcurl')" >> \
    "${R_HOME}/etc/Rprofile.site"

apt-get update -qq
apt-get -y --no-install-recommends install pipx
rm -rf /var/lib/apt/lists/*

PIPX_BIN_DIR=/usr/local/bin pipx install radian

install2.r --error --skipinstalled -n $NCPUS \
    covr \
    devtools \
    languageserver \
    microbenchmark \
    profvis \
    rlang

# httpgd is currently off of CRAN because of c++ compiler conflicts
# https://github.com/nx10/httpgd/issues/218

R - "remotes::install_github('nx10/httpgd')"

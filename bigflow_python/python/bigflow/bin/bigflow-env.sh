#!/bin/bash

#------Required Parameters-------

# Path of a writable directory on HDFS, used for DAGMR Pipeline pre-deploymenti/job submitting.
BIGFLOW_TEMP_PATH_HDFS="hdfs:///tmp/bigflow"

#------Optional Parameters------

# Path of Bigflow Python log files. If not set, log to screen.
#BIGFLOW_LOG_FILE=

# If Pipeline.get() is triggered for str(p) when p is a PType, it is false by default
BIGFLOW_PTYPE_GET_ON_STR=false

# Path of Bigflow Python home directory, if not set, it is "Path of bin scripts"/../..
#BIGFLOW_PYTHON_HOME=

# Path of Bigflow Backend server, if not set, it is $BIGFLOW_PYTHON_HOME/flume/worker
#BIGFLOW_SERVER_PATH=

# Port used by local Bigflow backend, if not set backend will choose one available automatically
#BIGFLOW_SERVER_PORT=

# If MRPipeline._prepare_cache_archive() is triggered for re-prepare, it is false by default
# you can also use job_conf['reprepare_cache_archive'] when Pipeline.create()
#BIGFLOW_REPREPARE_CACHE_ARCHIVE=false

# It is triggered for auto-update when auto-update fail, it is true by default
BIGFLOW_IGNORE_AUTO_UPDATE_ERROR=true

# It is triggered for auto-update when pyrun, it is true by default
BIGFLOW_AUTO_UPDATE=true

# build doc
export WORK_ROOT=$(cd `dirname "$0"`;pwd)
export BIGFLOW_PYTHON_HOME=${BIGFLOW_PYTHON_HOME:-$WORK_ROOT/../bigflow_python/python/}
BIGFLOW=$BIGFLOW_PYTHON_HOME/bigflow/bin/bigflow
$BIGFLOW pip install sphinx
$BIGFLOW make html
touch _build/html/.touch _build/html/.nojekyll

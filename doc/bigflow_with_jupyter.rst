
下载bigflow_python.tar.gz
""""""""""""""""""""""""""""""""""""

::

  cd ~/
  mkdir bigflow
  cd bigflow
  wget https://github.com/angopher/bigflow/releases/download/untagged-04e38c8705db118f7148/bigflow_python.tar.gz
  tar zxvf bigflow_python.tar.gz


安装pysqlite
""""""""""""""""""""""""""""""""""""

::

  yum install sqlite-devel -y
  export LIBRARY_PATH=~/bigflow/bigflow/python_runtime/lib:$LIBRARY_PATH
  bigflow/bin/bigflow pip install pysqlite


安装jupyter
""""""""""""""""""""""""""""""""""""

::

	bigflow/bin/bigflow pip install jupyter

运行jupyter
""""""""""""""""""""""""""""""""""""

::
	bigflow jupyter notebook --no-browse --port 8732 --ip=*

之后按提示，访问页面即可。

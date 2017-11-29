Bigflow Contributing
======================


Contributing by Testing Releases
-------------------------------

Bigflow 的release 版本会列出新增功能，以及功能优化，Bigflow 用户可以通过测试Bigflow 的release 版本，指出
Bigflow 的性能、功能正确性问题，可以提交问题提交至pull request

Contributing by Reviewing Changes
---------------------------------

Bigflow 用户可以review Bigflow的源代码，并且指出代码中的不合适的地方，或者可能存在的bug。对于发现的问题，
用户可以自己修复或者直接提交pull request

Contributing Documentation Changes
------------------------------------

Bigflow 用户可以对Bigflow 的文档提出修改意见，或者直接修改，并提交pull request

Contributing Bug Reports
-------------------------

Bigflow 用户在使用Bigflow 的过程中，或者review Bigflow 代码的过程中，发现bug，可以自己修复，或者创建pull
request

Bigflow 代码贡献
---------------------

* Code Style Guide

  代码规范(请遵守现有代码规范)

* Bigflow 开发环境构建

  :doc:`编译构建 <build>`

* 测试

::

    cd bigflow/bigflow_python/python
    bigflow/bin/pyrun  xx.py

* 添加单测

  可参考已有的 `*_test.py`

* 运行Python 单测

  eg:

::

    cd bigflow/bigflow_python/python
    ./run-tests bigflow/transform_impls/test/map_test.py

* Pull Request

  Bigflow 开发环境构建模块已经将代码clone 到本地，并且编译通过，所以下面的所有操作在这个基础之上

1. git checkout -b branch_name

2. bug fix or feature develop

3. git add file(new file or modified file)

4. git commit -m "comments"

5. git push origin branch_name

The Review Process
-------------------

* reviewers 和committers 都可以review 用户提交的pull request

* pull request 可能被拒绝

* 如果遇到与其他commiter 提交的修改冲突的情况，那么，需要解决这个冲突才能merge 到master，解决
  冲突可以通过这种方式：

  a. git remote add upstream https://github.com/baidu/bigflow.git

  b. git rebase upstream/master

  c. 解决冲突

  d. 修改提交到branch

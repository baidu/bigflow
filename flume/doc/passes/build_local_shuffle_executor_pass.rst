=============================
BuildLocalShuffleExecutorPass
=============================

1. 功能介绍
-----------------
BuildLocalShuffleExecutorPass的作用是标记LOCAL_SHUFFLE, 并且为LOCAL_SHUFFLE节点创建Executor。主要
过程包含两个步骤：

第一个步骤是标记符合条件的DEFAULT节点, 将其标记成LOCAL_SHUFFLE节点, 标记条件
如下：

* unit为DEFAULT节点
* unit包含PbScope
* unit只包含类型为SHUFFLE的叶节点

第二个步骤是找到符合以上条件的节点, 修改成LOCAL_SHUFFLE, 并创建相应的Executor。


2. 依赖说明
-----------
**PASS**

* `BuildLogicalExecutorPass <build_logical_executor_pass.html>`_

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

无

**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_

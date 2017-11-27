=========================
AddTaskUnitPass
=========================

1. 功能介绍
-----------------
在plan被创建起来之后中, plan中的节点会被直接连接在ROOT节点下, 需要在ROOT节点下创建TASK节点,
使得plan可以按照TASK为单位执行。AddTaskUnitPass的作用就是创建TASK节点。

2. 依赖说明
-----------
**PASS**

* `LoadLogicalPlanPass <load_logical_plan_pass.html>`_
* `RemoveUselessUnionPass <remove_useless_union_pass.html>`_
* `RemoveUnsinkedUnitPass <remove_unsinked_unit_pass.html>`_

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/add_task_unit_pass_0.png
       :width: 600px

上图中, 右侧三个DEFAULT节点直接被连接在ROOT节点之下, 需要在三者与ROOT节点之间创建TASK节点。

**plan图（运行后）**

    .. image:: ../static/passes/add_task_unit_pass_1.png
       :width: 600px

对plan应用了RemoveUnsinkedUnitPass之后, DEFAULT节点与ROOT节点之间创建了TASK节点。


`返回 <../plan_pass.html#pass>`_

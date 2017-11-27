=========================
RemoveUnsinkedUnitPass
=========================

1. 功能介绍
-----------------
在plan中, 只有类型为SINK的叶子节点才能够作为最后一个没有输出的节点, 当没有输出的节点不是SINK类型时,
说明在plan中存在无效的执行分支, 需要被删除。RemoveUnsinkedUnitPass的作用就是删除这类节点。

2. 依赖说明
-----------
**PASS**

无

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/remove_unsinked_unit_pass_0.png
       :width: 600px

上图中, 右侧的执行分支的结束节点没有输出, 但是又不是类型为SINK的节点, 该分支在执行过程在不具有任何
执行的意义, 可以被删除。

**plan图（运行后）**

    .. image:: ../static/passes/remove_unsinked_unit_pass_1.png
       :width: 600px

对plan应用了RemoveUnsinkedUnitPass之后, 执行计划中的右侧执行分支被删除。


`返回 <../plan_pass.html#pass>`_

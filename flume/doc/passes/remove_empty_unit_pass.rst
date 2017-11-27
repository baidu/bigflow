=========================
RemoveEmptyUnitPass
=========================

1. 功能介绍
-----------------
在plan中, 经过优化后的plan可能会产生空的节点, 这些节点没有孩子节点。RemoveEmptyUnitPass的作用就是
删除这类节点。

2. 依赖说明
-----------
**PASS**

无

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/remove_empty_unit_pass_0.png
       :width: 600px

    .. image:: ../static/passes/remove_empty_unit_pass_1.png
       :width: 600px

上图中, 由于plan存在无数据输出的非SINK类型节点, 所以在运行
`RemoveUnsinkedUnitPass <remove_unsinked_unit_pass.html>`_
之后, 产生了大量的空节点。

**plan图（运行后）**

    .. image:: ../static/passes/remove_empty_unit_pass_2.png
       :width: 600px

对plan应用了RemoveEmptyUnitPass之后, 所有空节点都被删除, 只剩下ROOT节点。


`返回 <../plan_pass.html#pass>`_

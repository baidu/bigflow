=========================
RemoveUselessUnionPass
=========================

1. 功能介绍
-----------------
在plan中, 类型为UNION的节点往往只会将多路输入汇集成一路输出, 当UNION节点的输入只有一路的时候,
该UNION节点就失去了UNION节点的作用, 可以被删除。RemoveUselessUnionPass的作用就是删除这类节点。

2. 依赖说明
-----------
**PASS**

无

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/remove_useless_union_pass_0.png
       :width: 600px

上图中, 共有4个UNION节点, 而实际上, 只有第一个UNION节点起到汇聚多路输入的作用, 后面三个UNION的输入
只有一路, 所以可以被删除。

**plan图（运行后）**

    .. image:: ../static/passes/remove_useless_union_pass_1.png
       :width: 600px

对plan应用了RemoveUselessUnionPass之后, 执行计划中多余的UNION节点被删除。


`返回 <../plan_pass.html#pass>`_

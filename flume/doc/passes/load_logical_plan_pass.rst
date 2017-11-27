=========================
LoadLogicalPlanPass
=========================

1. 功能介绍
-----------------
LoadLogicalPlanPass的主要作用是读取PbLogicalPlan, 根据Pb的内容, 生成plan。

2. 依赖说明
-----------
**PASS**

无

**ANALYSIS**

无

3. 图示说明
-------------
**plan图（运行前）**

无图。

**plan图（运行后）**

    .. image:: ../static/passes/load_logical_plan_pass_1.png
       :width: 600px

运行了LoadLogicalPlanPass之后, 生成的plan如上图所示。


`返回 <../plan_pass.html#pass>`_

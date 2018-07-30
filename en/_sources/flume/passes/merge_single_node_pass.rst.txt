=========================
MergeSingleNodePass
=========================

1. 功能介绍
-----------------
在plan创建起来之后，有一些TASK节点孩子节点只有类型为PROCESS，UNION，SINK的节点，而这些节点完全可以
合并到同一个TASK中去，而不会对结果造成任何影响。MergeSingleNodePass的作用就是将只有PROCESS，UNION，
SINK节点的TASK进行合并。

在一些特殊情况下，并不是所有TASK都能被合并，譬如A->B，B->C，C->D，A->D，在A，C，D都可以合并的情
况下，若B也能合并，那么ABCD将会被全比合并到一个TASK中去，若B不能合并，则只能将CD合并。

2. 依赖说明
------------
**PASS**

* `AddTaskUnitPass <add_task_unit_pass.html>`_

**ANALYSIS**

* `DataFlowAnalysis <../analysises/data_flow_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/merge_single_node_pass_0.png
       :width: 600px

上图中，节点task2与节点task5含有SHUFFLE节点，所以无法进行合并，而task1，task6，task3，task4只含有
PROCESS，UNION，SINK节点，所以具有合并条件。

**plan图（运行后）**

    .. image:: ../static/passes/merge_single_node_pass_1.png
       :width: 600px

对plan应用了MergeSingleNodePass之后，执行计划中task1与task6进行了合并，task3与task4进行了合并，
减少了TASK节点的数量。


`返回 <../plan_pass.html#pass>`_

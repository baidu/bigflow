=========================
MergeTaskPass
=========================

1. 功能介绍
-----------------
MergeTaskPass主要包含四个Pass, MergeConcurrencyOneTaskAnalysis, MergeDownstreamTaskAnalysis,
MergeBrotherTaskAnalysis, MergeTaskActionPass。其中, 各个MergeConcurrencyOneTaskAnalysis, 
MergeBrotherTaskAnalysis不会处理所有符合条件的Task, 当找到一组可以合并的Task之后, 便会执行
MergeTaskActionPass操作, 由于MergeDownstreamTaskAnalysis的特殊性, 不会存在连续可合并的上下游
Task，所以Pass会找到所有可以上推的Scope，一次性完成合并操作。

MergeConcurrencyOneTaskAnalysis主要的功能是分析所有Concurrency为1的Task, 并且把能够合并的Task进行
标记, 然后由MergeTaskActionPass执行合并操作。在一些特殊情况下, 并不是所有TASK都能被合并, 譬如A->B,
B->C, C->D, A->D, 在A, C, D都可以合并的情况下, 若B也能合并, 那么ABCD将会被全比合并到一个TASK中去,
若B不能合并, 则只能将CD合并。

MergeDownstreamTaskAnalysis主要的功能是分析所有可能的具有Distributed的Task, 并且将其中能够被上推到
上游Task的Scope进行标记, 然后由MergeTaskActionPass执行上推操作。

MergeBrotherTaskAnalysis主要的功能是分析所有具有相同VertexIndex的Task, 并且标记其中能够合并的Task,
然后由MergeTaskActionPass执行合并操作。

MergeTaskActionPass主要的功能是对标记为需要合并的Task进行合并, 合并Task的同时将具有相同identity的
节点进行合并，重新计算合并后Task的Concurrency。


整个Pass在执行的过程中, 按照以下顺序执行:

* 1. 合并所有Concurrency为1的Task进行合并分析, 若是没有可以合并的Task，执行第2步, 否则执行第1步;
* 2. 合并所有可以上推的Distributed Scope, 若是没有可以上推的Scope, 执行第3步, 否则执行第2步;
* 3. 合并所有属于同一Vertex的Task, 若是没有满足合并要求的Task, 执行结束, 否则执行第3步;


2. 依赖说明
------------
**PASS**

**ANALYSIS**

3. 图示说明
-------------
**plan图（运行前）**

无


**plan图（运行后）**

无


`返回 <../plan_pass.html#pass>`_

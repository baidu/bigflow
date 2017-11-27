==============================
BuildShuffleEncoderDecoderPass
==============================

1. 功能介绍
-----------------
在plan中, task与task之间, 经过SplitShufflePass的拆分, 将PARTIAL_SHUFFLEF分配到数据流上游的task中,
将MERGE_SHUFFLE分配到数据流下游的task中, 从而利用HCE平台完成shuffle工作。在利用平台shuffle的同时,
需要给平台提供keys, ENCODER节点的作用就是在平台shuffle之前, 提供keys, 而DECODER节点的作用是在平台
完成shuffle之后, 将key解析出来。

BuildShuffleEncoderDecoderPass的作用就是在PARTIAL_SHUFFLE与MERGE_SHUFFLE之间添加ENCODER与DECODER,
添加的顺序为先添加DECODER, 再添加ENCODER, 完成之后BuildShuffleEncoderDecoderPass会将被标记的SHUFFLE
节点删除。

2. 依赖说明
------------
**PASS**

* `BuildMapInputPass <build_map_input_pass.html>`_

**ANALYSIS**

* `DataFlowAnalysis <../analysises/data_flow_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/build_shuffle_encoder_decoder_pass_0.png
       :width: 600px

上图中, PARTIAL_SHUFFLE与MERGE_SHUFFLE分别属于两个task。

**plan图（运行后）**

    .. image:: ../static/passes/build_shuffle_encoder_decoder_pass_1.png
       :width: 600px

运行BuildShuffleEncoderDecoderPass之后, 在task之间创建了ENCODER节点, 同时在MERGE_SHUFFLE端创建了
DECODER节点。


`返回 <../plan_pass.html#pass>`_

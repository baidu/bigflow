==============================
BuildShuffleWriterReaderPass
==============================

1. 功能介绍
-----------------
在plan中, task与task之间, 经过SplitShufflePass的拆分, 将PARTIAL_SHUFFLEF分配到数据流上游的task中,
将MERGE_SHUFFLE分配到数据流下游的task中, 从而利用HCE平台完成shuffle工作。在利用平台shuffle的同时,
需要给平台提供keys, ENCODER节点的作用就是在平台shuffle之前, 提供keys, 而DECODER节点的作用是在平台
完成shuffle之后, 将key解析出来。

同时, 需要在ENCODER端添加SHUFFLE_WRITER, 在DECODER端添加SHUFFLE_READER, SHUFFLE_WRITER主要是对数据
进行序列化, SHUFFLE_READER主要是对数据进行反序列化, 从而保证数据在shuffle前与shuffle后的一致性。

BuildShuffleEncoderDecoderPass的作用就是在PARTIAL_SHUFFLE与MERGE_SHUFFLE之间添加SHUFFLE_WRITER与
SHUFFLE_READER, 添加的顺序为先添加READER, 再添加WRITER, 完成之后BuildShuffleWriterReaderPass更新
SHUFFLE_WRITER与SHUFFLE_READER中的数据流信息。

2. 依赖说明
------------
**PASS**

* `BuildShuffleEncoderDecoderPass <build_shuffle_encoder_decoder_pass.html>`_

**ANALYSIS**

* `DataFlowAnalysis <../analysises/data_flow_analysis.html>`_

3. 图示说明
-------------
**plan图（运行前）**

    .. image:: ../static/passes/build_shuffle_writer_reader_pass_0.png
       :width: 600px

上图为运行BuildShuffleEncoderDecoderPass之后的结果, 在此基础上需要添加SHUFFLE_WRITER与
SHUFFLE_READER。

**plan图（运行后）**

    .. image:: ../static/passes/build_shuffle_writer_reader_pass_1.png
       :width: 600px

运行BuildShuffleWriterReaderPass之后, 在task之间创建了SHUFFLE_WRITER节点, 同时在MERGE_SHUFFLE端
创建了SHUFFLE_READER节点。


`返回 <../plan_pass.html#pass>`_

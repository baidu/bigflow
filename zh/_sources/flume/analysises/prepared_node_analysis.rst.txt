=============================
PreparedNodeAnalysis
=============================

1. 功能介绍
-----------------
PreparedNodeAnalysis的作用是将符合条件的节点做上Prepared标记, 被做上Prepared标记的那条边的数据
在发送的时候会被优先发送, 以保证尽快装载进入内存, 具体使用场景请参考
`BuildShuffleEncoderDecoderPass <../passes/build_shuffle_encoder_decoder_pass.html>`_

2. 依赖说明
-----------
**ANALYSIS**

无


`返回 <../plan_pass.html#analysis>`_

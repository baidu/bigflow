############
名次解释
############

Bigflow API: 面向Bigflow最终用户的API，提供了函数式编程风格的高层计算抽象接口，
简单易用。API中最主要的抽象包括Pipeline（计算的入口，同时可以指定某种特定的执行
引擎），PTypes（分布式数据集抽象，概念来自Google FlumeJava中的PCollection，与Spark
RDD类似）

Bigflow Core API: 面向Bigflow开发者，用于逻辑计划描述的API，由Node和Scope构成，拥
有比Bigflow API更加强大的计算描述能力

Bigflow Planner: Bigflow的分布式计算计划优化层，其有两个任务：将逻辑计划翻译为针对
于特定执行引擎的物理执行计划，同时在这个过程中对计划进行逻辑和物理层面的优化。对于
每一个Bigflow所支持的分布式计算执行引擎，其均有相应的Planner实现，例如DCEPlanner，
SparkPlanner等

Bigflow Runtime: Bigflow的分布式计算执行层，负责实际的计算过程。其包含两部分：作为Client
端将作业提交给分布式计算平台的部分，以及作为Worker在分布式平台的执行节点上进行数据计算的部分

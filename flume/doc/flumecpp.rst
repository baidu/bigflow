##########
Flume C++
##########

Flume C++是基于Flume-Core开发的一种方便用户直接使用的C++泛型接口. Flume C++的设计参考了google的
`flumejava <http://pages.cs.wisc.edu/~akella/CS838/F12/838-CloudPapers/FlumeJava.pdf>`_.


基本数据模型和接口
==================

Flume C++的基本类型是PCollection<T>, 该类型抽象了Map阶段的输入流,
即按行组织的数据流, 每一行的类型都是T. 对于一个PCollection<T>对象,
我们不提供直接访问其行数据的接口, 相反我们提供了ParallelDo接口,
用户可以将自定义的functor传入, 对PCollection<T>中的每一行应用该functor.
下面的伪代码反映了其使用方式.

.. code-block:: cpp

    PCollection<std::string> lines = ReadTextFileCollection("hdfs://path/to/file.txt");

    struct SplitLineFn {
        void Process(const std::string& line, EmitFn<std::string>* emit_fn) {
            boost::tokenizer<> tokens(line);
            for (boost::tokenizer<>::iterator it = tokens.begin(); it != tokens.end(); ++it) {
                emit_fn->Emit(*it);
            }
        };
    };
    PCollection<std::string> words = lines.ParallelDo(SplitLineFn(), CollectionOf(Strings()));

从PCollection<T>衍生出来的类型是PTable<K, V>, 实际上PTable<K, V>继承自PCollection< Pair<K, V> >.
返回类型为Pair<K, V>的functor, 可以将PCollection<T>转化为PTable<K, V>.在MR模型中,
PTable<K, V>实际上代表MR中Mapper的输出结果. 下面的伪代码给出了一种使用方法.


.. code-block:: cpp

    struct CountOneFn {
        void Process(const std::string& word, EmitFn<std::pair<std::string, int> >* emit_fn) {
            emit_fn->Emit(std::make_pair(word, 1));
        }
    };
    PTable<std::string, int> words_with_ones =
            words.ParallelDo(CountOneFn(), TableOf(Strings(), Ints()));

PTable<K, V>在PCollection提供的接口之外, 还提供了GroupByKey接口,
这个接口会将PTable<K, V>转化为PTable<K, Collection<V>>. 代码示例如下:

.. code-block:: cpp

    PTable<std::string, Collection<int> > grouped_words_with_ones = words_with_ones.GroupByKey();

这一转换过程可以类比为MapReduce模型中的Shuffle过程,
在这个过程中得到的PTable<K, Collection<V>>是Flume C++的第三种基础类型.
PTable<K, Collection<V>>代表了被分组后的数据, 对于这类数据, 我们可以进行Combine操作.
代码示例:

.. code-block:: cpp

    struct SUM_INTS {
        int Combine(const Collection<int>& inputs) {
            return std::accumulate(inputs.begin(), inputs.end(), 0);
        }
    };
    PTable<std::string, int> word_counts = grouped_words_with_ones.CombineValues(SUM_INTS());

上面的用户代码展示了一个WordCount程序的书写流程. 事实上,
Flume C++所有的数据操作都可以被抽象成在这几种基本数据类型之间的转换. 下面的图展示了转换的路径:

    .. image:: static/flumejava_datamodel.png
       :width: 450px


扩展接口
========

上一节中, 我们展示了模仿FlumeJava提供的Flume C++基本接口. 可以看到,
该接口基本是基于MapReduce模型进行设计. 然而, 从 :doc:`core` 的设计中我们可以看到,
Flume的底层抽象实际上具有比MR模型更强的表达能力. 我们可以从以下几个方面对Flume C++进行扩展:

* 提供更加丰富的接口. 实际上, FlumeJava中的PCollection和
  `Spark <http://spark.apache.org/docs/latest/scala-programming-guide.html>`_
  中的RDD概念有很大相似性. 而在 :doc:`core` 的设计中, 每一个逻辑计划节点只有一路输出,
  因此该逻辑节点实际上可以被看做一个PCollection(RDD). 因此, Spark中为RDD提供的操作方法,
  也都可以较容易的基于 :doc:`core` 实现.

* 提供嵌套分组的PTable. 在FlumeJava的论文中, PTable<K, V>是用来描述带有分组信息的集合,
  PTable<K, Collection<V>>用来描述分组后的集合. 我们可以通过提供PTable<K1, K2, V>这样的集合,
  使得Flume C++同样具有描述嵌套分组的能力. 这样, 用户就可以把同一个算子应用到不同的分组作用域中,
  提高代码复用能力. 该机制经过扩展, 也可能具备支持实时计算的能力, 如:

  .. code-block:: cpp

    PTable<Hour, Minute, std::string> lines =
            ReadRealTimeText("minus://target", 24, 60);

    // Group by the leftmost ONE key
    WordCount(lines.GroupByKey<1>()).SaveEachGroup("hdfs://$1.log.hours");

    // Group by the leftmost TWO key
    WordCount(lines.GroupByKey<2>()).SaveEachGroup("hdfs://$1:$2.log.minuts");

* 实现PObject和Accumulator接口. PObject是FlumeJava中的概念, 它提供了一种方便的手段,
  使得可以从程序中直接获得分布式计算的结果. Accumulator是Spark中的概念,
  它类似于MapReduce中的Counter, 只不过Accumulator可以被绑定到一个程序变量上,
  在分布式计算结束后可以从程序中直接访问到. 这两种机制的加入, 可以方便多轮迭代计算的开发.



API Reference
=============

待补充


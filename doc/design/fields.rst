SchemaPCollection设计文档
````````````````````````````

设计目的
==========

虽然Bigflow现有接口较spark等同类系统抽象层次更高，嵌套数据集已经使得代码复用上有了较大优势，
但因为无字段支持，在字段需求明显时，使用bigflow会降低代码的可读性，且实现也略为复杂。

例如，bigflow中较常见下面的代码：::

    nums = pipeline.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9), (10, 11, 12)])
    sum_by_cols = nums.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))

这种代码一是无法复用sum（甚至更复杂的）函数，
二是如果列数较多且各列做的操作不同时，从下标很难知道每列是干什么的，代码阅读起来困难。

所以，希望能够提供一套类似于spark上的DataFrame的系统，对字段操作进行支持。

设计目标
==========

1. 支持字段操作功能
2. 进一步提升Bigflow用户代码可重用性
3. 用户尽可能容易理解
4. 额外的开销尽可能少
5. 用户代码量尽可能减少

其中，优先级从上到下依次降低，当发生严重冲突时，一般取上边的目标。
关于“用户代码量尽可能减少”这个目标，本方案不作为重心，
因为减少代码量可以在基础的接口之上再想办法提供一些上层库，包装一下来实现，
甚至有了基本API之后用户都能轻松的包装出来一些能减少代码量的函数。

接口设计
==========

接口说明
----------

基本接口
~~~~~~~~~~~

提供一种PCollection，称为SchemaPCollection，用来表示结构化的，带字段的PCollection，
它拥有普通PCollection的所有操作，可以直接当作每个元素是一个dict的PCollection来用。

细节参考：

:mod:`schema <bigflow.schema>`

原则上，select/agg都应支持side_input / broadcast。

以上接口为基本接口，在以上接口上，还需要实现出众多辅助API，如join/distinct/sort_by。

PObject上添加操作符重载
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

让PObject支持以下操作符重载:

    >>> + - * / > < >= <= == not and or

例如：

    >>> a = _pipeline.parallelize(2)
    >>> b = _pipeline.parallelize(1)
    >>> print (a + b).get()  #输出3
    >>> print (a - b).get()  #输出1
    >>> print (a * b).get()  #输出2
    >>> print (a / b).get()  #输出2
    >>> print (a > b).get()  #输出2

注意，需要支持右操作符不是PObject。

    >>> print (not(a * 2 >= 1)).get()  #输出False

有了这些操作符之后，可以使得用户写出的代码更为直观。

输入输出
~~~~~~~~~~

另提供input.SchemaTextFile类和output.SchemaTextFile类，
支持用某种分隔符分隔的文件直接读取成SchemaPCollection，
或者将SchemaPCollection直接write到外部存储。

例如：

    >>> infile= input.SchemaTextFile('./input.txt', columns = ['firstname', 'secondname'])
    >>> intable = pipeline.read(infile)
    >>> outtable = intable.apply(schema.select, lambda cols: {
    >>>     fullname: cols['firstname'] + cols['secondname']
    >>> })
    >>> pipeline.write(outtable, output.SchemaTextFile('./output.txt'))

代码示范
--------

输入数据第一列为websites，第二列为clicknum。
现需求是将websites按逗号拆分成website。
然后按website分组，求出每个website总clicknum, 最大clicknum，以及平均clicknum。

::

    from bigflow import base, schema

    p = base.Pipeline.create('local')

    analytics = p.parallelize([('www.alibaba.com,www.baidu.com,www.tencent.com', 1),
                               ('www.baidu.com,www.tencent.com', 2),
                               ('www.alibaba.com,www.tencent.com', 3),
                               ('www.alibaba.com,www.baidu.com', 2),
                               ('www.alibaba.com,www.jd.com', 1)])\
        .apply(schema.from_tuple, ['websites', 'clicknum'])
        .apply(schema.select, lambda cols: {
            'website': cols['websites'].flat_map(lambda line: line.split(',')),
            'clicknum': cols['clicknum']
        })
        .apply(schema.group_by, ['website'])\
        .apply_values(schema.agg, lambda cols: {
            'max_click_num': cols['clicknum'].max(),
            'sum_click_num': cols['clicknum'].sum(),
            'avg_click_num': cols['clicknum'].sum() / cols['clicknum'].count()
        })
        .apply(schema.flatten)

    print analytics.get()
    """
    输出结果为
    [{'sum_click_num': 7, 'website': 'www.alibaba.com', 'avg_click_num': 1, 'max_click_num': 3},
    {'sum_click_num': 6, 'website': 'www.tencent.com', 'avg_click_num': 2, 'max_click_num': 3},
    {'sum_click_num': 5, 'website': 'www.baidu.com', 'avg_click_num': 1, 'max_click_num': 2},
    {'sum_click_num': 1, 'website': 'www.jd.com', 'avg_click_num': 1, 'max_click_num': 1}]
    """
    print analytics.apply(schema.to_tuple, ['website', 'max_click_num']).get()
    """
    输出结果为
    [('www.alibaba.com', 3), ('www.tencent.com', 3), ('www.baidu.com', 2), ('www.jd.com', 1)]
    """


设计折衷点
------------

1. 与DataFrame的比较

    如下是DataFrame代码：

    ::

        people.filter(people.age > 30)\
              .join(department, people.deptId == department.id)) \
              .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})

    较本文中所描述的方案，在合适的情况下，由于默认不改变字段的命名，不需要为每个字段都重命名，
    且由于DataFrame提供了大量的API接口，且每个接口都接收多种不同风格的参数，所以，
    每处如果选对合适风格的合适的API接口，代码量会可能会比前述接口代码量更少，并且直观上也更像SQL。

    但DataFrame相关接口有以下缺点：

    1. 需要提供max/min/count等UDF，且UDF都是处理单机数据集的，无法进行高级优化，无法复用旧的sum/count等操作。
    2. 字段上可进行的操作极其有限，且扩展起来困难。用户已实现的任意分布式算法都无法复用，如用户实现了个count_distinct，后来需要在某字段上使用时，发现无法复用。
    3. 用户自定义UDF需要派生自某几个特定的类，书写起来略为复杂。
    4. 需要提供数量众多的API才能完成完整的语义，学习成本较大。

    换句话说，Bigflow上述API，主要提供了两个新语义：select/agg，其它的语义在Bigflow中都原来已存在，
    即使有改变，也都极其相似。在添加了这两个新操作之后，可以与旧的任意操作任意组合，
    进而拼接完成一切完备的功能，而DataFrame则为了表达完整语义，提供了大量不同的基本接口，
    且每个接口都提供了数种不同风格的参数，用户学习成本较大；且除显式转回旧风格rdd外，
    没有任何办法复用旧有代码。

    当然，我们也可以同时提供上述风格以及我们的风格的API，
    但无法避免DataFrame本来就有的接口风格不统一，太过庞杂的问题。

    而关于使用bigflow在一些代码书写时，可能会代码量略大于spark的问题，
    由于用户可以很轻松的在我们的API上包装出spark类似的API，所以，问题不大。

2. 为何不直接让普通的元素为dict的PCollection上即可进行上述操作，而要添加特殊的类型？

    如果使用上述接口，普通的PCollection因为不知道有哪些字段，无法拼出select_fn/agg_fn的参数，
    但，但如果把传入的参数改为一个特殊实现的类型，
    在调用中括号操作符时才去生成相应字段对应的PCollection(PObject)，或者把把接口改成下面的样子：::

        p.agg({'* => cnt': transforms.count,
               'A => A_count_distinct': lambda A: A.distinct().count(),
               'A, B => percent_AB': lambda A, B: A.count().map(lambda a, b: a / b, B.count())
        })

    则都可以在及时拿到输入字段，拼出用户要求的数据集。

    此方案优点是用户可以在任意每个元素为dict的PCollection上进行字段操作，
    不需要多一步从别的类型转成SchemaPCollection的操作。

    但有以下劣势：
    1. 即使我们提供了from_tuple/to_tuple函数，用户也可能会认为此函数与手工转化效率相同，而直接手工转换。
        但实际上，使用这些函数，能够避免计算中间出现dict，从而使得计算效率大大提升。
    2. 较难支持以下用法，p.select(lambda cols: cols.update({'a': cols['a'] + 1}) or cols)。
        这样表示只改变少量字段。
    3. 无法从语法层面避免输出字段重复，用户较难发现这个错误的用法，需要在运行层检查。
    4. 需要传入许多函数，需要每个输出字段都指明需要哪几个输入字段，较为繁琐。

    但总体来看，两个方法优劣差异并不明显，各有利弊。

3. cartesian/join的设计

    如果直接复用旧的cartesian，则返回的是一个每个元素是({}, {})的PCollection，则此后，就无法再调用schema相关操作了，且性能也较低。
    这个接口有两种可能的实现，来让用户使用起来更为方便。

    方案一:

        a = SchemaPCollection{a, b, c}

        # 我们把一个有a,b,c三列的SchemaPCollection记作SchemaPCollection{a,b,c}

        b = SchemaPCollection{a, d}

        schema.cartesian(a, b) 返回的是 SchemaPCollection{0.a, 0.b, 0.c, 1.a, 1.d}

    方案二：

        a = SchemaPCollection{a, b, c}

        b = SchemaPCollection{a, d}

        如果是两个SchemaPCollection（或可转为SchemaPCollection的类型），则直接修改原cartesian函数，
        返回的类型表面看来和原来的cartesian一模一样，但是不同之处是可以直接调用select/agg，
        select_fn传入两个{}，分别表示两个表里对应的多个PObject(PCollection)

        例如：

            schema.cartesian(a, b).apply(schema.select, lambda a, b: {'name': a['name'] + b['name']})

        用户也可以直接把a.cartesian(b)返回的类型当作每个元素是一个tuple(dict, dict)的PCollection用。

        类似的, schema.join(a, b, a['id'] == b['searchid'])返回类型与cartesian返回的类型一样一样。

    方案三：

        cartesian不允许用户有重复字段名，或者如果有重复则以左边的为准。

4. max_elements/sort_by等接口设计

    用户可以直接使用PCollection上的max/min/max_elements/min_elements/sort_by来完成相应的功能，
    但由于出现dict操作，性能会比较慢，并且无法在用户无感知的情况下完成优化。
    所以需要提供特殊的这几个函数，但用户如果使用原来接口，也能满足其需求。

    这里主要还是受限于python语言本身，导致将接口的dict设计成一个结构体类似的对象，
    将取字典数据的操作改为取成员，性能也不会有多大改善。
    如果是一些编译型语言，可以把SchemaPCollection的元素设置成一种满足特定约束的对象
    （如JavaBean对象），这样，因为在对象中设置、读取某个字段避免了hash操作，可以大大提高性能。


实现
========

1. API层接口实现

    为了让SchemaPCollection看起来和PCollection一样，能够执行任意的PCollection上的操作，
    可以让SchemaPCollection派生自PCollection，并且里面的数据类型确实是dict。
    但，同时，在SchemaPCollection里有一个成员变量，是一个数据类型为tuple的PCollection，
    另有成员标识出各个字段名，如果是任意的字段操作，则从数据类型为tuple的PCollection进行计算，
    构造出新的SchemaPCollection。
    按照这种方案，可以使得，如果用户没有把SchemaPCollection当成普通的PCollection进行普通PCollection的操作，
    则计算过程中生成的所有元素类型为{}的PCollection都不会在有效路径上，
    都会因为下游没有输出结点而在优化时被删除。从而使得计算过程中不会出现{}，避免了引入{}借来的开销。

    from_tuple/to_tuple/group_by/flatten实现都比较简单，这里略过实现方案。

    agg函数实现时，可以查出有哪些字段，调用多次map，把每个字段都map出一个PCollection，然后拼到dict中，
    作为参数调用用户传入的函数，调用完成后，将用户返回的dict中的多个字段进行cartesian，
    生成每个元素是tuple的PCollection，然后用该PCollection生成每个元素是dict的SchemaPCollection。

    select函数实现时，需要先将原数据进行一个特殊的group_by_every_record的操作（后述如何实现该操作），
    该操作会生成一个PTable,key是一个UniqID,value是每条数据的PObject。然后，
    可以利用该PObject进行map生成各个字段对应的PObject，拼成dict传递给用户，后续处理与agg函数实现类似。

    为何非要搞一个group_by_every_record，可以考虑下边的例子：::

        p.select(lambda cols: {
            'website_cnt': cols['websites'].flat_map(lambda line: line.split()).count()
        })

    另外，需要注意，SchemaPCollection的count/take等功能，可以重写一下，以便从tuple的PCollection上出发，
    避免生成字典。

2. group_by_every_record如何实现

    group_by_every_record就是一种特殊的Shuffle，
    在LogicalPlan里添加一种特殊的ShuffleNode，该ShuffleNode性质在Planner层做优化时，
    与DistribteByDefault完全一致，可以完全复用相关Planner层代码。

    在Runtime层需要实现一种特殊的ShuffleExecutor，它每读到一条正常的Shuffle数据，
    都需要调用一次所有孩子的BeginGroup/FinishGroup。

    另外，如果要支持select时可传入side_input/broadcast，需要支持同一ShuffleScope下存在BroadcastShuffleNode，
    那就需要让GroupByEveryRecordShuffleExecutor可以在Broadcast数据全部到达之前，
    把另外其它路的数据全部缓存下来，直到所有Broadcast数据到达之后，再回放每条数据调用BeginGroup/FinishGroup，
    以及在每个Group内都把Broadcast数据回放一遍。
    （由于目前框架问题，这么简单一个需求实际实现时也略复杂，也许重构框架时可以考虑一下这个问题）

    另有一个需要注意的地方是，group_by_every_record不可使用group_by一个random uniqid的方法实现，
    因为出现不确定性的计算并用它来进行partition的话，可能在MR框架failover时产生错误。
    同样的，在全局调用group_by_every_record的话，它应该按照record本身来计算hash进行partition，
    而不应该是随便的生成一个key告诉MR框架，以防重算是一条数据两次分给了不同的下游，导致failover时出错。

3. sort_by之后agg需要保证agg里收到的数据的顺序，该如何保证？

    我们的sort_by操作只保证后续的一个操作有序，不保证后续所有操作有序。
    这里为了保证sort_by后agg里的操作有序，只能通过一个trick的方案，
    即在sort_by之后如果调用的是agg，则添加一个什么事儿也不干的NonPartial的Processor
    来强迫结点不前移，保证后续的agg操作都有序。

高级用户教

性能分析
===========

大部分场景下，数据计算时都不会真正出现dict（所有dict结点都被Planner优化删除了），所以并不会影响性能。

另外，select之类的操作，由于选取字段的map函数会被前移，所以，会使得只有需要用到的数据才会过shuffle，
进而提升性能。

但，如果用户真的拿它当作元素为{}的PCollection用，调用map等操作时，确实会有性能损失，但如果是高级用户，
可以轻松的避免写出类似的代码而完成同样的功能。


缺陷与未来可能的优化点
======================

1. 由于缺少一层字段相关planner层，无法完成高级字段优化。
   未来可以添加一层优化层，将操作记录下来，翻译成flume的logical plan。

2. 暂未实现直接可执行SQL语句的功能。
    类似于DataFrame和Spark-SQL的整合，这里也需要bigflow与wing进行整合，才能更好的完成此功能。

3. 可以重新实现一个FieldsDictSerde，让它的Deserialize返回一个对象，该对象重写了[]操作符，
    在用户调用[]操作符时才去进行反序列化对应的列。实现与Spark Dataset类似的效果。
    但缺点依然是要进行hash操作（或者取字段的操作，在python里性能较差）。

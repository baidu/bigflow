###############
Planner编码规范
###############

为了统一Flume项目中Planner部分的编码风格, 增强代码的可读性和可维护性,
这里对Pass/Rule的编写/组织作出约定, 开发者在为Planner编写代码之前, 请务必阅读此规范.
关于Planner的基础知识, 请参见 :doc:`planner`


前言
====

Planner的编程需遵守以下原则.

1.可读性优先
------------

Planner部分的代码基本都是基于图的计算和变换, 代码很容易变得晦涩难懂.
在性能/可读性/可扩展性这三个代码基本评判标准之间, 我们认为可读性>可扩展性>性能.

Planner中的代码往往只在任务提交时运行一遍, 且在一般计算中的算子数量往往有限,
故执行计划的分析和优化绝不会成为整个任务的瓶颈, 因此性能因素在Planner的编码过程中通常可以不予考虑.

原则上, 我们希望一个Pass可以被复用在多个Backend上, 出于这种考虑, 我们可以将某个Pass设计的较为复杂,
适应更广泛的边界条件, 但是这种复杂不应该影响到代码的可读性. 可读性是一切复用的基础.


2.遵循范式
----------

基于图的算法在实现上往往具有较大的自由度, 尤其在C++这样的混合型语言上,
同一种遍历过程都可以有多种写法. 不幸的是, 这种自由度往往会对代码的阅读和理解造成严重干扰.

Flume在Planner中提供了一些机制, 如利用PassManager来管理Pass之间的依赖,
利用Dispatcher/Rule的来规范化遍历过程, 提供机制可以在每个图节点上加标记(Tag),
这些机制存在的主要目的是固化相应的 *编程范式*, 而不在于提供编程方便性.

我们在为Planner编写代码时, **不应该** 试图绕过这些机制, 或是自行实现类似机制. 严格遵循范式,
在牺牲编程自由度的同时, 可以方便代码阅读者把更多的精力关注到图算法上, 这一点尤其重要.


3.代码组织原则
--------------

所有的Planner代码, 要按照入口Pass/内部Pass/Rule的原则来组织(入口/内部Pass参见 :ref:`rule_8`).
Pass/Rule本身应该是无状态的(常量除外), 所有的计算结果(包括中间结果)都应该体现在Plan/Unit上,
这一点和过程式编程语言十分相似. 对应的, 我们可以把入口Pass看作是模块, 把内部Pass看成是方法,
Rule看成是语句.

每个编译单元中有且只有一个入口Pass, 其中应该包含尽量完整的逻辑.  功能相同,
或者逻辑相似但使用范围不同的Pass, 应该尽量被整合到一个Pass文件中.
分散定义且数量很多的Pass文件往往不利于代码阅读和维护, 同时也不利用公共逻辑的抽取和复用.
如果某一Analysis只对某一个Pass产生作用, 建议将它和该Pass合并到同一文件中去.

和Pass相反, 作为'语句'级的编程结构, 每个Rule中包含的逻辑要尽量简单易懂. 对于复杂的Rule,
要利用 :ref:`rule_3`, :ref:`rule_4`, :ref:`rule_5` 中描述的技巧进行拆解.

另外, 我们往往利用在节点打Tag的方式, 在Pass&Rule之间传递信息. 这里要注意的是,
如果某个头文件暴露了过多的公共Tag, 往往说明这个模块和其它模块之间的职责分配不够清晰,
纠缠在一起的多个Pass会增加阅读的难度. 编程的时候, 要尽量设法避免暴露不必要的Tag.


规范
====

.. _rule_0:

0.文档范式
----------

* 约定

    描述信息

* Bad Example
    .. code-block:: cpp

        // Bad Code!

* Good Example
    .. code-block:: cpp

        // Good Code!


.. _rule_1:

1.使用规定的遍历算法
--------------------

* 约定

    flume/planner目录下提供了若干种格式化的遍历方法, 他们包括基于从属关系的先序/中序/后序遍历,
    基于数据流的正/反拓扑序遍历, 基于Task的遍历等. 我们编写的所有算法, 都必须基于这些机制进行遍历.

    '自由'的遍历方式往往造成难以阅读的代码. 另外, 通过一定程度的训练和适应, 一旦接受现有的设定,
    会发现还是可以比较自如的实现各种图算法的.

* Bad Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit == plan->Root();
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                FindUnitsAndDoSomething(unit);
                return true;
            }

            void FindUnitsAndDoSomething(Unit* unit) {  // BAD CODE!
                DoSomething(unit);
                BOOST_FOREACH(unit* child, unit->children()) {
                    FindUnitsAndDoSomething(child);
                }
            }

            void DoSomething(Unit* unit) {}
        };

        RuleDispatcher dispatcher;
        dispatcher.AddRule(new DoSomethingRule);
        dispatcher.Run();

* Good Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoSomething(unit);
            }

            bool DoSomething(Unit* unit) { return true; }
        };

        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::PRE_ORDER);
        dispatcher.AddRule(new DoSomethingRule);
        dispatcher.Run();


.. _rule_2:

2.把完整判断放入Accept函数中
----------------------------

* 约定

    所有的遍历算法都需要实现Accept和Run方法. 尽量把所有的判断逻辑放在Accept中,
    而不是分布在两个函数中.

* Bad Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit->type() == Unit::LOCAL_SHUFFLE;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                if (unit->father() != unit->task()) {  // BAD CODE!
                    return false;
                }
                return DoSomething(unit);
            }

            bool DoSomething(Unit* unit) { return true; }
        };

* Good Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit->type() == Unit::LOCAL_SHUFFLE && unit->father() == unit->task();
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoSomething(unit);
            }

            bool DoSomething(Unit* unit) { return true; }
        };


.. _rule_3:

3.避免在Rule::Run函数中做二次遍历
---------------------------------

* 约定

    尽量利用Accept方法将某个Rule的作用锚定在直接操作的节点上, 而不是锚定在上游/父节点上.

* Bad Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit->type() == Unit::LOCAL_SHUFFLE;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                bool is_changed = false;
                BOOST_FOREACH(unit* child, unit->children()) {  // BAD CODE!
                    is_changed ||= DoSomething(child);
                }
                return is_changed;
            }

            bool DoSomething(Unit* unit) { return true; }
        };

* Good Example
    .. code-block:: cpp

        class DoSomethingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit->father()->type() == Unit::LOCAL_SHUFFLE;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoSomething(unit);
            }

            bool DoSomething(Unit* unit) { return true; }
        };


.. _rule_4:

4.在Rule中只访问周围节点
------------------------

* 约定

    Unit中提供了方法, 能够得到某个节点的父/子/直接上下游节点. 同时,
    如DataFlowAnalysis这样的公共Pass提供了访问所有前继/后继/子孙的方法.

    我们约定, 在相似的实现代价下, 优先采用只访问周围节点的算法. 这样的实现往往更容易理解,
    同时能够减少不必要的外部依赖.

* Bad Example
    .. code-block:: cpp

        struct DesendantCount {
            int value;

            DesendantCount() : value(0) {}
        };

        class CountDesendantRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                DataFlow& dataflow = unit->get<DataFlow>();  // BAD CODE!
                unit->get<DesendantCount>().value = dataflow.nodes.size();
                return true;
            }
        };

* Good Example
    .. code-block:: cpp

        class CountDesendantRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                int& sum = unit->get<DesendantCount>().value;
                BOOST_FOREACH(unit* child, unit->children()) {
                    sum += child->get<DesendantCount>().value;
                }
                return true;
            }
        };

        DepthFirstDispatcher dispatcher(DepthFirstDispatcher::POST_ORDER);
        dispatcher.AddRule(new DoSomethingRule);
        dispatcher.Run();


.. _rule_5:

5.保持一个Rule内的逻辑尽量简单
------------------------------

* 约定

    Rule为Planner代码编写过程中的最小单位. 一个Rule内包含Accept和Run两个方法,
    这两个方法需被视为整体看待.

    类似于一般程序编写中不要定义过长的语句, 我们在Planner中也不要编写职责过于复杂的Rule.
    如果一个Rule中的逻辑过于复杂, 我们要尽量分拆这个Rule.

* Bad Example
    .. code-block:: cpp

        class DoManyThingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                // BAD CODE!
                DoFirstThing();
                DoSecondThing();
                DoLastThing();
                return true;
            }

            bool DoFirstThing();
            bool DoSecondThing();
            bool DoLastThing();
        };

* Good Example
    .. code-block:: cpp

        class DoFirstThingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoFirstThing();
            }

            bool DoFirstThing();
        };

        class DoSecondThingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoSecondThing();
            }
            
            bool DoSecondThing();
        };

        class DoLastThingRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                return DoLastThing();
            }
            
            bool DoLastThing();
        };

        RuleDispatcher dispatcher;
        dispatcher.AddRule(new DoFirstThingRule);
        dispatcher.AddRule(new DoSecondThingRule);
        dispatcher.AddRule(new DoLastThingRule);
        dispatcher.Run();


.. _rule_6:

6.只使用Tag保存状态和中间结果
-----------------------------

* 约定

    有三个地方可以保存计算中间结果: 全局变量, Rule中定义的成员变量, Unit节点上记录的Tag标记.
    我们约定, **只允许** 在Unit上通过Tag保存中间计算结果.

    在全局变量上保存中间结果容易引起并发和扩展性问题. 成员变量上保存的结果不方便在Rule之间分享,
    并且往往会使Rule的设计变得更加复杂. 故作此约定.

* Bad Example
    .. code-block:: cpp

        struct AllNodesCount {
            int value;

            AllNodesCount() : value(0) {}
        };

        class CountAllNodesRule : public RuleDispatcher::Rule {
        public:
            CountAllNodesRule() : m_count(0) {}

            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                if (unit == plan->Root()) {
                    unit->get<AllNodesCount>().value = m_count + 1;
                } else {
                    ++m_count;
                }
                return true;
            }

        private:
            int m_count;  // BAD CODE!
        };

* Good Example
    .. code-block:: cpp

        // By POST_ORDER
        class CountAllNodesRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return true;
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                int count = 1;  // for myself
                BOOST_FOREACH(unit* child, unit->children()) {
                    count += child->get<AllNodesCount>();
                }
                unit->get<AllNodesCount>().value = count;
                return true;
            }
        };


.. _rule_7:

7.不要使用公共类型作为Tag
-------------------------

* 约定

    Tag机制的实现依赖于C++的类型系统, 在Unit上可以为每一种C++类型保存唯一实例.
    如果用公共类型, 如std::map/std::string等作为Tag类型, 则很容易和其它Pass产生冲突/混淆.
    因此不要使用这些类型作为Tag.

    另外, 我们约定各种Proto类型只能由相应的BuildXxxMessagePass使用.

* Bad Example
    .. code-block:: cpp

        typedef std::set<Unit*> NodeSetTag;

* Good Example
    .. code-block:: cpp

        class NodeSetTag : public std::set<Unit*> {};


.. _rule_8:

8.禁止显式调用Pass的Run方法
---------------------------

* 约定

    PassManager中定义了Pass之间的依赖关系, 每个Pass可以在定义的时候声明自己依赖哪些Pass,
    然后通过PassManager中的Apply方法来调用Pass.

    统一的依赖入口, 可以方便我们对代码做调整和拆分, 同时PassManager在调用Pass会记录相应的调试信息,
    方便我们跟踪线上问题. 为了维护代码的统一性, 我们禁止在单测之外直接调用Pass::Run方法.

    一些Pass在遍历过程中会改变拓扑, 从而破坏所依赖的Analysis结果. 遇到这种情形,
    建议实现者多思考一下, 大部分情况下可以通过调整算法规避这些问题. 如果确实无法避免,
    可以将一个Pass拆分成入口Pass和内部Pass.

* Bad Example
    .. code-block:: cpp

        class DoSomethingPass : public Pass {
        public:
            virtual bool Run(Plan* plan) {
                PerformFirstStep(plan);
                DataFlowAnalysis().Run(plan);  // BAD CODE!
                PerformSecondStep(plan);
            }

            void PerformFirstStep(Plan* plan);

            void PerformSecondStep(Plan* plan);
        };

* Good Example
    .. code-block:: cpp

        namespace internal {
            class PerformFirstStepPass : public Pass {
                RELY_PASS(DataFlowAnalysis);
            };

            class PerformSecondStepPass : public Pass {
                RELY_PASS(DataFlowAnalysis);
                RELY_PASS(PerformFirstStepPass);
            };
        }  // namespace internal

        class DoSomethingPass : public Pass {
            RELY_PASS(PerformFirstStepPass);
            RELY_PASS(PerformSecondStepPass);
        };


.. _rule_9:

9.保证Run方法的返回值正确
-------------------------

* 约定

    Rule::Run和Pass::Run的返回值代表Plan的拓扑或者关键信息是否发生改变. 有时程序员为了简单实现,
    会让这两个方法总是返回true. 这种做法容易导致死循环, 并且使得优化中间步骤变多, 影响调试.
    因此我们约定, 实现Pass时一定要保证返回值正确.

* Bad Example
    .. code-block:: cpp

        struct Tag {};

        class SetTagRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit == plan->Root();
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                unit->set<Tag>();
                return true;  // BAD CODE!
            }
        };

* Good Example
    .. code-block:: cpp

        struct Tag {};

        class SetTagRule : public RuleDispatcher::Rule {
        public:
            virtual bool Accept(Plan* plan, Unit* unit) {
                return unit == plan->Root();
            }

            virtual bool Run(Plan* plan, Unit* unit) {
                bool is_modified = !unit->has<Tag>();
                unit->set<Tag>();
                return is_modified;
            }
        };

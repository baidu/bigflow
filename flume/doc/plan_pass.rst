===============================
Planner优化——Analysis与Pass说明
===============================

1. Analysis与Pass调用
---------------------------

    .. image:: static/passes/pass_rely.png
       :width: 1000px


//TODO(Wen Xiang)

2. Analysis与Pass依赖
-----------------------
//TODO(Wen Xiang)

3. Analysis介绍
-------------------
* `DataFlowAnalysis <analysises/data_flow_analysis.html>`_
* `ScopeBasicInfoAnalysis <analysises/scope_basic_info_analysis.html>`_
* `PartialNodeAnalysis <analysises/partial_node_analysis.html>`_
* `PromotableNodeAnalysis <analysises/promotable_node_analysis.html>`_
* `ReduceNumberAnalysis <analysises/reduce_number_analysis.html>`_
* `PreparedNodeAnalysis <analysises/prepared_node_analysis.html>`_
* `TaskIndexAnalysis <analysises/task_index_analysis.html>`_
* `TaskFlowAnalysis <analysises/task_flow_analysis.html>`_
* `VertexAnalysis <analysises/vertex_analysis.html>`_
* `ScopeLevelAnalysis <analysises/scope_level_analysis.html>`_
* `ExecutorDependencyAnalysis <analysises/executor_dependency_analysis.html>`_

4. Pass介绍
--------------
* `LoadLogicalPlanPass <passes/load_logical_plan_pass.html>`_
* `RemoveUnsinkedUnitPass <passes/remove_unsinked_unit_pass.html>`_
* `RemoveUselessUnionPass <passes/remove_useless_union_pass.html>`_
* `RemoveEmptyUnitPass <passes/remove_empty_unit_pass.html>`_
* `AddTaskUnitPass <passes/add_task_unit_pass.html>`_
* `MergeSingleNodePass <passes/merge_single_node_pass.html>`_
* `MergeDistributeShufflePass <passes/merge_distribute_shuffle_pass.html>`_
* `PromoteUnionDownstreamPass <passes/promote_union_downstream_pass.html>`_
* `PromoteGlobalPartialNodePass <passes/promote_global_partial_node_pass.html>`_
* `MergePromoteGlobalPartialPass <passes/merge_promote_global_partial_pass.html>`_
* `PromotePurePartialShufflePass <passes/promote_pure_partial_shuffle_pass.html>`_
* `SplitShufflePass <passes/split_shuffle_pass.html>`_
* `BuildMapInputPass <passes/build_map_input_pass.html>`_
* `BuildShuffleEncoderDecoderPass <passes/build_shuffle_encoder_decoder_pass.html>`_
* `BuildShuffleWriterReaderPass <passes/build_shuffle_writer_reader_pass.html>`_
* `BuildAggregatorPass <passes/build_aggregator_pass.html>`_
* `BuildDummyExecutorPass <passes/build_dummy_executor_pass.html>`_
* `BuildLogicalExecutorPass <passes/build_logical_executor_pass.html>`_
* `BuildPartialExecutorPass <passes/build_partial_executor_pass.html>`_
* `BuildLocalShuffleExecutorPass <passes/build_local_shuffle_executor_pass.html>`_
* `BuildPartialShuffleExecutorPass <passes/build_partial_shuffle_executor_pass.html>`_
* `BuildMergeShuffleExecutorPass <passes/build_merge_shuffle_executor_pass.html>`_
* `BuildTaskExecutorPass <passes/build_task_executor_pass.html>`_
* `BuildPhysicalPlanPass <passes/build_physical_plan_pass.html>`_
* `MergeTaskPass <passes/merge_task_pass.html>`_


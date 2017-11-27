ADD_SUBDIRECTORY("proto")

cc_library(flume-flume SRCS ../flume/flume.cpp DEPS toft flume-runtime-counter flume-runtime-backend)

cc_library(flume-flume_flags SRCS ../flume/flags.cpp DEPS glog gflags boost)

#cc_library(flume-service-commmon-profiler_util SRCS ../flume/service/common/profiler_util.cpp DEPS glog gflags toft perftools)

cc_library(flume-util-util SRCS  DEPS flume-util-arena)

cc_library(flume-util-arena SRCS ../flume/util/arena.cpp DEPS toft)

cc_test(flume-util-arena_test SRCS ../flume/util/arena_test.cpp DEPS gmock glog gflags boost flume-util-arena)

cc_library(flume-util-bitset SRCS ../flume/util/bitset.cpp DEPS glog gflags boost)

cc_test(flume-util-bitset_test SRCS ../flume/util/bitset_test.cpp DEPS gmock glog gflags flume-util-bitset)

cc_test(flume-util-type_guard_test SRCS ../flume/util/type_guard_test.cpp DEPS gmock glog gflags boost boost)

cc_library(flume-util-path_util SRCS ../flume/util/path_util.cpp DEPS toft flume-util-hadoop_conf)

cc_test(flume-util-path_util_test SRCS ../flume/util/path_util_test.cpp DEPS gmock glog gflags boost flume-util-path_util)

cc_library(flume-util-config_util SRCS ../flume/util/config_util.cpp DEPS toft glog gflags boost)

cc_library(flume-util-hadoop_conf SRCS ../flume/util/hadoop_conf.cpp DEPS tinyxml glog gflags boost)

cc_test(flume-util-hadoop_conf_test SRCS ../flume/util/hadoop_conf_test.cpp DEPS gmock glog gflags flume-util-hadoop_conf)

cc_test(flume-util-compiler_test SRCS ../flume/util/compiler_test.cpp DEPS gmock glog gflags boost)

cc_library(flume-util-reflection SRCS ../flume/util/reflection.cpp DEPS glog gflags boost LINK_ALL_SYMBOLS "true")

cc_library(flume-util-reflection_test_helper SRCS ../flume/util/reflection_test_helper.cpp DEPS flume-util-reflection)

cc_library(flume-util-reflection_dlopen_test_helper SRCS ../flume/util/reflection_dlopen_test_helper.cpp DEPS flume-util-reflection)

cc_test(flume-util-reflection_test SRCS ../flume/util/reflection_test.cpp DEPS gmock glog gflags flume-util-reflection_test_helper)

cc_library(flume-util-process_launcher SRCS ../flume/util/process_launcher.cpp DEPS toft)

cc_library(flume-util-jni_environment SRCS ../flume/util/jni_environment.cpp DEPS toft)

cc_test(flume-util-cached_allocator_test SRCS ../flume/util/cached_allocator_test.cpp DEPS gmock glog gflags boost)

cc_test(flume-util-intrusive_ptr_base_test SRCS ../flume/util/intrusive_ptr_base_test.cpp DEPS gmock glog gflags boost)

cc_test(flume-util-reusable_object_pool_test SRCS ../flume/util/reusable_object_pool_test.cpp DEPS gmock glog gflags boost)

cc_test(flume-util-config_util_test SRCS ../flume/util/config_util_test.cpp DEPS toft gmock glog gflags boost flume-util-config_util)

cc_library(flume-core-entity SRCS ../flume/core/entity.cpp DEPS glog gflags flume-util-reflection flume-proto-entity)

cc_test(flume-core-entity_test SRCS ../flume/core/entity_test.cpp DEPS gmock glog gflags flume-core-entity)

cc_library(flume-core-loader SRCS ../flume/core/loader.cpp DEPS flume-proto-split)

cc_test(flume-core-loader_test SRCS ../flume/core/loader_test.cpp DEPS gmock glog gflags flume-proto-custom_split flume-core-loader)

cc_library(flume-core-logical_plan SRCS ../flume/core/logical_plan.cpp DEPS toft flume-util-config_util flume-runtime-task flume-runtime-session flume-runtime-local-local_executor_factory flume-runtime-local-local_backend flume-runtime-counter flume-runtime-common-memory_dataset flume-proto-logical_plan flume-proto-config flume-planner-local-local_planner flume-flume_flags flume-core-entity boost)

cc_test(flume-core-logical_plan_test SRCS ../flume/core/logical_plan_test.cpp DEPS gmock glog gflags flume-core-logical_plan)

cc_library(flume-core-empty_environment SRCS ../flume/core/empty_environment.cpp DEPS glog gflags boost)

cc_library(flume-core-timer_queue SRCS ../flume/core/timer_queue.cpp DEPS glog gflags boost)

cc_test(flume-core-testing-mock_emitter_test SRCS ../flume/core/testing/mock_emitter_test.cpp DEPS gmock glog gflags boost)

cc_library(flume-core-testing-processor_test_helper SRCS ../flume/core/testing/processor_test_helper.cpp DEPS flume-core-entity)

cc_test(flume-core-testing-processor_test_helper_test SRCS ../flume/core/testing/processor_test_helper_test.cpp DEPS toft gmock glog gflags flume-core-testing-processor_test_helper)

cc_test(flume-core-testing-mock_objector_test SRCS ../flume/core/testing/mock_objector_test.cpp DEPS gmock glog gflags flume-core-entity)

cc_test(flume-core-testing-mock_key_reader_test SRCS ../flume/core/testing/mock_key_reader_test.cpp DEPS gmock glog gflags flume-core-entity)

cc_test(flume-core-testing-mock_partitioner_test SRCS ../flume/core/testing/mock_partitioner_test.cpp DEPS gmock glog gflags flume-core-entity)

cc_test(flume-core-testing-mock_loader_test SRCS ../flume/core/testing/mock_loader_test.cpp DEPS gmock glog gflags flume-core-entity)

cc_test(flume-core-testing-mock_sinker_test SRCS ../flume/core/testing/mock_sinker_test.cpp DEPS toft gmock glog gflags flume-core-entity)

cc_test(flume-core-testing-mock_processor_test SRCS ../flume/core/testing/mock_processor_test.cpp DEPS toft gmock glog gflags flume-core-entity)

cc_library(flume-planner-plan SRCS ../flume/planner/plan.cpp ../flume/planner/unit.cpp DEPS toft flume-proto-physical_plan flume-proto-logical_plan)

cc_test(flume-planner-plan_test SRCS ../flume/planner/plan_test.cpp DEPS gmock glog gflags flume-planner-plan flume-core-logical_plan flume-core-entity)

cc_test(flume-planner-unit_test SRCS ../flume/planner/unit_test.cpp DEPS gmock glog gflags flume-planner-plan flume-core-logical_plan flume-core-entity)

cc_library(flume-planner-pass_manager SRCS ../flume/planner/pass_manager.cpp DEPS flume-util-reflection flume-planner-plan flume-planner-common-draw_plan_pass)

cc_test(flume-planner-pass_manager_test SRCS ../flume/planner/pass_manager_test.cpp DEPS gmock glog gflags flume-planner-pass_manager)

cc_library(flume-planner-rule_dispatcher SRCS ../flume/planner/rule_dispatcher.cpp DEPS flume-planner-plan)

cc_library(flume-planner-depth_first_dispatcher SRCS ../flume/planner/depth_first_dispatcher.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan)

cc_library(flume-planner-topological_dispatcher SRCS ../flume/planner/topological_dispatcher.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan)

cc_library(flume-planner-first_layer_dispatcher SRCS ../flume/planner/first_layer_dispatcher.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan)

cc_library(flume-planner-task_dispatcher SRCS ../flume/planner/task_dispatcher.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan)

cc_library(flume-planner-spark-add_distribute_by_default_pass SRCS ../flume/planner/spark/add_distribute_by_default_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-remove_useless_union_pass flume-planner-common-remove_unsinked_pass flume-flume_flags)

cc_library(flume-planner-spark-add_task_unit_pass SRCS ../flume/planner/spark/add_task_unit_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-remove_useless_union_pass flume-planner-common-remove_unsinked_pass flume-flume_flags)

cc_library(flume-planner-spark-add_transfer_executor_pass SRCS ../flume/planner/spark/add_transfer_executor_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager)

cc_library(flume-planner-spark-build_physical_plan_pass SRCS ../flume/planner/spark/build_physical_plan_pass.cpp DEPS flume-proto-physical_plan flume-proto-logical_plan flume-planner-spark-build_task_executor_pass flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager)

cc_library(flume-planner-spark-build_task_executor_pass SRCS ../flume/planner/spark/build_task_executor_pass.cpp DEPS flume-proto-physical_plan flume-planner-topological_dispatcher flume-planner-spark-build_transfer_executor_pass flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-scope_level_analysis flume-planner-common-draw_plan_pass flume-planner-common-build_executor_message_pass flume-planner-common-build_common_executor_pass)

cc_library(flume-planner-spark-build_transfer_executor_pass SRCS ../flume/planner/spark/build_transfer_executor_pass.cpp DEPS flume-proto-transfer_encoding flume-proto-physical_plan flume-planner-spark-init_rdd_info_pass flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-build_common_executor_pass)

cc_library(flume-planner-spark-init_rdd_info_pass SRCS ../flume/planner/spark/init_rdd_info_pass.cpp DEPS flume-proto-physical_plan flume-planner-testing-plan_draw_helper flume-planner-task_dispatcher flume-planner-spark-task_stage_analysis flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher flume-planner-common-task_flow_analysis flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis flume-flume_flags)

cc_library(flume-planner-spark-spark_planner SRCS ../flume/planner/spark/spark_planner.cpp DEPS flume-runtime-session flume-runtime-resource flume-proto-physical_plan flume-proto-logical_plan flume-planner-spark-set_default_concurrency_pass flume-planner-spark-merge_task_pass flume-planner-spark-build_transfer_executor_pass flume-planner-spark-build_physical_plan_pass flume-planner-spark-add_transfer_executor_pass flume-planner-spark-add_task_unit_pass flume-planner-spark-add_distribute_by_default_pass flume-planner-pass_manager flume-planner-common-split_union_unit_pass flume-planner-common-side_effect_analysis flume-planner-common-scope_analysis flume-planner-common-remove_useless_union_pass flume-planner-common-remove_unsinked_pass flume-planner-common-remove_empty_unit_pass flume-planner-spark-prune_cached_path_pass flume-planner-common-promote_partial_process_pass flume-planner-common-load_logical_plan_pass flume-planner-common-cache_analysis flume-planner-common-build_common_executor_pass flume-planner-common-broadcast_shuffle_node_under_scope_analysis flume-planner-common-add_common_executor_pass flume-planner-spark-add_cache_task_pass)

cc_library(flume-planner-spark-merge_task_pass SRCS ../flume/planner/spark/merge_task_pass.cpp DEPS flume-planner-topological_dispatcher flume-planner-task_dispatcher flume-planner-spark-task_stage_analysis flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher flume-planner-common-remove_empty_unit_pass flume-planner-common-promote_partial_units_pass flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_library(flume-planner-spark-set_default_concurrency_pass SRCS ../flume/planner/spark/set_default_concurrency_pass.cpp DEPS flume-planner-task_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher flume-planner-common-data_flow_analysis)

cc_library(flume-planner-spark-task_stage_analysis SRCS ../flume/planner/spark/task_stage_analysis.cpp DEPS flume-planner-task_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-task_flow_analysis flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis flume-flume_flags)

cc_library(flume-planner-testing-plan_draw_helper SRCS ../flume/planner/testing/plan_draw_helper.cpp DEPS toft flume-planner-topological_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher boost)

cc_test(flume-planner-testing-plan_draw_helper_test SRCS ../flume/planner/testing/plan_draw_helper_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-testing-plan_draw_helper)

cc_library(flume-planner-testing-plan_test_helper SRCS ../flume/planner/testing/plan_test_helper.cpp DEPS toft flume-util-reflection flume-planner-testing-plan_draw_helper flume-planner-plan flume-planner-common-load_logical_plan_pass flume-planner-common-data_flow_analysis flume-core-logical_plan boost)

cc_test(flume-planner-testing-plan_test_helper_test SRCS ../flume/planner/testing/plan_test_helper_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-data_flow_analysis flume-core-logical_plan)

cc_library(flume-planner-testing-pass_test_helper SRCS ../flume/planner/testing/edge_desc.cpp ../flume/planner/testing/tag_desc.cpp ../flume/planner/testing/plan_desc.cpp ../flume/planner/testing/pass_test_helper.cpp DEPS toft flume-planner-topological_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-draw_plan_pass flume-core-logical_plan boost)

cc_test(flume-planner-testing-pass_test_helper_test SRCS ../flume/planner/testing/pass_test_helper_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-data_flow_analysis)

cc_library(flume-planner-monitor-monitor_planner SRCS ../flume/planner/monitor/monitor_planner.cpp DEPS flume-runtime-resource flume-proto-logical_plan flume-planner-plan flume-planner-pass_manager flume-planner-monitor-build_reader_writer_pass flume-planner-monitor-build_physical_plan_pass flume-planner-monitor-build_log_input_pass flume-planner-monitor-add_fixed_task_unit_pass flume-planner-monitor-add_channel_unit_pass flume-planner-first_layer_dispatcher flume-planner-depth_first_dispatcher flume-planner-common-load_logical_plan_pass flume-planner-common-data_flow_analysis flume-core-logical_plan)

cc_test(flume-planner-monitor-monitor_planner_test SRCS ../flume/planner/monitor/monitor_planner_test.cpp DEPS toft gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-monitor-monitor_planner flume-core-logical_plan flume-core-entity)

cc_library(flume-planner-monitor-add_fixed_task_unit_pass SRCS ../flume/planner/monitor/add_fixed_task_unit_pass.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-monitor-prepared_node_basic_analysis flume-planner-depth_first_dispatcher flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_test(flume-planner-monitor-add_fixed_task_unit_pass_test SRCS ../flume/planner/monitor/add_fixed_task_unit_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-monitor-add_fixed_task_unit_pass)

cc_library(flume-planner-monitor-add_channel_unit_pass SRCS ../flume/planner/monitor/add_channel_unit_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_library(flume-planner-monitor-build_log_input_pass SRCS ../flume/planner/monitor/build_log_input_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_test(flume-planner-monitor-build_log_input_pass_test SRCS ../flume/planner/monitor/build_log_input_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-monitor-build_log_input_pass)

cc_library(flume-planner-monitor-build_reader_writer_pass SRCS ../flume/planner/monitor/build_reader_writer_pass.cpp DEPS flume-planner-task_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-monitor-prepared_node_basic_analysis flume-planner-depth_first_dispatcher flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_test(flume-planner-monitor-build_reader_writer_pass_test SRCS ../flume/planner/monitor/build_reader_writer_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-monitor-build_reader_writer_pass)

cc_library(flume-planner-monitor-build_physical_plan_pass SRCS ../flume/planner/monitor/build_physical_plan_pass.cpp DEPS flume-planner-task_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-monitor-build_task_executor_pass flume-planner-depth_first_dispatcher)

cc_library(flume-planner-monitor-build_task_executor_pass SRCS ../flume/planner/monitor/build_task_executor_pass.cpp DEPS flume-proto-physical_plan flume-planner-topological_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-scope_level_analysis flume-planner-common-draw_plan_pass flume-planner-common-build_executor_message_pass flume-planner-common-build_common_executor_pass)

cc_library(flume-planner-monitor-prepared_node_basic_analysis SRCS ../flume/planner/monitor/prepared_node_basic_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-common-draw_plan_pass)

cc_library(flume-planner-monitor-prepared_node_analysis SRCS ../flume/planner/monitor/prepared_node_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-monitor-prepared_node_basic_analysis flume-planner-common-draw_plan_pass)

cc_test(flume-planner-monitor-prepared_node_analysis_test SRCS ../flume/planner/monitor/prepared_node_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-monitor-prepared_node_analysis)

cc_library(flume-planner-local-build_local_input_executor_pass SRCS ../flume/planner/local/build_local_input_executor_pass.cpp DEPS flume-proto-physical_plan flume-proto-logical_plan flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-cache_util)

cc_library(flume-planner-local-build_physical_plan_pass SRCS ../flume/planner/local/build_physical_plan_pass.cpp DEPS flume-proto-physical_plan flume-proto-logical_plan flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-local-build_task_executor_pass)

cc_library(flume-planner-local-build_task_executor_pass SRCS ../flume/planner/local/build_task_executor_pass.cpp DEPS flume-proto-physical_plan flume-planner-topological_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-scope_level_analysis flume-planner-common-draw_plan_pass flume-planner-common-build_executor_message_pass flume-planner-common-build_common_executor_pass)

cc_library(flume-planner-local-local_planner SRCS ../flume/planner/local/local_planner.cpp DEPS flume-runtime-session flume-runtime-resource flume-proto-physical_plan flume-proto-logical_plan flume-planner-pass_manager flume-planner-local-build_physical_plan_pass flume-planner-local-build_local_input_executor_pass flume-planner-common-split_union_unit_pass flume-planner-common-side_effect_analysis flume-planner-common-scope_analysis flume-planner-common-remove_useless_union_pass flume-planner-common-remove_unsinked_pass flume-planner-common-remove_empty_unit_pass flume-planner-common-prune_cached_path_pass flume-planner-common-promote_partial_process_pass flume-planner-common-plan_analysis flume-planner-common-load_logical_plan_pass flume-planner-common-cache_analysis flume-planner-common-build_common_executor_pass flume-planner-common-add_common_executor_pass)

cc_test(flume-planner-local-local_planner_test SRCS ../flume/planner/local/local_planner_test.cpp DEPS gmock glog gflags flume-runtime-local-local_backend flume-planner-local-local_planner flume-core-logical_plan flume-core-entity)

cc_library(flume-planner-common-draw_plan_pass SRCS ../flume/planner/common/draw_plan_pass.cpp DEPS toft flume-planner-topological_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher)

cc_test(flume-planner-common-draw_plan_pass_test SRCS ../flume/planner/common/draw_plan_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-draw_plan_pass flume-core-entity)

cc_library(flume-planner-common-load_logical_plan_pass SRCS ../flume/planner/common/load_logical_plan_pass.cpp DEPS flume-planner-plan flume-planner-common-draw_plan_pass flume-core-empty_environment)

cc_test(flume-planner-common-load_logical_plan_pass_test SRCS ../flume/planner/common/load_logical_plan_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-load_logical_plan_pass flume-planner-common-data_flow_analysis flume-core-logical_plan flume-core-entity)

cc_library(flume-planner-common-remove_unsinked_pass SRCS ../flume/planner/common/remove_unsinked_pass.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-common-group_generator_analysis)

cc_test(flume-planner-common-remove_unsinked_pass_test SRCS ../flume/planner/common/remove_unsinked_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-remove_unsinked_pass flume-core-logical_plan)

cc_library(flume-planner-common-remove_useless_union_pass SRCS ../flume/planner/common/remove_useless_union_pass.cpp DEPS flume-planner-plan)

cc_test(flume-planner-common-task_flow_analysis_test SRCS ../flume/planner/common/task_flow_analysis_test.cpp DEPS gmock glog gflags flume-planner-common-task_flow_analysis)

cc_library(flume-planner-common-task_flow_analysis SRCS ../flume/planner/common/task_flow_analysis.cpp DEPS flume-planner-common-data_flow_analysis)

cc_library(flume-planner-common-session_analysis SRCS ../flume/planner/common/session_analysis.cpp DEPS flume-runtime-session flume-proto-logical_plan)

cc_library(flume-planner-common-cache_analysis SRCS ../flume/planner/common/cache_analysis.cpp DEPS flume-planner-common-session_analysis flume-planner-common-draw_plan_pass)

cc_library(flume-planner-common-group_generator_analysis SRCS ../flume/planner/common/group_generator_analysis.cpp DEPS flume-planner-common-session_analysis flume-planner-common-draw_plan_pass)

cc_test(flume-planner-common-session_analysis_test SRCS ../flume/planner/common/session_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-session_analysis flume-planner-common-data_flow_analysis flume-core-logical_plan flume-core-entity)

cc_library(flume-planner-common-check_cycle_pass SRCS ../flume/planner/common/check_cycle_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-data_flow_analysis)

cc_test(flume-planner-common-check_cycle_pass_test SRCS ../flume/planner/common/check_cycle_pass_test.cpp DEPS gmock glog gflags flume-planner-common-check_cycle_pass)

cc_library(flume-planner-common-data_flow_analysis SRCS ../flume/planner/common/data_flow_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_library(flume-planner-common-broadcast_shuffle_node_under_scope_analysis SRCS ../flume/planner/common/broadcast_shuffle_node_under_scope_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_test(flume-planner-common-data_flow_analysis_test SRCS ../flume/planner/common/data_flow_analysis_test.cpp DEPS gmock glog gflags flume-planner-common-data_flow_analysis)

cc_test(flume-planner-common-remove_useless_union_pass_test SRCS ../flume/planner/common/remove_useless_union_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-remove_useless_union_pass flume-core-logical_plan)

cc_library(flume-planner-common-remove_empty_unit_pass SRCS ../flume/planner/common/remove_empty_unit_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_test(flume-planner-common-remove_empty_unit_pass_test SRCS ../flume/planner/common/remove_empty_unit_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-plan_test_helper flume-planner-common-remove_unsinked_pass flume-planner-common-remove_empty_unit_pass flume-core-logical_plan)

cc_library(flume-planner-common-add_common_executor_pass SRCS ../flume/planner/common/add_common_executor_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-remove_unsinked_pass flume-planner-common-remove_empty_unit_pass flume-planner-common-draw_plan_pass)

cc_test(flume-planner-common-add_common_executor_pass_test SRCS ../flume/planner/common/add_common_executor_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-add_common_executor_pass)

cc_library(flume-planner-common-build_common_executor_pass SRCS ../flume/planner/common/build_common_executor_pass.cpp DEPS flume-proto-transfer_encoding flume-proto-physical_plan flume-planner-rule_dispatcher flume-planner-plan flume-planner-depth_first_dispatcher flume-planner-common-prepared_analysis flume-planner-common-draw_plan_pass)

cc_test(flume-planner-common-build_common_executor_pass_test SRCS ../flume/planner/common/build_common_executor_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-build_common_executor_pass)

cc_library(flume-planner-common-scope_analysis SRCS ../flume/planner/common/scope_analysis.cpp DEPS flume-planner-plan flume-planner-depth_first_dispatcher)

cc_test(flume-planner-common-scope_analysis_test SRCS ../flume/planner/common/scope_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-scope_analysis)

cc_library(flume-planner-common-scope_level_analysis SRCS ../flume/planner/common/scope_level_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-rule_dispatcher flume-planner-plan flume-planner-common-draw_plan_pass)

cc_library(flume-planner-common-executor_dependency_analysis SRCS ../flume/planner/common/executor_dependency_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-rule_dispatcher flume-planner-plan flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_test(flume-planner-common-executor_dependency_analysis_test SRCS ../flume/planner/common/executor_dependency_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-executor_dependency_analysis)

cc_library(flume-planner-common-build_executor_message_pass SRCS ../flume/planner/common/build_executor_message_pass.cpp DEPS flume-planner-topological_dispatcher flume-planner-rule_dispatcher flume-planner-plan flume-planner-common-prepared_analysis flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_test(flume-planner-common-build_executor_message_pass_test SRCS ../flume/planner/common/build_executor_message_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-build_executor_message_pass)

cc_library(flume-planner-common-prepared_analysis SRCS ../flume/planner/common/prepared_analysis.cpp DEPS flume-planner-topological_dispatcher flume-planner-plan flume-planner-common-executor_dependency_analysis flume-planner-common-draw_plan_pass flume-planner-common-data_flow_analysis)

cc_test(flume-planner-common-prepared_analysis_test SRCS ../flume/planner/common/prepared_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-prepared_analysis)

cc_library(flume-planner-common-promote_partial_process_pass SRCS ../flume/planner/common/promote_partial_process_pass.cpp DEPS flume-planner-rule_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-depth_first_dispatcher)

cc_test(flume-planner-common-promote_partial_process_pass_test SRCS ../flume/planner/common/promote_partial_process_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-promote_partial_process_pass)

cc_library(flume-planner-common-promote_partial_units_pass SRCS ../flume/planner/common/promote_partial_units_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager flume-planner-common-remove_useless_union_pass flume-planner-common-remove_unsinked_pass)

cc_test(flume-planner-common-promote_partial_units_pass_test SRCS ../flume/planner/common/promote_partial_units_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-promote_partial_units_pass)

cc_library(flume-planner-common-cache_util SRCS ../flume/planner/common/cache_util.cpp DEPS flume-runtime-io-io_format flume-core-loader flume-core-entity)

cc_test(flume-planner-common-cache_util_test SRCS ../flume/planner/common/cache_util_test.cpp DEPS gmock glog gflags flume-planner-common-cache_util)

cc_test(flume-planner-common-prune_cached_path_pass_test SRCS ../flume/planner/common/prune_cached_path_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-prune_cached_path_pass)

cc_library(flume-planner-common-prune_cached_path_pass SRCS ../flume/planner/common/prune_cached_path_pass.cpp DEPS flume-runtime-io-io_format flume-planner-plan flume-planner-pass_manager flume-planner-common-cache_util)

cc_library(flume-planner-spark-prune_cached_path_pass SRCS ../flume/planner/spark/prune_cached_path_pass.cpp DEPS flume-planner-common-scope_analysis flume-planner-depth_first_dispatcher flume-planner-plan flume-planner-pass_manager flume-planner-topological_dispatcher flume-planner-task_dispatcher flume-planner-common-draw_plan_pass flume-planner-common-remove_empty_unit_pass)

cc_library(flume-planner-common-split_union_unit_pass SRCS ../flume/planner/common/split_union_unit_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager)

cc_test(flume-planner-common-split_union_unit_pass_test SRCS ../flume/planner/common/split_union_unit_pass_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-split_union_unit_pass)

cc_library(flume-planner-common-side_effect_analysis SRCS ../flume/planner/common/side_effect_analysis.cpp DEPS flume-planner-common-cache_analysis)

cc_test(flume-planner-common-side_effect_analysis_test SRCS ../flume/planner/common/side_effect_analysis_test.cpp DEPS gmock glog gflags flume-planner-testing-pass_test_helper flume-planner-common-side_effect_analysis)

cc_library(flume-planner-common-plan_analysis SRCS ../flume/planner/common/plan_analysis.cpp DEPS flume-proto-physical_plan)

cc_library(flume-runtime-task SRCS ../flume/runtime/task.cpp DEPS flume-proto-physical_plan boost)

cc_library(flume-runtime-executor_factory SRCS ../flume/runtime/executor_factory.cpp DEPS flume-runtime-common-union_executor flume-runtime-common-task_executor flume-runtime-common-sinker_executor flume-runtime-common-shuffle_executor flume-runtime-common-processor_executor flume-runtime-common-partial_executor flume-runtime-common-loader_executor flume-runtime-backend boost)

cc_test(flume-runtime-executor_factory_test SRCS ../flume/runtime/executor_factory_test.cpp DEPS gmock glog gflags flume-runtime-executor_factory flume-runtime-common-memory_dataset flume-proto-logical_plan)

cc_library(flume-runtime-status_table SRCS ../flume/runtime/status_table.cpp DEPS glog gflags boost)

cc_library(flume-runtime-resource SRCS ../flume/runtime/resource.cpp DEPS toft boost)

cc_test(flume-runtime-resource_test SRCS ../flume/runtime/resource_test.cpp DEPS gmock glog gflags flume-runtime-resource)

cc_library(flume-runtime-counter SRCS ../flume/runtime/counter.cpp DEPS toft)

cc_test(flume-runtime-counter_test SRCS ../flume/runtime/counter_test.cpp DEPS gmock glog gflags flume-runtime-counter)

cc_library(flume-runtime-backend SRCS ../flume/runtime/backend.cpp DEPS toft flume-core-loader flume-runtime-resource flume-runtime-common-cache_manager flume-runtime-common-cache_iterator flume-proto-physical_plan flume-core-entity)

cc_library(flume-runtime-session SRCS ../flume/runtime/session.cpp DEPS flume-proto-logical_plan boost)

cc_test(flume-runtime-session_test SRCS ../flume/runtime/session_test.cpp DEPS gmock glog gflags flume-runtime-session)

cc_library(flume-runtime-spark-spark_job SRCS ../flume/runtime/spark/spark_job.cpp DEPS re2 flume-util-jni_environment flume-runtime-backend flume-proto-physical_plan flume-proto-logical_plan flume-planner-spark-spark_planner)

cc_library(flume-runtime-spark-spark_cache_manager SRCS ../flume/runtime/spark/spark_cache_manager.cpp DEPS flume-util-jni_environment flume-runtime-spark-kv_buffer flume-runtime-common-cache_manager)

cc_library(flume-runtime-spark-spark_context SRCS ../flume/runtime/spark/spark_context.cpp DEPS flume-util-jni_environment flume-proto-physical_plan flume-proto-config)

cc_library(flume-runtime-spark-cache_input_executor SRCS ../flume/runtime/spark/cache_input_executor.cpp DEPS flume-proto-physical_plan flume-runtime-common-general_dispatcher flume-planner-common-cache_util)

cc_library(flume-runtime-spark-spark_task SRCS ../flume/runtime/spark/spark_task.cpp DEPS flume-runtime-task flume-runtime-spark-spark_executor_factory flume-runtime-spark-spark_cache_manager flume-runtime-spark-kv_buffer flume-runtime-spark-hadoop_input_executor flume-runtime-resource flume-runtime-counter flume-runtime-common-local_dataset)

cc_library(flume-runtime-spark-kv_buffer SRCS ../flume/runtime/spark/kv_buffer.cpp DEPS toft)

cc_test(flume-runtime-spark-kv_buffer_test SRCS ../flume/runtime/spark/kv_buffer_test.cpp DEPS gmock glog gflags flume-runtime-spark-kv_buffer)

cc_library(flume-runtime-spark-flags SRCS ../flume/runtime/spark/flags.cpp DEPS gflags boost)

cc_library(flume-runtime-spark-hadoop_input_executor SRCS ../flume/runtime/spark/hadoop_input_executor.cpp DEPS flume-runtime-io-io_format flume-runtime-common-sub_executor_manager flume-runtime-common-general_dispatcher flume-proto-physical_plan)

cc_library(flume-runtime-spark-shuffle_input_executor SRCS ../flume/runtime/spark/shuffle_input_executor.cpp DEPS toft flume-runtime-common-transfer_encoding flume-runtime-common-executor_impl flume-proto-physical_plan)

cc_library(flume-runtime-spark-shuffle_output_executor SRCS ../flume/runtime/spark/shuffle_output_executor.cpp DEPS toft flume-runtime-common-transfer_encoding flume-runtime-common-executor_impl flume-proto-physical_plan)

cc_library(flume-runtime-spark-spark_executor_factory SRCS ../flume/runtime/spark/spark_executor_factory.cpp DEPS toft flume-runtime-spark-cache_input_executor flume-runtime-spark-shuffle_output_executor flume-runtime-spark-shuffle_input_executor flume-runtime-spark-hadoop_input_executor flume-runtime-executor_factory flume-runtime-common-write_cache_executor flume-runtime-common-shuffle_executor flume-runtime-common-fix_empty_group_executor flume-proto-physical_plan)

cc_library(flume-runtime-spark-spark_cache_iterator SRCS ../flume/runtime/spark/spark_cache_iterator.cpp DEPS boost)

cc_library(flume-runtime-spark-spark_driver SRCS ../flume/runtime/spark/spark_driver.cpp DEPS toft protobuf gflags re2 glog gflags thrift flume-runtime-spark-spark_cache_iterator flume-planner-spark-spark_planner flume-proto-logical_plan flume-proto-physical_plan flume-thrift-backend_service_thrift flume-runtime-backend)

cc_library(flume-runtime-spark-spark_backend LINK_ALL_SYMBOLS "true" SRCS ../flume/runtime/spark/spark_backend.cpp DEPS toft protobuf gflags re2 glog gflags flume-util-path_util flume-util-config_util flume-runtime-task flume-runtime-spark-spark_task flume-runtime-spark-spark_launcher_jar flume-runtime-spark-spark_job flume-runtime-spark-spark_context flume-runtime-session flume-runtime-resource flume-runtime-counter flume-runtime-common-local_dataset flume-runtime-backend flume-proto-physical_plan flume-proto-logical_plan flume-proto-config flume-planner-spark-spark_planner flume-runtime-spark-spark_driver flume-runtime-spark-profile_scripts ctemplate)

cc_library(flume-runtime-spark-jni-jni_buffer LINK_ALL_SYMBOLS "true" SRCS ../flume/runtime/spark/jni/jni_buffer.cpp DEPS flume-util-jni_environment flume-runtime-spark-kv_buffer)

cc_library(flume-runtime-spark-jni_util SRCS ../flume/runtime/spark/jni_util/util.cpp ../flume/runtime/spark/jni_util/java_exception_thrower.cpp)

cc_library(flume-runtime-spark-jni-jni_task LINK_ALL_SYMBOLS "true" SRCS ../flume/runtime/spark/jni/jni_task.cpp DEPS flume-util-jni_environment flume-runtime-spark-spark_task flume-runtime-spark-jni-jni_buffer flume-runtime-spark-jni_util)

cc_library(flume-runtime-spark-jni-flume_partitioner SRCS ../flume/runtime/spark/jni/flume_partitioner.cpp DEPS flume-util-jni_environment)

cc_library(flume-runtime-spark-jni-flume_filter_before_repartition SRCS ../flume/runtime/spark/jni/flume_filter_before_repartition.cpp DEPS flume-util-jni_environment toft)

cc_library(flume-runtime-spark-jni-jni SRCS  DEPS flume-runtime-spark-jni-jni_task flume-runtime-spark-jni-jni_buffer flume-runtime-spark-jni-flume_partitioner flume-runtime-spark-jni-flume_filter_before_repartition)

cc_library(flume-runtime-io-io_format SRCS ../flume/runtime/io/io_format.cpp DEPS toft flume-util-path_util flume-runtime-io-compression_file_factory flume-runtime-counter flume-core-entity)
cc_test(flume-runtime-io-io_format_test SRCS ../flume/runtime/io/io_format_test.cpp DEPS toft gmock glog gflags flume-runtime-io-io_format)

cc_library(flume-runtime-io-compression_file_factory SRCS ../flume/runtime/io/compression_file_factory.cpp DEPS flume-runtime-io-gzip_file)

cc_library(flume-runtime-io-gzip_file SRCS ../flume/runtime/io/gzip_file.cpp DEPS toft toft_other LINK_ALL_SYMBOLS "true")

cc_test(flume-runtime-io-gzip_file_test SRCS ../flume/runtime/io/gzip_file_test.cpp DEPS toft gmock glog gflags flume-runtime-io-compression_file_factory boost)

#cc_library(flume-runtime-io-parquet_io_format SRCS ../flume/runtime/io/parquet_io_format.cpp ../flume/runtime/io/toft_arrow_file.cpp ../flume/runtime/io/arrow_record_batch_io.cpp DEPS toft bigflow-third-party-parquet-cpp-parquet-cpp flume-util-path_util flume-runtime-io-io_format)

#cc_test(flume-runtime-io-parquet_io_format_test SRCS ../flume/runtime/io/parquet_io_format_test.cpp DEPS gmock glog gflags flume-runtime-io-parquet_io_format)

#cc_test(flume-runtime-io-arrow_record_batch_io_test SRCS ../flume/runtime/io/arrow_record_batch_io_test.cpp DEPS svn-baidu-bigflow-third-party-parquet-cpp-parquet-cpp svn-baidu-bigflow-third-party-arrow-arrow gmock glog gflags flume-runtime-io-parquet_io_format)

cc_library(flume-runtime-local-local_input_executor SRCS ../flume/runtime/local/local_input_executor.cpp DEPS toft flume-util-reflection flume-runtime-common-sub_executor_manager flume-core-entity)

cc_library(flume-runtime-local-flags SRCS ../flume/runtime/local/flags.cpp DEPS gflags)

cc_test(flume-runtime-local-local_input_executor_test SRCS ../flume/runtime/local/local_input_executor_test.cpp DEPS gmock glog gflags flume-runtime-local-local_input_executor)

cc_library(flume-runtime-local-local_executor_factory SRCS ../flume/runtime/local/local_executor_factory.cpp DEPS flume-runtime-local-local_input_executor flume-runtime-executor_factory flume-runtime-common-write_cache_executor flume-proto-physical_plan)

cc_test(flume-runtime-local-local_executor_factory_test SRCS ../flume/runtime/local/local_executor_factory_test.cpp DEPS gmock glog gflags flume-runtime-local-local_executor_factory flume-planner-local-local_planner flume-core-logical_plan)

cc_library(flume-runtime-local-local_backend LINK_ALL_SYMBOLS "true" SRCS ../flume/runtime/local/local_backend.cpp DEPS toft flume-util-config_util flume-runtime-task flume-runtime-resource flume-runtime-local-scripts flume-runtime-local-local_executor_factory flume-runtime-counter flume-runtime-common-local_dataset flume-runtime-common-file_cache_manager flume-runtime-backend flume-proto-physical_plan flume-proto-logical_plan flume-planner-local-local_planner flume-flume_flags flume-runtime-local-flags ctemplate)

cc_test(flume-runtime-local-local_backend_test SRCS ../flume/runtime/local/local_backend_test.cpp DEPS toft gmock glog gflags flume-runtime-local-local_backend flume-flume flume-core-logical_plan)

cc_library(flume-runtime-platform-hadoop_client SRCS ../flume/runtime/platform/hadoop_client.cpp DEPS toft flume-util-process_launcher boost)

cc_test(flume-runtime-platform-hadoop_client_test SRCS ../flume/runtime/platform/hadoop_client_test.cpp DEPS gmock glog gflags flume-runtime-platform-hadoop_client flume-core-logical_plan)

cc_library(flume-runtime-common-sub_executor_manager SRCS ../flume/runtime/common/sub_executor_manager.cpp DEPS flume-proto-physical_plan boost)

cc_test(flume-runtime-common-sub_executor_manager_test SRCS ../flume/runtime/common/sub_executor_manager_test.cpp DEPS toft gmock glog gflags flume-runtime-common-sub_executor_manager)

cc_library(flume-runtime-common-memory_dataset SRCS ../flume/runtime/common/memory_dataset.cpp DEPS flume-util-arena)

cc_test(flume-runtime-common-memory_dataset_test SRCS ../flume/runtime/common/memory_dataset_test.cpp DEPS gmock glog gflags flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-local_dataset SRCS ../flume/runtime/common/local_dataset.cpp DEPS leveldb gmock flume-util-arena)

cc_test(flume-runtime-common-local_dataset_test SRCS ../flume/runtime/common/local_dataset_test.cpp DEPS gmock glog gflags flume-runtime-common-local_dataset)

cc_library(flume-runtime-common-general_dispatcher SRCS ../flume/runtime/common/general_dispatcher.cpp DEPS toft flume-util-arena flume-core-entity)

cc_test(flume-runtime-common-general_dispatcher_test SRCS ../flume/runtime/common/general_dispatcher_test.cpp DEPS toft gmock glog gflags flume-runtime-common-memory_dataset flume-runtime-common-general_dispatcher)

cc_library(flume-runtime-common-single_dispatcher SRCS ../flume/runtime/common/single_dispatcher.cpp DEPS toft flume-util-arena flume-core-entity)

cc_test(flume-runtime-common-single_dispatcher_test SRCS ../flume/runtime/common/single_dispatcher_test.cpp DEPS toft gmock glog gflags flume-runtime-common-single_dispatcher flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-executor_base SRCS ../flume/runtime/common/executor_base.cpp DEPS toft gmock flume-runtime-common-sub_executor_manager flume-runtime-common-single_dispatcher flume-runtime-common-general_dispatcher)

cc_test(flume-runtime-common-executor_base_test SRCS ../flume/runtime/common/executor_base_test.cpp DEPS toft gmock glog gflags flume-runtime-common-executor_base)

cc_library(flume-runtime-common-entity_dag SRCS ../flume/runtime/common/entity_dag.cpp DEPS toft flume-runtime-counter flume-core-entity flume-proto-logical_plan flume-proto-split)

cc_test(flume-runtime-common-entity_dag_test SRCS ../flume/runtime/common/entity_dag_test.cpp DEPS gmock glog gflags flume-runtime-common-entity_dag flume-core-logical_plan)

cc_library(flume-runtime-common-processor_cache SRCS ../flume/runtime/common/processor_cache.cpp DEPS glog gflags boost)

cc_library(flume-runtime-common-task_executor SRCS ../flume/runtime/common/task_executor.cpp DEPS flume-runtime-common-sub_executor_manager flume-proto-physical_plan)

cc_library(flume-runtime-common-union_executor SRCS ../flume/runtime/common/union_executor.cpp DEPS toft flume-runtime-common-executor_impl flume-proto-physical_plan flume-core-entity)

cc_test(flume-runtime-common-union_executor_test SRCS ../flume/runtime/common/union_executor_test.cpp DEPS toft gmock glog gflags flume-runtime-common-union_executor flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-loader_executor SRCS ../flume/runtime/common/loader_executor.cpp DEPS flume-runtime-common-executor_impl flume-proto-physical_plan flume-core-loader flume-core-entity)

cc_test(flume-runtime-common-loader_executor_test SRCS ../flume/runtime/common/loader_executor_test.cpp DEPS gmock glog gflags flume-runtime-common-memory_dataset flume-runtime-common-loader_executor)

cc_library(flume-runtime-common-sinker_executor SRCS ../flume/runtime/common/sinker_executor.cpp DEPS toft flume-proto-physical_plan flume-core-entity)

cc_test(flume-runtime-common-sinker_executor_test SRCS ../flume/runtime/common/sinker_executor_test.cpp DEPS toft gmock glog gflags flume-runtime-common-sinker_executor)

cc_library(flume-runtime-common-processor_executor SRCS ../flume/runtime/common/processor_executor.cpp DEPS toft flume-runtime-common-general_dispatcher flume-runtime-common-executor_base flume-proto-physical_plan flume-core-entity)

cc_test(flume-runtime-common-processor_executor_test SRCS ../flume/runtime/common/processor_executor_test.cpp DEPS toft gmock glog gflags flume-runtime-common-processor_executor flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-shuffle_runner SRCS ../flume/runtime/common/shuffle_runner.cpp DEPS toft flume-util-arena flume-proto-physical_plan flume-proto-logical_plan flume-core-entity)

cc_test(flume-runtime-common-shuffle_runner_test SRCS ../flume/runtime/common/shuffle_runner_test.cpp DEPS toft gmock glog gflags flume-runtime-common-shuffle_runner flume-runtime-common-memory_dataset flume-runtime-common-general_dispatcher)

cc_library(flume-runtime-common-status_shuffle_executor SRCS ../flume/runtime/common/status_shuffle_executor.cpp DEPS toft flume-runtime-common-shuffle_runner flume-runtime-common-general_dispatcher flume-runtime-common-executor_base flume-proto-physical_plan flume-core-entity)

cc_test(flume-runtime-common-status_shuffle_executor_test SRCS ../flume/runtime/common/status_shuffle_executor_test.cpp DEPS toft gmock glog gflags flume-runtime-common-status_shuffle_executor flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-shuffle_executor SRCS ../flume/runtime/common/shuffle_executor.cpp DEPS toft flume-runtime-common-shuffle_runner flume-runtime-common-executor_impl flume-proto-physical_plan)

cc_test(flume-runtime-common-shuffle_executor_test SRCS ../flume/runtime/common/shuffle_executor_test.cpp DEPS toft gmock glog gflags flume-runtime-common-shuffle_executor flume-runtime-common-memory_dataset)

cc_library(flume-runtime-common-partial_executor SRCS ../flume/runtime/common/partial_executor.cpp DEPS toft flume-runtime-common-sub_executor_manager flume-runtime-common-general_dispatcher flume-runtime-common-entity_dag flume-proto-physical_plan flume-core-entity)

cc_test(flume-runtime-common-partial_executor_test SRCS ../flume/runtime/common/partial_executor_test.cpp DEPS cityhash toft gmock glog gflags flume-runtime-common-partial_executor flume-runtime-common-memory_dataset flume-core-logical_plan)

cc_library(flume-runtime-common-write_cache_executor SRCS ../flume/runtime/common/write_cache_executor.cpp DEPS toft flume-util-path_util flume-runtime-common-general_dispatcher flume-runtime-common-executor_base flume-proto-physical_plan flume-core-entity)

cc_library(flume-runtime-common-memory_status_table SRCS ../flume/runtime/common/memory_status_table.cpp DEPS flume-core-logical_plan)

cc_test(flume-runtime-common-memory_status_table_test SRCS ../flume/runtime/common/memory_status_table_test.cpp DEPS gmock glog gflags flume-runtime-common-memory_status_table)

cc_library(flume-runtime-common-cache_manager SRCS ../flume/runtime/common/cache_manager.cpp DEPS glog gflags boost)

cc_library(flume-runtime-common-file_cache_manager SRCS ../flume/runtime/common/file_cache_manager.cpp DEPS toft flume-util-path_util flume-runtime-common-cache_manager)

cc_test(flume-runtime-common-file_cache_manager_test SRCS ../flume/runtime/common/file_cache_manager_test.cpp DEPS gmock glog gflags flume-runtime-common-file_cache_manager)

cc_library(flume-runtime-common-cache_iterator SRCS ../flume/runtime/common/cache_iterator.cpp DEPS flume-util-reflection boost)

cc_test(flume-runtime-common-cache_iterator_test SRCS ../flume/runtime/common/cache_iterator_test.cpp DEPS gmock glog gflags flume-runtime-common-file_cache_manager flume-runtime-common-cache_iterator)

cc_library(flume-runtime-common-executor_impl SRCS ../flume/runtime/common/executor_impl.cpp DEPS flume-runtime-common-sub_executor_manager flume-runtime-common-single_dispatcher flume-runtime-common-general_dispatcher)

cc_test(flume-runtime-common-executor_impl_test SRCS ../flume/runtime/common/executor_impl_test.cpp DEPS toft gmock glog gflags flume-runtime-common-executor_impl)

cc_library(flume-runtime-common-transfer_encoding SRCS ../flume/runtime/common/transfer_encoding.cpp DEPS flume-proto-transfer_encoding boost)

cc_test(flume-runtime-common-transfer_encoding_test SRCS ../flume/runtime/common/transfer_encoding_test.cpp DEPS toft gmock glog gflags flume-runtime-common-transfer_encoding)

cc_library(flume-runtime-common-split_combiner SRCS ../flume/runtime/common/split_combiner.cpp DEPS flume-proto-split flume-thrift-backend_service_thrift)

cc_test(flume-runtime-common-split_combiner_test SRCS ../flume/runtime/common/split_combiner_test.cpp DEPS toft gmock glog gflags flume-runtime-common-split_combiner)

cc_library(flume-runtime-common-fix_empty_group_executor SRCS ../flume/runtime/common/fix_empty_group_executor.cpp DEPS toft flume-runtime-common-shuffle_runner flume-runtime-common-executor_impl flume-proto-physical_plan)

cc_test(flume-runtime-common-fix_empty_group_executor_test SRCS ../flume/runtime/common/fix_empty_group_executor_test.cpp DEPS gmock glog gflags flume-runtime-common-fix_empty_group_executor boost)

cc_test(flume-runtime-util-serialize_buffer_test SRCS ../flume/runtime/util/serialize_buffer_test.cpp DEPS toft gmock glog gflags boost)

resource_library(flume-runtime-local-scripts SRCS ../flume/runtime/local/static/start.sh ../flume/runtime/local/static/commit.sh OUTPUTS start_sh.c commit_sh.c)

ADD_CUSTOM_TARGET(bulid_spark_launcher_jar
  COMMAND cd ${PROJECT_SOURCE_DIR}/flume/runtime/scala && PATH=${THRIFT_INSTALL_DIR}/bin:$ENV{PATH} mvn package -q
)
ADD_DEPENDENCIES(bulid_spark_launcher_jar thrift)

resource_library(flume-runtime-spark-spark_launcher_jar SRCS ../flume/runtime/spark/static/spark_launcher.jar OUTPUTS spark_launcher_jar.c DEPS bulid_spark_launcher_jar)

# todo: move this function into generic.cmake as thrift_library
ADD_CUSTOM_COMMAND(OUTPUT thrift/backend_service_constants.h thrift/backend_service_constants.cpp thrift/backend_service_types.h thrift/backend_service_types.cpp thrift/TBackendService.h thrift/TBackendService.cpp
  MAIN_DEPENDENCY ../flume/thrift/backend_service.thrift
  DEPENDS thrift
  COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/thrift
  COMMAND cd ${PROJECT_SOURCE_DIR}/flume/thrift && ${THRIFT_INSTALL_DIR}/bin/thrift --gen cpp -out ${CMAKE_CURRENT_BINARY_DIR}/thrift backend_service.thrift
)

cc_library(flume-thrift-backend_service_thrift SRCS thrift/backend_service_constants.cpp thrift/backend_service_types.cpp thrift/TBackendService.cpp DEPS thrift)

resource_library(flume-runtime-spark-profile_scripts SRCS ../flume/runtime/spark/static/draw.sh ../flume/runtime/spark/static/pprof OUTPUTS draw_sh.c pprof.c)

cc_library(flume-planner-spark-add_cache_task_pass SRCS ../flume/planner/spark/add_cache_task_pass.cpp DEPS flume-planner-plan flume-planner-pass_manager flume-planner-rule_dispatcher flume-planner-depth_first_dispatcher)

cc_library(flume-planner-spark-testing_helper SRCS ../flume/planner/spark/testing/testing_helper.cpp DEPS flume-planner-testing-pass_test_helper)

cc_test(flume-planner-spark-init_rdd_info_pass_test SRCS ../flume/planner/spark/init_rdd_info_pass_test.cpp DEPS gmock flume-planner-spark-init_rdd_info_pass flume-planner-testing-pass_test_helper flume-planner-spark-testing_helper)

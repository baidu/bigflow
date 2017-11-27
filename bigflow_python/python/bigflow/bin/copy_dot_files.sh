#!/bin/bash
# @Author: zhangyuncong
# @Date:   2016-04-12 11:17:48
# @Last Modified by:   zhangyuncong
# @Last Modified time: 2016-04-12 12:51:09
TASKID=$mapred_task_id
TARGET=$USER_LOG_DIR/$TASKID/explain
cp -r ./flume/dot/ $TARGET

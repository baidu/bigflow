# @Author: zhangyuncong
# @Date:   2016-04-12 13:33:22
# @Last Modified by:   zhangyuncong
# @Last Modified time: 2016-04-12 13:34:44

TASKID=$mapred_task_id
HEAPPATH=$USER_LOG_DIR/$TASKID/heap
mkdir -p $HEAPPATH
export HEAPPROFILE="$HEAPPATH/heap.file"

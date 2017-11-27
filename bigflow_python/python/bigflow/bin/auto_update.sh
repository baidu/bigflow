echo "Bigflow auto-update starting..." >&2

export BIGFLOW_FILE_OUTPUT_FORMAT_ASYNC_MODE=true

AUTO_UPDATE_BIGFLOW_VERSION=`grep -o "[0-9]\.[0-9]\.[0-9]\.[0-9]"  $CUR/../../bigflow/pipeline/mr_pipeline.py`
AUTO_UPDATE_PATCH_PATH=$CUR/../../patch
AUTO_UPDATE_PATCH_ID=`cat /proc/sys/kernel/random/uuid`
AUTO_UPDATE_PATCH_TMP_PATH="$AUTO_UPDATE_PATCH_PATH/$AUTO_UPDATE_PATCH_ID"

if [ ! -d "$AUTO_UPDATE_PATCH_PATH" ]; then
    mkdir -p "$AUTO_UPDATE_PATCH_PATH"
fi

function fUpdateFile() {
    PATCH_FILE_PATH="$AUTO_UPDATE_PATCH_TMP_PATH/$1"
    PATCH_MD5_FILE_PATH="${PATCH_FILE_PATH}.md5"
    DESTINATION_MD5_FILE_PATH="$AUTO_UPDATE_PATCH_PATH/${1}.md5"
    SOURCE_FILE_PATH="http://bigflow.cloud/download/update/$2/$1"
    DESTINATION_FILE_PATH="$CUR/../../$1"
    MD5=$3

    if [ -f "$DESTINATION_MD5_FILE_PATH" ]; then
        if [ -n $MD5 ]; then
            MD5_FILE_VALUE=`cat $DESTINATION_MD5_FILE_PATH`
            if [ "$MD5_FILE_VALUE"x == "$MD5"x ]; then
                echo "Update $1 has been completed" >&2
                return
            fi
        fi
    fi

    if [ ! -d `dirname $PATCH_FILE_PATH` ]; then
        mkdir -p `dirname $PATCH_FILE_PATH`
    fi

    # download patch & update
    curl -sSo "$PATCH_FILE_PATH" "$SOURCE_FILE_PATH"

    if [ ! -d `dirname $DESTINATION_FILE_PATH` ]; then
        mkdir -p `dirname $DESTINATION_FILE_PATH`
    fi

    mv "$PATCH_FILE_PATH" "$DESTINATION_FILE_PATH"

    # calc md5sum
    md5sum "$DESTINATION_FILE_PATH" | awk '{print $1}' > "$PATCH_MD5_FILE_PATH"

    if [ ! -d `dirname $DESTINATION_MD5_FILE_PATH` ]; then
        mkdir -p `dirname $DESTINATION_MD5_FILE_PATH`
    fi

    mv "$PATCH_MD5_FILE_PATH" "$DESTINATION_MD5_FILE_PATH"

    echo "Update $1 completed" >&2
}



if [ "$AUTO_UPDATE_BIGFLOW_VERSION"x == "1.0.3.4"x ]; then
    fUpdateFile bigflow/core/entity.py 1.0.3.4_to_1.0.3.5
    fUpdateFile bigflow/pipeline/mr_pipeline.py 1.0.3.4_to_1.0.3.5
fi

if [ "$AUTO_UPDATE_BIGFLOW_VERSION"x == "1.0.3.5"x ]; then
    fUpdateFile bigflow/pipeline/mr_pipeline.py 1.0.3.5_to_1.0.3.5 c41031bec16433efb74bc1bca1372967
fi

rm -rf "$AUTO_UPDATE_PATCH_TMP_PATH"

echo "Bigflow auto-update done." >&2

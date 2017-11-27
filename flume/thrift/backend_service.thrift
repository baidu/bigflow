namespace java baidu.flume.thrift_gen
namespace cpp baidu.flume


struct TResponseStatus {
   1: optional bool success,
   2: optional string reason,
}

struct TVoidResponse {
    1: optional TResponseStatus status,
}

struct TKeysValue {
    1: optional list<binary> keys;
    2: optional binary value;
}
struct TGetCachedDataResponse {
    1: optional TResponseStatus status;
    2: optional list<TKeysValue> data;
}

// todo: need a proper name
service TBackendService {

    TVoidResponse runJob(1: binary physical_job),

    TGetCachedDataResponse getCachedData(1: string node_id),

    oneway void stop(),
}

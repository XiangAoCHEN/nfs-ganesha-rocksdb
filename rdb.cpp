#include <cstdio>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
// g++ -std=c++17 -o rdb rdb.cpp -lpthread -lrocksdb -ldl
using namespace std;
using namespace rocksdb;

const std::string PATH = "/tmp/rocksdb_tmp";
int main(){
    DB* db;
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, PATH, &db);
    assert(status.ok());
    Slice key("foo");
    Slice value("bar");
    std::string get_value;
    status = db->Put(WriteOptions(), key, value);
    if(status.ok()){
        status = db->Get(ReadOptions(), key, &get_value);
        if(status.ok()){
            printf("get %s success!!\n", get_value.c_str());
        }else{
            printf("get failed\n");
        }
    }else{
        printf("put failed\n");
    }
    delete db;
}

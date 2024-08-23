#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

// g++ -std=c++17 -o rangedelete rangedelete.cpp -lpthread -lrocksdb -ldl

int main() {
    // RocksDB数据库配置
    rocksdb::Options options;
    options.create_if_missing = true;

    // 打开数据库
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/testdb", &db);
    if (!status.ok()) {
        std::cerr << "Unable to open RocksDB: " << status.ToString() << std::endl;
        return 1;
    }

    // 插入数据
    std::string prefix = "1_2_";
    std::vector<std::string> keys = {"2","3", "4", "5", "6", "7", "8", "9", "10", "11"};
    for (const auto& key_suffix : keys) {
        std::string key = prefix + key_suffix;
        std::string value = "value_" + key_suffix;
        status = db->Put(rocksdb::WriteOptions(), key, value);
        if (!status.ok()) {
            std::cerr << "Error inserting key: " << key << std::endl;
        }
    }

    // 使用 DeleteRange 删除 [1_2_3, 1_2_10)
    std::string start_key = prefix;           // 以 "1_2_" 为前缀的键
    std::string end_key = prefix + "\xFF";    // end_key 为 "1_2_\xFF"，这样就涵盖了所有以 "1_2_" 开头的键
    status = db->DeleteRange(rocksdb::WriteOptions(), db->DefaultColumnFamily(), start_key, end_key);
    if (!status.ok()) {
        std::cerr << "Error deleting range: " << status.ToString() << std::endl;
    }

    // 打印剩下的键值对
    std::cout << "Remaining keys after DeleteRange:" << std::endl;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "Key: " << it->key().ToString() << ", Value: " << it->value().ToString() << std::endl;
    }
    if (!it->status().ok()) {
        std::cerr << "An error occurred during iteration: " << it->status().ToString() << std::endl;
    }

    // 清理资源
    delete it;
    delete db;

    return 0;
}

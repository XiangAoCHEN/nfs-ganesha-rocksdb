#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/table.h"
#include "rocksdb/sst_file_reader.h"
#include <iostream>
#include <vector>
#include <algorithm>
#include <memory>
// g++ -std=c++17 -o sst sst.cpp -lpthread -lrocksdb -ldl

int main() {
    // 设置数据库路径
    std::string db_path = "/tmp/testdb";

    // 删除已有的数据库文件，确保是从一个干净的数据库开始
    rocksdb::DestroyDB(db_path, rocksdb::Options());

    // RocksDB Options
    rocksdb::Options options;
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger = 2; // 触发compaction的文件数阈值
    options.max_background_compactions = 2;
    options.max_bytes_for_level_base = 1 * 16 * 1024; // Level 1 的文件大小限制为 3MB
    options.target_file_size_base = 16  * 1024; // 每个SST文件的目标大小
    options.write_buffer_size = 16  * 1024;     // Memtable的大小
    options.max_bytes_for_level_multiplier = 2;

    // 打开数据库
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "Unable to open RocksDB: " << status.ToString() << std::endl;
        return 1;
    }

    for(int j=0; j<50; j++){
        // 插入一些数据
        for (int i = 0; i < 10000; ++i) {
            db->Put(rocksdb::WriteOptions(), "key" + std::to_string(j*1000+i), "value" + std::to_string(j*1000+i));
        }

        // 手动触发 Compaction，以确保生成多个Level的SST文件
        db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    }

    for (int i = 0; i < 10000; ++i) {
        db->Put(rocksdb::WriteOptions(), "key" + std::to_string(60*1000+i), "value" + std::to_string(60*1000+i));
    }
    

    // 获取 Live File Metadata
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);

    // 打印 SST 文件的详细信息
    int max_level = 0;
    for (const auto& file_meta : metadata) {
        max_level = std::max(max_level, file_meta.level);
    }
    std::cout << "Max Level: " << max_level << std::endl;
    std::string max_level_file_path;
    std::string max_level_file_name;
    for (const auto& file_meta : metadata){
        if(file_meta.level == max_level){
            std::cout << "SST File Name: " << file_meta.name << std::endl;
            std::cout << "Level: " << file_meta.level << std::endl;
            std::cout << "Size: " << file_meta.size << " bytes" << std::endl;
            std::cout << "Smallest Key: " << file_meta.smallestkey << std::endl;
            std::cout << "Largest Key: " << file_meta.largestkey << std::endl;
            std::cout << "Smallest SeqNo: " << file_meta.smallest_seqno << std::endl;
            std::cout << "Largest SeqNo: " << file_meta.largest_seqno << std::endl;
            std::cout << "Num Reads Sampled: " << file_meta.num_reads_sampled << std::endl;
            std::cout << "Being Compacted: " << (file_meta.being_compacted ? "Yes" : "No") << std::endl;
            std::cout << "Column Family: " << file_meta.column_family_name << std::endl;
            std::cout << "File directory: " << file_meta.directory << std::endl;
            std::cout << "File Path: " << (file_meta.directory + file_meta.name) << std::endl;
            std::cout << "---------------------------------------------" << std::endl;

            max_level_file_path = file_meta.directory + file_meta.name;
            max_level_file_name = file_meta.name;
            break;
        }
    }

    // 配置 Options
    // rocksdb::Options options;
    // options.env = rocksdb::Env::Default();
    
    // 创建 SstFileReader
    rocksdb::SstFileReader sst_reader(options);
    status = sst_reader.Open(max_level_file_path);
    if (!status.ok()) {
        std::cerr << "Error opening SST file: " << status.ToString() << std::endl;
        return 1;
    }

    // 获取迭代器
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> it(sst_reader.NewIterator(read_options));

    // 迭代并打印 KV 对
    // for (it->SeekToFirst(); it->Valid(); it->Next()) {
    //     std::cout << "Key: " << it->key().ToString() << " Value: " << it->value().ToString() << std::endl;
    // }

    if (!it->status().ok()) {
        std::cerr << "Error during iteration: " << it->status().ToString() << std::endl;
    }

    it.reset(); // 确保迭代器被释放

    status = db->DeleteFile(max_level_file_name);
    if (!status.ok()) {
        std::cerr << "Error deleting SST file: " << status.ToString() << std::endl;
    } else {
        std::cout << "SST file deleted successfully." << std::endl;
    }

    // 关闭数据库
    delete db;

    return 0;
}

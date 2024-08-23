#include <vector>
#include <string>
#include <fcntl.h>
#include <cerrno>
#include <chrono>
#include "applier/interface.h"
#include "applier/log_parse.h"
#include "applier/applier_config.h"
#include "applier/log_log.h"
#include "applier/log_apply.h"

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "common_utils.h"
#include "log.h"
#ifdef __cplusplus
}
#endif

void init_applier_module(void) {

    // test rocksdb
    rocksdb::Options options;
    options.create_if_missing = true;
    //write buffer
    options.write_buffer_size = ROCKSDB_WRITE_BUFFER_SIZE;
    options.max_write_buffer_number = ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
    options.db_write_buffer_size = ROCKSDB_DB_WRITE_BUFFER_SIZE;
    // block cache
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(ROCKSDB_BLOCK_CACHE_SIZE);
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    rocksdb::Status status = rocksdb::DB::Open(options,ROCKSDB_DATA_PATH,&db);
    std::cout<<"create and open db ok, status " <<status.code()<<"\n";

    std::string test_key1("key1");
    std::string test_value1("value1");
    rocksdb::Status s1 = db->Put(rocksdb::WriteOptions(),test_key1,test_value1);
    std::string test_key2("key2");
    std::string test_value2("value2");
    rocksdb::Status s2 = db->Put(rocksdb::WriteOptions(),test_key2,test_value2);
    std::string test_key3("key3");
    std::string test_value3("value3");
    rocksdb::Status s3 = db->Put(rocksdb::WriteOptions(),test_key3,test_value3);
    if(s1.ok() && s2.ok() && s3.ok()){
        std::cout<<"Put Key OK \n";
        std::string saved_val1;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), test_key1, &saved_val1);

        if(s.ok()){
            std::cout<<"read ok, value is "<<saved_val1<<" \n";
        }else{
            std::cout<<"failed. \n";
        }
        std::string saved_val2;
        rocksdb::Status s4 = db->DeleteRange(rocksdb::WriteOptions(), db->DefaultColumnFamily(),test_key1,test_key3);
        if(s4.ok()){
            std::cout<<"delete range ok \n";
            rocksdb::Status s5 = db->Get(rocksdb::ReadOptions(), test_key2, &saved_val2);
            if(s5.ok()){
                std::cout<<"read: "<<saved_val2<<" status " <<s.code()<<"\n";
            }else{
                std::cout<<"OK, read no value \n ";
            }
            
        }
    }
    std::string test_key4("123_9_78");
    std::string test_key4_prefix("123_9");
    std::string test_value4("value4");
    rocksdb::Status s4 = db->Put(rocksdb::WriteOptions(),test_key4,test_value4);
    if(s4.ok()){
        std::cout<<"put key4 ok\n";
    }
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for(it->Seek(test_key4_prefix); it->Valid() && it->key().starts_with(test_key4_prefix); it->Next()){
        // 打印key
        std::cout << "Key: " << it->key().ToString() << std::endl;
        // 打印value
        std::cout << "Value: " << it->value().ToString() << std::endl;
    }
    delete it;
    // end of test rocksdb
    insert_duration_micros = std::chrono::microseconds(0);
    search_duration_micros = std::chrono::microseconds(0);
    extract_duration_micros = std::chrono::microseconds(0);
    insert_cnt = 0;
    search_cnt = 0;
    extract_cnt = 0;
    db_write_duration_micros = std::chrono::microseconds(0);
    db_fg_read_duration_micros = std::chrono::microseconds(0);
    db_bg_read_duration_micros = std::chrono::microseconds(0);
    db_write_cnt = 0;
    db_fg_read_cnt = 0;
    db_bg_read_cnt = 0;

    PTHREAD_MUTEX_init(&log_group_mutex, NULL);
    PTHREAD_COND_init(&log_parse_condition, NULL);
    PTHREAD_COND_init(&log_apply_condition, NULL);



    // init log group
    log_group.log_file_number = LOG_FILE_NUMBER;
    for (int i = 0; i < log_group.log_file_number; ++i) {
        std::string filename;
        filename += LOG_FILES_BASE_NAME;
        filename += std::to_string(i);
        log_group.log_filenames.push_back(filename);
        log_group.file_handles.push_back(nullptr);
        std::string full_path = LOG_PATH_PREFIX;
        full_path += filename;
        log_group.log_file_full_paths.push_back(full_path);
        int fd = open(full_path.c_str(), O_RDWR);
        assert(fd > 0);
        log_group.log_fds.push_back(fd);
        if (i == 0) {
            log_group.per_file_size = lseek(fd, 0, SEEK_END);
            assert(log_group.per_file_size != 0);
        }
        assert(log_group.per_file_size == lseek(fd, 0, SEEK_END));// 保证所有的log file是一样大小的
    }

    log_group.sys_tablespace_fd = open(SYS_TABLE_FILE, O_RDWR);
    assert(log_group.sys_tablespace_fd > 0);
    log_group.batch_size = APPLY_BATCH_SIZE;

    // APPLY_BATCH_SIZE必须能被log_group.per_file_size整除
    if (log_group.per_file_size % log_group.batch_size != 0) {
        LogFatal(COMPONENT_INIT, "APPLY_BATCH_SIZE must be divisible by log per file size. "
                                 "APPLY_BATCH_SIZE is %zu, but log per file size is %zu",
                                 APPLY_BATCH_SIZE, log_group.per_file_size);
    }
    // init apply_pointer
    log_group.checkpoint_no = 0;
    log_group.checkpoint_lsn = 0;
//    log_group.parsed_lsn = 0;
    log_group.written_lsn = 0;
    log_group.applied_lsn = 0;
    log_group.parsed_offset = 0;
    log_group.written_offset = 0;
    log_group.applied_offset = 0;
    log_group.need_to_parse = 0;
    log_group.written_isn = 0;
    log_group.parsed_isn = 0;
    log_group.applied_isn = 0;
    log_group.log_buf = nullptr;
    log_group.first_written = false;

    // 每一个log block中能存储的有效的log大小
    auto log_data_per_block = LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE;

    // 每一个log文件中能存储log的block数量
    auto log_blocks = log_group.per_file_size / LOG_BLOCK_SIZE - N_LOG_METADATA_BLOCKS;
    log_group.log_buf_size_per_file = log_data_per_block * log_blocks;
    log_group.log_buf_size = (sizeof(unsigned char) * log_group.log_buf_size_per_file * log_group.log_file_number);
    log_group.log_buf = (unsigned char *) malloc(log_group.log_buf_size);

    log_group.log_meta_buf = nullptr;
    log_group.log_meta_buf_size = N_LOG_METADATA_BLOCKS * LOG_BLOCK_SIZE;
    log_group.log_meta_buf = (unsigned char *) malloc(log_group.log_meta_buf_size);

//    auto log_data_size_per_file = log_group.per_file_size - log_group.log_meta_buf_size;
    if (log_group.log_buf == nullptr || log_group.log_meta_buf == nullptr) {
        LogFatal(COMPONENT_INIT, "start nfs-ganesha failed, malloc failed %s", strerror(errno));
    }

    // 填充 log meta buf
    auto res = pread(log_group.log_fds[0],
                     (void *)(log_group.log_meta_buf),
                     log_group.log_meta_buf_size,
                     0);
    if (res != log_group.log_meta_buf_size) {
        LogFatal(COMPONENT_INIT, "start nfs-ganesha failed, %s", strerror(errno));
    }

    log_group.log_header_start_lsn = mach_read_from_8(log_group.log_meta_buf + LOG_HEADER_START_LSN);
    LogEvent(COMPONENT_INIT, "LOG_HEADER_START_LSN %zu", log_group.log_header_start_lsn);
    size_t checkpoint_no;
    size_t checkpoint_offset;
    size_t checkpoint_lsn;
    find_max_checkpoint(log_group.log_meta_buf, &checkpoint_lsn,
                        &checkpoint_no,
                        &checkpoint_offset);

    LogEvent(COMPONENT_INIT, "checkpoint_no %zu, checkpoint_offset %zu, checkpoint_lsn %zu",
             checkpoint_no, checkpoint_offset, checkpoint_lsn);
    log_group.checkpoint_lsn = checkpoint_lsn;
    log_group.checkpoint_no = checkpoint_no;
    log_group.checkpoint_offset = checkpoint_offset;
    log_group.start_offset = checkpoint_offset / LOG_BLOCK_SIZE * LOG_BLOCK_SIZE;
    // 寻找parse和log write的起点
    auto n_file = checkpoint_offset / log_group.per_file_size;
    auto off_in_file = checkpoint_offset % log_group.per_file_size;

    auto n_block = off_in_file / LOG_BLOCK_SIZE;
    auto off_in_block = off_in_file % LOG_BLOCK_SIZE;
    unsigned char log_block_buf[LOG_BLOCK_SIZE];

    res = pread(log_group.log_fds[n_file],
                (void *)(log_block_buf),
                LOG_BLOCK_SIZE,
                n_block * LOG_BLOCK_SIZE);
    if (res != LOG_BLOCK_SIZE) {
        LogFatal(COMPONENT_INIT, "start nfs-ganesha failed, %s", strerror(errno));
    }

    auto data_len = mach_read_from_2(log_block_buf + LOG_BLOCK_HDR_DATA_LEN);

    if (off_in_block != data_len) {
        // 在log file 中有尚未被恢复的log
        LogFatal(COMPONENT_INIT, "start nfs-ganesha failed, there are logs that have not been recovered in log file");
    }

    log_group.written_offset = log_group.parsed_offset = log_group_off_to_log_buf_off(checkpoint_offset);
    log_group.need_to_parse = 0;
    log_group.written_capacity = log_group.log_buf_size;

    log_group.written_capacity = log_group.log_file_number * log_group.per_file_size;

    log_parse_thread_start();
    log_apply_thread_start(APPLIER_THREAD);
}

int is_log_file_in_name(const char *filename) {
    assert(log_group.log_file_number == log_group.log_filenames.size());
    for (int i = 0; i < log_group.log_file_number; ++i) {
        if (log_group.log_filenames[i] == filename) {
            return i;
        }
    }

    return -1;
}
int is_ibd_file_in_name(const char *filename) {
    return DataPageGroup::Get().Exist(filename);
}
int is_log_file_in_handle(const struct fsal_obj_handle *handle) {
    assert(log_group.log_file_number == log_group.log_filenames.size());
    for (int i = 0; i < log_group.log_file_number; ++i) {
        if (log_group.file_handles[i] == handle) {
            return i;
        }
    }

    return -1;
}
int is_ibd_file_in_handle(const struct fsal_obj_handle *handle) {
    return DataPageGroup::Get().Exist(handle);
}
void register_log_file_handle(int index, struct fsal_obj_handle *handle) {
    assert(0 <= index && index < log_group.log_file_number);
    log_group.file_handles[index] = handle;
}
void register_ibd_file_handle(struct fsal_obj_handle *handle, int space_id) {
    DataPageGroup::Get().Insert(handle, space_id);
}
const auto copy_buf_size = 8 << 10 << 10; // 8M
unsigned char* const copy_buf = new unsigned char[copy_buf_size] ;

pthread_mutex_t log_writer_mutex = PTHREAD_MUTEX_INITIALIZER;

void copy_log_to_buf(int log_file_index, size_t offset, struct iovec iov[], int iov_count) {
    PthreadMutexGuard guard(log_writer_mutex);

    assert(offset % LOG_BLOCK_SIZE == 0); // offset必须是block对齐的

    // 收集所有的iovec到一起
    size_t total_len = 0;
    auto *dest_buf = copy_buf;
    for (int i = 0; i < iov_count; ++i) {
        assert(total_len + iov[i].iov_len <= copy_buf_size); // 一次log write不能写超过8M
        std::memcpy(dest_buf, iov[i].iov_base, iov[i].iov_len);
        total_len += iov[i].iov_len;
        dest_buf += iov[i].iov_len;
    }
    assert(total_len % LOG_BLOCK_SIZE == 0); // 每次log write必须是以LOG_BLOCK_SIZE为单位

    if (offset == 0 && (total_len > N_LOG_METADATA_BLOCK_BYTES)) {
        // 跳过 meta_block
        dest_buf = copy_buf + N_LOG_METADATA_BLOCK_BYTES;
        offset += N_LOG_METADATA_BLOCK_BYTES;
        total_len -= N_LOG_METADATA_BLOCK_BYTES;
    } else {
        dest_buf = copy_buf;
    }
//    auto log_group_offset_start = log_file_index * log_group.per_file_size + offset + LOG_BLOCK_HDR_SIZE;
//    auto log_group_offset_end = log_file_index * log_group.per_file_size + offset + total_len - LOG_BLOCK_TRL_SIZE;
//    auto log_buf_offset_start = log_group_off_to_log_buf_off(log_group_offset_start);
//    auto log_buf_offset_end = log_group_off_to_log_buf_off(log_group_offset_end);

    // 这是一个重复的写请求，不需要将log写到log buf
//    if (!(log_buf_offset_start <= log_group.written_offset && log_group.written_offset <= log_buf_offset_end)) {
//        return;
//    }

//    auto write_start = std::max(log_group.written_offset, log_buf_offset_start);
//    auto write_end = log_buf_offset_end;
//    auto actual_len = write_end - write_start + 1;

    auto blocks = total_len / LOG_BLOCK_SIZE;
    size_t actual_len = 0; // 掐头去尾之后，真正需要写log buf中的长度
    size_t log_group_offset = log_file_index * log_group.per_file_size + offset;
    size_t data_offsets = 0;
    for (auto [buf, i] = std::pair<unsigned char *, int>(dest_buf, 0);
            i < blocks;
            ++i, buf += LOG_BLOCK_SIZE) {
        auto data_len = mach_read_from_2(buf + LOG_BLOCK_HDR_DATA_LEN);
        if (data_len == 0) {
            break;
        }
        data_offsets += data_len;
        // FIXME 切换日志的时候，会写meta block，一次io是4k为单位，meta block是2k，为什么meta block之后的block的data_len是0？
        assert(data_len >= LOG_BLOCK_HDR_SIZE);
        assert(data_len == 512 || data_len < LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE);

        data_len = (data_len == LOG_BLOCK_SIZE ? data_len - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE : data_len - LOG_BLOCK_HDR_SIZE);
        auto log_buf_offset_start = log_group_off_to_log_buf_off(log_group_offset + LOG_BLOCK_HDR_SIZE);
        auto log_buf_offset_end = log_buf_offset_start + data_len;
        if (log_buf_offset_end > log_group.written_offset) {
            actual_len += log_buf_offset_end - std::max(log_group.written_offset, log_buf_offset_start);
            assert(actual_len >= 0);
        }
        log_group_offset += LOG_BLOCK_SIZE;

        if (data_len != LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) {
            break;
        }
    }

    if (actual_len > 0) {
        assert((log_group.written_offset + actual_len) <= log_group.log_buf_size); // 每次log writer写不会出现跨文件的情况
        // 等待log buffer有足够的空间
        PTHREAD_MUTEX_lock(&log_group_mutex);
        while (log_group.written_capacity < actual_len) {
            LogEvent(COMPONENT_FSAL, "log writer waiting for a enough space\n");
            pthread_cond_wait(&log_write_condition, &log_group_mutex);
        }
        PTHREAD_MUTEX_unlock(&log_group_mutex);

//    auto start_ptr = log_group.log_buf + log_group.written_offset;
//    auto end_ptr = start_ptr + actual_len;
        // 把掐头去尾之后的日志拷贝到log buf
        log_group_offset = log_file_index * log_group.per_file_size + offset;
        for (auto [buf, i] = std::pair<unsigned char *, int>(dest_buf, 0);
             i < blocks;
             ++i, buf += LOG_BLOCK_SIZE) {
            auto data_len = mach_read_from_2(buf + LOG_BLOCK_HDR_DATA_LEN);

            assert(data_len >= LOG_BLOCK_HDR_SIZE);
            assert(data_len == 512 || data_len < LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE);

            data_len = (data_len == LOG_BLOCK_SIZE ? data_len - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE : data_len - LOG_BLOCK_HDR_SIZE);
            auto log_buf_offset_start = log_group_off_to_log_buf_off(log_group_offset + LOG_BLOCK_HDR_SIZE);
            auto log_buf_offset_end = log_buf_offset_start + data_len;
            if (log_buf_offset_end > log_group.written_offset) {
                assert(log_buf_offset_start <= log_group.written_offset); // 写log必须是挨个写，不能出现空洞
                auto actual_data_len = log_buf_offset_end - std::max(log_group.written_offset, log_buf_offset_start);
                auto actual_start_buf = buf + LOG_BLOCK_HDR_SIZE + data_len - actual_data_len;
                std::memcpy(log_group.log_buf + log_group.written_offset, actual_start_buf, actual_data_len);

                log_group.written_offset = (log_group.written_offset + actual_data_len) % log_group.log_buf_size;
            }
            log_group_offset += LOG_BLOCK_SIZE;
            if (data_len != LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) {
                break;
            }
        }

//    while (start_ptr < end_ptr) {
//        LOG_TYPE type;
//        space_id_t space_id;
//        page_id_t page_id;
//        byte *body;
//        auto len = ParseSingleLogRecord(type, start_ptr, end_ptr, space_id, page_id, &body);
//        assert(len != 0);
//        start_ptr += len;
//        LogEvent(COMPONENT_FSAL, "%s", GetLogString(type));
//    }

        // 更新log group的状态
        PTHREAD_MUTEX_lock(&log_group_mutex);
        log_group.written_capacity -= actual_len;
        log_group.need_to_parse += actual_len;
        log_group.written_isn += actual_len;
        pthread_cond_signal(&log_parse_condition);
        PTHREAD_MUTEX_unlock(&log_group_mutex);

        size_t new_checkpoint_offset = data_offsets + offset;
        size_t new_checkpoint_lsn = log_group.log_header_start_lsn + new_checkpoint_offset - 2048;
        size_t new_checkpoint_no = 100;
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_1, new_checkpoint_no);
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_1 + LOG_CHECKPOINT_LSN, new_checkpoint_lsn);
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_1 + LOG_CHECKPOINT_OFFSET, new_checkpoint_offset);
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_2, new_checkpoint_no);
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_2 + LOG_CHECKPOINT_LSN, new_checkpoint_lsn);
        mach_write_to_8(log_group.log_meta_buf + LOG_CHECKPOINT_2 + LOG_CHECKPOINT_OFFSET, new_checkpoint_offset);
        mach_write_to_8(log_group.sys_first_page_flushed_lsn, new_checkpoint_lsn);
        pwrite(log_group.log_fds[0], log_group.log_meta_buf + LOG_CHECKPOINT_1, 24, LOG_CHECKPOINT_1);
        pwrite(log_group.log_fds[0], log_group.log_meta_buf + LOG_CHECKPOINT_2, 24, LOG_CHECKPOINT_2);
        pwrite(log_group.sys_tablespace_fd, log_group.sys_first_page_flushed_lsn, 8, FIL_PAGE_FILE_FLUSH_LSN);
    }
}

void wait_until_apply_done(int space_id, uint64_t offset, size_t io_amount) {
    assert(offset % DATA_PAGE_SIZE == 0);
    assert(io_amount % DATA_PAGE_SIZE == 0);
    page_id_t start_page_id = offset / DATA_PAGE_SIZE;
    page_id_t end_page_id = start_page_id + io_amount / DATA_PAGE_SIZE;
    auto current_written_isn = log_group.written_isn.load();

    for (page_id_t page_id = start_page_id; page_id < end_page_id; page_id++) {
        PageAddress page_address(space_id, page_id);
        // 自旋等待log parser解析到当前已经写入的最大isn
//        LogEvent(COMPONENT_FSAL, "data page reader start reading space id = %d, page_id = %u", space_id, page_id);
        while (log_group.parsed_isn < current_written_isn);

//     主动提取相关的log进行apply
//        LogEvent(COMPONENT_FSAL, "data page reader start applying space id = %d, page_id = %u", space_id, page_id);
        auto start_time{std::chrono::steady_clock::now()};
        auto log_vector = apply_index.Search(page_address);//== on demand apply, read log
        auto end_time{std::chrono::steady_clock::now()};
        auto duration_micros = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        search_duration_micros += duration_micros;
        search_cnt ++;


        // rocksdb prefix seek
        auto rcdb_log_list = std::make_unique<std::list<LogEntry>>();
        std::string key_prefix = std::to_string(page_address.SpaceId()) + "_" +
                                 std::to_string(page_address.PageId()) + "_";
        rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
        rocksdb::WriteOptions write_options;
        auto db_start_time = std::chrono::steady_clock::now();
        // scan + delete
        size_t cnt_tmp = 0;
        for(it->Seek(key_prefix); it->Valid() && it->key().starts_with(key_prefix); it->Next()){
            // LogEvent(COMPONENT_FSAL, "Current key: %s", it->key().ToString().c_str());
            rocksdb::Slice value_slice = it->value();
            const byte* bytes = reinterpret_cast<const byte*>(value_slice.data());
            size_t size = value_slice.size();
            LogEntry retrieved_entry = LogEntry::deserialize(bytes, size);
            rcdb_log_list->push_back(std::move(retrieved_entry));

            // Delete the key after reading the value
            // rocksdb::Status del_status = db->Delete(write_options, it->key());
            // if (!del_status.ok()) {
            //     std::cout << "Error deleting key: " << it->key().ToString() << "\n";
            // }
            cnt_tmp++;
        }
        std::string start_key = key_prefix;
        std::string end_key = key_prefix + "\xFF";
        rocksdb::Status del_status = db->DeleteRange(rocksdb::WriteOptions(), db->DefaultColumnFamily(), start_key, end_key);
        if (!del_status.ok()) {
            std::cerr << "Error deleting range: " << del_status.ToString() << std::endl;
        }
        auto db_end_time = std::chrono::steady_clock::now();
        auto db_duration_micros = std::chrono::duration_cast<std::chrono::microseconds>(db_end_time - db_start_time);
        db_fg_read_duration_micros += db_duration_micros;
        db_fg_read_cnt ++;


        // rocksdb multiget
        // std::vector<lsn_t> memory_lsn_vector;
        // for (const auto &item: log_vector) {
        //     std::list<LogEntry> *log_entry_list = item.get();
        //     for (const auto &log: (*log_entry_list)) {
        //         lsn_t log_lsn = log.log_start_lsn_;
        //         memory_lsn_vector.push_back(log_lsn);
        //     }
        // }
        // std::sort(memory_lsn_vector.begin(), memory_lsn_vector.end());

        // auto multiget_res = std::make_unique<std::list<LogEntry>>();
        // std::string key_prefix = std::to_string(page_address.SpaceId()) + "_" +
        //                          std::to_string(page_address.PageId()) + "_";

        // latency test
        
        if(DataPageGroup::Get().Exist(space_id)){
            
            std::vector<lsn_t> memory_lsn_vector;
            for (const auto &item: log_vector) {
                std::list<LogEntry> *log_entry_list = item.get();
                for (const auto &log: (*log_entry_list)) {
                    lsn_t log_lsn = log.log_start_lsn_;
                    memory_lsn_vector.push_back(log_lsn);
                }
            }
            std::sort(memory_lsn_vector.begin(), memory_lsn_vector.end());

            std::vector<lsn_t> rcdb_lsn_vector;
            for (const auto &log: (*rcdb_log_list)) {
                lsn_t log_lsn = log.log_start_lsn_;
                rcdb_lsn_vector.push_back(log_lsn);
            }
            std::sort(rcdb_lsn_vector.begin(), rcdb_lsn_vector.end());

            if(memory_lsn_vector.size() != rcdb_lsn_vector.size()){
                LogEvent(COMPONENT_FSAL, "## on demand apply for [%d,%d], size differ, memory_list.size() = %d, rocksd_list.size=%d",
                    page_address.SpaceId(), page_address.PageId(),memory_lsn_vector.size(), rcdb_lsn_vector.size());
            }else{
                bool same = true;
                for(int i=0; i<memory_lsn_vector.size(); i++){
                    if(memory_lsn_vector[i] != rcdb_lsn_vector[i]){
                        same = false;
                        break;
                    }
                }
                if(!same){
                    LogEvent(COMPONENT_FSAL, "## on demand apply for [%d,%d], size same, content differ, memory_list.size() = %d, rocksd_list.size=%d",
                        page_address.SpaceId(), page_address.PageId(),memory_lsn_vector.size(), rcdb_lsn_vector.size());
                }
            }
            

            // latency test
            if(db_fg_read_cnt % 10 == 0){
                LogEvent(COMPONENT_FSAL, "## on demand apply log for [%d,%d]", space_id, page_id);
                if(search_cnt>0){
                    LogEvent(COMPONENT_FSAL, "index search IO %ld us, total search %ld us, cnt = %ld, average search %.2f us",
                        duration_micros.count(), search_duration_micros.count(), search_cnt, double(search_duration_micros.count())/search_cnt);
                }
                if(extract_cnt>0){
                    LogEvent(COMPONENT_FSAL, "index extract, total extract %ld us, cnt = %ld, average extract %.2f us",
                        extract_duration_micros.count(), extract_cnt, double(extract_duration_micros.count())/extract_cnt);
                }
                LogEvent(COMPONENT_FSAL, "index total read %ld us, read cnt = %ld, average read %.2f us",
                        search_duration_micros.count()+extract_duration_micros.count(), search_cnt+extract_cnt, double(search_duration_micros.count()+extract_duration_micros.count())/(search_cnt+extract_cnt));
                if(db_fg_read_cnt>0){
                    LogEvent(COMPONENT_FSAL, "db foreground read IO %ld us, total fg read %ld us, cnt = %ld, average fg read %.2f us",
                        db_duration_micros.count(), db_fg_read_duration_micros.count(), db_fg_read_cnt, double(db_fg_read_duration_micros.count())/db_fg_read_cnt);
                }
                if(db_bg_read_cnt>0){
                    LogEvent(COMPONENT_FSAL, "db background read, total bg read %ld us, cnt = %ld, average bg read %.2f us",
                        db_bg_read_duration_micros.count(), db_bg_read_cnt, double(db_bg_read_duration_micros.count())/db_bg_read_cnt);
                }
                if(insert_cnt>0){
                    LogEvent(COMPONENT_FSAL, "index insert, total %ld us, cnt=%ld, average %.2f us",
                        insert_duration_micros.count(), insert_cnt, double(insert_duration_micros.count())/insert_cnt);
                }
                if(db_write_cnt>0){
                    LogEvent(COMPONENT_FSAL, "rocksdb insert IO, total %ld us, cnt =%ld, average %.2f us",
                        db_write_duration_micros.count(), db_write_cnt, double(db_write_duration_micros.count())/db_write_cnt);
                }

            }
            
        }
        


        // for (const auto &item: log_vector) {
        //     log_apply_do_apply(page_address, item.get());
        // }

        if(rcdb_log_list->size() > 0){
            log_apply_do_apply(page_address, rcdb_log_list.get());
        }

    }
}

void copy_page_to_buf(char *dest_buf, space_id_t space_id, page_id_t start_page_id, int n_pages) {
    for (int i = 0; i < n_pages; ++i) {
        buffer_pool.CopyPage(dest_buf + (i * DATA_PAGE_SIZE), space_id, start_page_id + i);
    }

}
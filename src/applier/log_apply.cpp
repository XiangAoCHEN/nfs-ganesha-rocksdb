#include <memory>
#include <sstream>
#include "applier/log_apply.h"
#include "applier/log_log.h"
#include "applier/utility.h"
#include "applier/buffer_pool.h"
#include "applier/log_parse.h"
#include "applier/interface.h"

#include "rocksdb/sst_file_reader.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "common_utils.h"
#include "log.h"
#ifdef __cplusplus
}
#endif

// 检查是不是所有的log_applier都是空闲的
static bool log_apply_all_idle() {
    return std::all_of(log_appliers.cbegin(), log_appliers.cend(), [](const auto &log_applier) -> bool {
        return log_applier.is_running == false;
    });
}



static bool log_apply_apply_one_log(Page *page, const LogEntry &log) {
    // LogEvent(COMPONENT_FSAL, "apply [%d,%d], page lsn = %d, log lsn = %d, type = %s", page->GetSpaceId(),page->GetPageId(),page->GetLSN(),log.log_start_lsn_,GetLogString(log.type_));
    byte *ret;
    switch (log.type_) {
        case MLOG_1BYTE:
        case MLOG_2BYTES:
        case MLOG_4BYTES:
        case MLOG_8BYTES:
            ret = ParseOrApplyNBytes(log.type_, log.log_body_start_ptr_, log.log_body_end_ptr_, page->GetData());
            assert(ret != nullptr);
            return ret != nullptr;
        case MLOG_WRITE_STRING:
            ret = ParseOrApplyString(log.log_body_start_ptr_, log.log_body_end_ptr_, page->GetData());
            assert(ret != nullptr);
            return ret != nullptr;
        case MLOG_COMP_PAGE_CREATE:
            ret = ApplyCompPageCreate(page->GetData());
            return ret != nullptr;
        case MLOG_INIT_FILE_PAGE2:
            return ApplyInitFilePage2(log, page);
        case MLOG_COMP_REC_INSERT:
            return ApplyCompRecInsert(log, page);
        case MLOG_COMP_REC_CLUST_DELETE_MARK:
            return ApplyCompRecClusterDeleteMark(log, page);
        case MLOG_REC_SEC_DELETE_MARK:
            return ApplyRecSecondDeleteMark(log, page);
        case MLOG_COMP_REC_SEC_DELETE_MARK:
            return ApplyCompRecSecondDeleteMark(log, page);
        case MLOG_COMP_REC_UPDATE_IN_PLACE:
            return ApplyCompRecUpdateInPlace(log, page);
        case MLOG_COMP_REC_DELETE:
            return ApplyCompRecDelete(log, page);
        case MLOG_COMP_LIST_END_COPY_CREATED:
            return ApplyCompListEndCopyCreated(log, page);
        case MLOG_COMP_PAGE_REORGANIZE:
            return ApplyCompPageReorganize(log, page);
        case MLOG_COMP_LIST_START_DELETE:
        case MLOG_COMP_LIST_END_DELETE:
            return ApplyCompListDelete(log, page);
        case MLOG_IBUF_BITMAP_INIT:
            return ApplyIBufBitmapInit(log, page);
        default:
            return false;
    }
}

void log_apply_do_apply(const PageAddress &page_address, std::list<LogEntry> *log_entry_list) {
    auto space_id = page_address.SpaceId();

    // skip!
    if (!(DataPageGroup::Get().Exist(space_id))) {
        return;
    }

    auto page_id = page_address.PageId();
    // 获取需要的page
    Page *page = buffer_pool.GetPage(space_id, page_id);

    // 磁盘上没有，需要新create一个page
    if (page == nullptr) {
        page = buffer_pool.NewPage(space_id, page_id);
    }

    lsn_t page_lsn = page->GetLSN();
    for (const auto &log: (*log_entry_list)) {
        lsn_t log_lsn = log.log_start_lsn_;
//        std::cout << "space id = " << space_id << ", page id = " << page_id << ", log type = " << GetLogString(log.type_);
        // skip!
        if (page_lsn > log_lsn) {
//            std::cout << "skip" << std::endl;
            continue;
        } else {
//            std::cout << std::endl;
        }

        if (log_apply_apply_one_log(page, log)) {
            page->WritePageLSN(log_lsn + log.log_len_);
            page->WriteCheckSum(BUF_NO_CHECKSUM_MAGIC);
        }
    }
    buffer_pool.WriteBackLock(space_id, page_id);
    BufferPool::ReleasePage(page);
}

static void log_apply_worker_work(int worker_index) {
    PTHREAD_MUTEX_lock(&(log_appliers[worker_index].mutex));
    while (!(log_appliers[worker_index].need_process)) {
        pthread_cond_wait(&(log_appliers[worker_index].need_process_cond), &(log_appliers[worker_index].mutex));
    }

    log_appliers[worker_index].is_running = true;

    //rocksdb
    //== read amplification = page_num / SST_num
    auto db_start_time = std::chrono::steady_clock::now();
    // pthread_mutex_lock(&db_mutex);
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    int max_level = -1;
    for (const auto& file_meta : metadata) {
        max_level = std::max(max_level, file_meta.level);
    }
    if(max_level == -1){
        LogEvent(COMPONENT_FSAL, "No SST file found");
    }else
    {
        for(const auto& sst_file : log_appliers[worker_index].SST_files){
            rocksdb::SstFileReader sst_reader(db_options);
            rocksdb::Status status = sst_reader.Open(sst_file.file_full_path);
            if (!status.ok()) {
                LogEvent(COMPONENT_FSAL, "Error opening SST file: %s", status.ToString().c_str());
            }else{
                rocksdb::ReadOptions read_options;
                std::unique_ptr<rocksdb::Iterator> it(sst_reader.NewIterator(read_options));

                size_t total_apply_len = 0;
                size_t tmp_db_bg_read_page_cnt = 0;
                PageAddress last_page_address;
                bool first_flag = true;
                auto page_logs = std::make_unique<std::list<LogEntry>>();
                for (it->SeekToFirst(); it->Valid(); it->Next()) {
                    std::stringstream ss(it->key().ToString());
                    std::string token;
                    space_id_t space_id = 0;
                    page_id_t page_id = 0;
                    lsn_t lsn = 0;
                    if (std::getline(ss, token, '_')) space_id = static_cast<uint32_t>(std::stoul(token));
                    if (std::getline(ss, token, '_')) page_id = static_cast<uint32_t>(std::stoul(token));
                    if (std::getline(ss, token, '_')) lsn = static_cast<uint64_t>(std::stoull(token));
                    
                    if(space_id == 0 && page_id == 0 && lsn == 0){
                        LogEvent(COMPONENT_FSAL, "Iterate SST file, error parsing key: %s", it->key().ToString().c_str());
                        continue;
                    }
                    
                    db_bg_read_log_cnt ++;
                    PageAddress page_address(space_id, page_id);

                    if (first_flag) {
                        first_flag = false;
                        last_page_address = page_address;
                    }
                    if(last_page_address == page_address){//append
                        rocksdb::Slice value_slice = it->value();
                        const byte* bytes = reinterpret_cast<const byte*>(value_slice.data());
                        size_t size = value_slice.size();
                        LogEntry retrieved_entry = LogEntry::deserialize(bytes, size);
                        total_apply_len += retrieved_entry.log_len_;
                        page_logs->push_back(std::move(retrieved_entry));
                    }else{// new page
                        tmp_db_bg_read_page_cnt ++;
                        
                        if(page_logs->size() > 0){
                            log_apply_do_apply(last_page_address, page_logs.get());
                        }

                        last_page_address = page_address;
                        page_logs->clear();
                        rocksdb::Slice value_slice = it->value();
                        const byte* bytes = reinterpret_cast<const byte*>(value_slice.data());
                        size_t size = value_slice.size();
                        LogEntry retrieved_entry = LogEntry::deserialize(bytes, size);
                        total_apply_len += retrieved_entry.log_len_;
                        page_logs->push_back(std::move(retrieved_entry));
                    }
                }

                // Handle the last page logs if any
                if (!page_logs->empty()) {
                    log_apply_do_apply(last_page_address, page_logs.get());
                    page_logs->clear();
                }

                if (!it->status().ok()) {
                    LogEvent(COMPONENT_FSAL, "Error during iteration: %s", it->status().ToString().c_str());
                }
                it.reset();

                status = db->DeleteFile(sst_file.file_name);

                if (!status.ok()) {//== todo: the selected SST must be in current max level?
                    LogEvent(COMPONENT_FSAL, "Error deleting SST file: %s", status.ToString().c_str());
                } else {
                    LogEvent(COMPONENT_FSAL, "deleted SST file %s successfully, level %d", sst_file.file_name.c_str(), sst_file.level);
                }

                PTHREAD_MUTEX_lock(&(db_bg_apply_metric_mutex));
                db_bg_read_page_cnt += tmp_db_bg_read_page_cnt;
                db_bg_read_SST_cnt ++;
                db_read_amplification_by_page = db_bg_read_page_cnt / db_bg_read_SST_cnt;//== read amplification = page_num / SST_num
                PTHREAD_MUTEX_unlock(&(db_bg_apply_metric_mutex));

                PTHREAD_MUTEX_lock(&(log_group_mutex));
                // log_group.applied_isn += total_apply_len;// log_group.applied_isn is not used
                log_group.written_capacity += total_apply_len;
                // 唤醒log writer
                pthread_cond_signal(&log_write_condition);
                PTHREAD_MUTEX_unlock(&(log_group_mutex));
            }
        }
    }
    // pthread_mutex_unlock(&db_mutex);
    auto db_end_time = std::chrono::steady_clock::now();
    auto db_duration_micros = std::chrono::duration_cast<std::chrono::microseconds>(db_end_time - db_start_time);
    db_bg_read_duration_micros += db_duration_micros;
    
    
    if(db_fg_read_page_cnt>0){
            LogEvent(COMPONENT_FSAL, "db foreground read page IO %ld us, total fg read %ld us, cnt = %ld, average fg read %.2f us",
                db_duration_micros.count(), db_fg_read_duration_micros.count(), db_fg_read_page_cnt, double(db_fg_read_duration_micros.count())/db_fg_read_page_cnt);
        }
    if(db_fg_read_log_cnt>0){
        LogEvent(COMPONENT_FSAL, "db foreground read log, total fg read %ld us, cnt = %ld, average fg read %.2f us",
            db_fg_read_duration_micros.count(), db_fg_read_log_cnt, double(db_fg_read_duration_micros.count())/db_fg_read_log_cnt);
    }
    if(db_write_cnt>0){
        LogEvent(COMPONENT_FSAL, "rocksdb insert IO, total %ld us, cnt =%ld, average %.2f us",
            db_write_duration_micros.count(), db_write_cnt, double(db_write_duration_micros.count())/db_write_cnt);
    }
    if(db_bg_read_SST_cnt>0){
        LogEvent(COMPONENT_FSAL, "db background read SST, total bg read %ld us, cnt = %ld, average bg read %.2f us",
            db_bg_read_duration_micros.count(), db_bg_read_SST_cnt, double(db_bg_read_duration_micros.count())/db_bg_read_SST_cnt);
    }
    if(db_bg_read_log_cnt>0){
        LogEvent(COMPONENT_FSAL, "db background read log, total bg read %ld us, cnt = %ld, average bg read %.2f us",
            db_bg_read_duration_micros.count(), db_bg_read_log_cnt, double(db_bg_read_duration_micros.count())/db_bg_read_log_cnt);
    }
    if(db_bg_read_page_cnt>0){
        LogEvent(COMPONENT_FSAL, "db background read page, total bg read %ld us, cnt = %ld, average bg read %.2f us",
            db_duration_micros.count(), db_bg_read_duration_micros.count(), db_bg_read_page_cnt, double(db_bg_read_duration_micros.count())/db_bg_read_page_cnt);
        LogEvent(COMPONENT_FSAL, "db background read amplification = page_cnt / SST_cnt = %d",
            db_read_amplification_by_page);
    }


    // 放掉lock
    // log_appliers[worker_index].logs.clear();
    log_appliers[worker_index].need_process = false;
    log_appliers[worker_index].is_running = false;
    log_appliers[worker_index].SST_files.clear();
    PTHREAD_MUTEX_unlock(&(log_appliers[worker_index].mutex));
}

static void *log_apply_worker_routine(void *worker_index) {
    int *index_ptr = static_cast<int *>(worker_index);
    int index = *(index_ptr);
    delete index_ptr;
    for (;;) {
        log_apply_worker_work(index);
    }
}

// 从index上摘下task请求，并且等待所有log worker变成空闲状态
static std::vector<PageAddress> log_apply_scheduler_acquire(size_t *total_log_len) {
//    PTHREAD_MUTEX_lock(&log_apply_task_mutex);
//    while (apply_task_requests.empty()) {
//        pthread_cond_wait(&log_apply_condition, &log_apply_task_mutex);
//    }
//    auto task = std::move(apply_task_requests.front());
//    apply_task_requests.erase(apply_task_requests.begin());
//    PTHREAD_MUTEX_unlock(&log_apply_task_mutex);

    // 自旋等待所有log apply worker变成空闲状态
    while (!log_apply_all_idle());

    return apply_index.ExtractFrontHint(total_log_len);
}

static std::vector<SSTFileMeta> log_apply_scheduler_acquire_SST(size_t *sst_num) {
    // 自旋等待所有log apply worker变成空闲状态
    while (!log_apply_all_idle());

    // read SST file
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    int max_level = -1;
    for (const auto& file_meta : metadata) {
        max_level = std::max(max_level, file_meta.level);
    }

    if(max_level == -1){
        *sst_num = 0;
        return std::vector<SSTFileMeta>();
    }
    
    std::vector<rocksdb::LiveFileMetaData> max_level_files;
    for (const auto& file_meta : metadata) {
        if (file_meta.level == max_level) {
            max_level_files.push_back(file_meta);
        }
    }
    // 对 max_level 的文件按 smallestkey 进行排序
    std::sort(max_level_files.begin(), max_level_files.end(), [](const rocksdb::LiveFileMetaData& a, const rocksdb::LiveFileMetaData& b) {
        return a.smallestkey < b.smallestkey;
    });

    // bg apply condition: 1. read amplification > threshold 2.max_level is not empty (for first triggering) 
    if(max_level >= ROCKSDB_FIRST_TRIGGER_LEVEL){
        if(db_read_amplification_by_page ==0){
            *sst_num = 1;
        }else{
                *sst_num = std::min(db_read_amplification_by_page / READ_AMPLIFICATION_THRESHOLD, 
                            max_level_files.size());
        }
    }
    
    std::vector<SSTFileMeta> sst_files;
    for(int i=0;i<*sst_num;i++){
        const auto& file_meta = max_level_files[i];
        LogEvent(COMPONENT_FSAL, "select SST file: %s, level: %d, smallest key: %s", file_meta.name.c_str(), file_meta.level, file_meta.smallestkey.c_str());

        std::string max_level_file_path = file_meta.directory + file_meta.name;
        std::string max_level_file_name = file_meta.name;
        std::string min_key = file_meta.smallestkey;
        std::string max_key = file_meta.largestkey;
        uint64_t file_size = file_meta.size;
        uint64_t level = file_meta.level;
        
        SSTFileMeta sst_file_meta(max_level_file_name, max_level_file_path, level, file_size, min_key, max_key);
        sst_files.push_back(sst_file_meta);
    }

    return sst_files;
}

static void *log_apply_scheduler_routine_new(void *scheduler_index) {
    int *index_ptr = static_cast<int *>(scheduler_index);
    int index = *(index_ptr);
    delete index_ptr;
    for (;;) {

        size_t sst_num_to_apply = 0;
        std::vector<SSTFileMeta> sst_files = log_apply_scheduler_acquire_SST(&sst_num_to_apply);

        if(sst_num_to_apply == 0){
            sleep(ROCKSDB_MONITOR_INTERVAL);// sleep 20 seconds, then check again 
            continue;
        }

        LogEvent(COMPONENT_FSAL, "bg log applier starting apply %zu SST", sst_num_to_apply);

        // 把每一个SST file分配给相应的worker
        size_t next_applier = 0;
        for (const auto &item: sst_files) {
            log_appliers[next_applier].SST_files.push_back(item);
            // next_applier = (next_applier + 1) % log_appliers.size();//== todo: ensure SST order among workers
        }

        // 唤醒相应的worker
        for (auto & log_applier : log_appliers) {
            PTHREAD_MUTEX_lock(&(log_applier.mutex));
            log_applier.need_process = true;
            pthread_cond_signal(&(log_applier.need_process_cond));
            PTHREAD_MUTEX_unlock(&(log_applier.mutex));
        }

        // 自己变成worker进行工作
        LogEvent(COMPONENT_FSAL, "log apply scheduler turn into worker");
        log_apply_worker_work(index);

        // 自旋等待所有log worker变成空闲状态
        while (!log_apply_all_idle());
    }
}

static void *log_apply_scheduler_routine(void *scheduler_index) {
    int *index_ptr = static_cast<int *>(scheduler_index);
    int index = *(index_ptr);
    delete index_ptr;
    for (;;) {

        size_t total_log_len = 0;
        auto task = log_apply_scheduler_acquire(&total_log_len);

        // LogEvent(COMPONENT_FSAL, "log applier starting apply %zu bytes log", total_log_len);
        auto need_to_apply = total_log_len;

        // 把每一个task分配给相应的worker
        size_t next_applier = 0;
        for (const auto &item: task) {
            log_appliers[next_applier].logs.push_back(item);
            next_applier = (next_applier + 1) % log_appliers.size();
        }

        // 唤醒相应的worker
        for (auto & log_applier : log_appliers) {

            PTHREAD_MUTEX_lock(&(log_applier.mutex));
            log_applier.need_process = true;
            pthread_cond_signal(&(log_applier.need_process_cond));
            PTHREAD_MUTEX_unlock(&(log_applier.mutex));

        }

        // 自己变成worker进行工作
        LogEvent(COMPONENT_FSAL, "log apply scheduler turn into worker");
        log_apply_worker_work(index);

        LogEvent(COMPONENT_FSAL, "applied %zu bytes log", need_to_apply);
        // 自旋等待所有log worker变成空闲状态
        while (!log_apply_all_idle());

        PTHREAD_MUTEX_lock(&(log_group_mutex));

        log_group.applied_isn += need_to_apply;
        log_group.written_capacity += need_to_apply;

        // 唤醒log writer
        pthread_cond_signal(&log_write_condition);

        PTHREAD_MUTEX_unlock(&(log_group_mutex));
    }
}

void log_apply_thread_start(int n_thread) {
    assert(APPLIER_THREAD >= 1); // 最少要有一个log apply线程
    assert(log_appliers.size() == APPLIER_THREAD);

    // 首先启动scheduler
    int *scheduler_index = new int(0);
    // START_THREAD("log apply scheduler 0", &log_appliers[0].thread_id, log_apply_scheduler_routine, (void *)(scheduler_index));
    START_THREAD("log apply scheduler 0", &log_appliers[0].thread_id, log_apply_scheduler_routine_new, (void *)(scheduler_index));
    n_thread -= 1;

    // 启动剩下的apply worker
    for (int i = 0; i < n_thread; ++i) {
        std::string thread_name = "log apply worker " + std::to_string(i+1);
        thread_name += std::to_string(i);
        int *work_index = new int(i);
        START_THREAD(thread_name.c_str(), &log_appliers[i].thread_id, log_apply_worker_routine, (void *)(work_index));
        n_thread--;
    }
}
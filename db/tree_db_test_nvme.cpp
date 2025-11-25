#define GLOBAL_VALUE_DEFINE
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <signal.h>

#include <boost/filesystem.hpp>
#include <jemalloc/jemalloc.h>

#include "chunk/XORChunk.hpp"
#include "db/version_set.h"
#include "head/Head.hpp"
#include "head/HeadAppender.hpp"
#include "head/MemSeries.hpp"
#include "label/EqualMatcher.hpp"
#include "label/Label.hpp"
#include "leveldb/cache.h"
#include "port/port.h"
#include "querier/tsdb_querier.h"
#include "third_party/rapidjson/document.h"
#include "third_party/rapidjson/rapidjson.h"
#include "third_party/rapidjson/stringbuffer.h"
#include "third_party/rapidjson/writer.h"
#include "third_party/thread_pool.h"

namespace tsdb {
    namespace db {

        std::vector<std::vector<std::string>> devops(
                {{"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
                         "usage_irq", "usage_softirq", "usage_steal", "usage_guest",
                         "usage_guest_nice"},
                 {"reads", "writes", "read_bytes", "write_bytes", "read_time", "write_time",
                         "io_time"},
                 {"total", "free", "used", "used_percent", "inodes_total", "inodes_free",
                         "inodes_used"},
                 {"boot_time", "interrupts", "context_switches", "processes_forked",
                         "disk_pages_in", "disk_pages_out"},
                 {"total", "available", "used", "free", "cached", "buffered",
                         "used_percent", "available_percent", "buffered_percent"},
                 {"bytes_sent", "bytes_recv", "packets_sent", "packets_recv", "err_in",
                         "err_out", "drop_in", "drop_out"},
                 {"accepts", "active", "handled", "reading", "requests", "waiting",
                         "writing"},
                 {"numbackends", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
                         "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
                         "tup_deleted", "conflicts", "temp_files", "temp_bytes", "deadlocks",
                         "blk_read_time", "blk_write_time"},
                 {"uptime_in_seconds",
                         "total_connections_received",
                                                "expired_keys",
                         "evicted_keys",
                         "keyspace_hits",
                         "keyspace_misses",
                         "instantaneous_ops_per_sec",
                         "instantaneous_input_kbps",
                         "instantaneous_output_kbps",
                         "connected_clients",
                                        "used_memory",
                                                     "used_memory_rss",
                                                                   "used_memory_peak",
                                                                                 "used_memory_lua",
                         "rdb_changes_since_last_save",
                                          "sync_full",
                         "sync_partial_ok",
                         "sync_partial_err",
                         "pubsub_channels",
                         "pubsub_patterns",
                         "latest_fork_usec",
                         "connected_slaves",
                         "master_repl_offset",
                         "repl_backlog_active",
                         "repl_backlog_size",
                         "repl_backlog_histlen",
                         "mem_fragmentation_ratio",
                         "used_cpu_sys",
                         "used_cpu_user",
                         "used_cpu_sys_children",
                         "used_cpu_user_children"}});
        std::vector<std::string> devops_names({"cpu_", "diskio_", "disk_", "kernel_",
                                               "mem_", "net_", "nginx_", "postgres_",
                                               "redis_"});

        std::unordered_map<std::string, bool> query_types({{"1-1-1", true},
                                                           {"1-1-12", false},
                                                           {"1-1-24", true},
                                                           {"1-1-all", false},
                                                           {"1-8-1", true},
                                                           {"5-1-1", true},
                                                           {"5-1-12", false},
                                                           {"5-1-24", true},
                                                           {"5-1-all", false},
                                                           {"5-8-1", true},
                                                           {"cpu-max-all-8", true},
                                                           {"double-groupby-5", true},
                                                           {"double-groupby-all", true},
                                                           {"high-cpu-1", false},
                                                           {"high-cpu-all", false},
                                                           {"lastpoint", true}});

        class TSDBTest {
        public:
            TSDBTest() {
                for (int i = 0; i < 50; i++)
                    matchers1.emplace_back("hostname", "host_" + std::to_string(i));
                matchers2 = std::vector<tsdb::label::EqualMatcher>(
                        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
                         tsdb::label::EqualMatcher("__name__", "diskio_reads"),
                         tsdb::label::EqualMatcher("__name__", "kernel_boot_time"),
                         tsdb::label::EqualMatcher("__name__", "mem_total"),
                         tsdb::label::EqualMatcher("__name__", "net_bytes_sent")});
                matchers3 = std::vector<tsdb::label::EqualMatcher>(
                        {tsdb::label::EqualMatcher("__name__", "cpu_usage_user"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_system"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_idle"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_nice"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_iowait"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_irq"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_softirq"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_steal"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest"),
                         tsdb::label::EqualMatcher("__name__", "cpu_usage_guest_nice")});
            }

            void set_parameters(int num_ts_, int tuple_size_, int num_tuple_) {
                num_ts = num_ts_;
                tuple_size = tuple_size_;
                num_tuple = num_tuple_;
                head::MEM_TUPLE_SIZE = tuple_size_;
                for (int i = 50; i < num_ts/100; i++)
                    matchers1.emplace_back("hostname", "host_" + std::to_string(i));
            }


            int load_devops(int num_lines, const std::string& name, int64_t st,
                            int64_t interval, int ts_counter) {
                std::ifstream file(name);
                std::string line;
                int cur_line = 0;

                std::vector<int64_t> tfluc;
                for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                std::vector<std::string> items, names, values;
                for (size_t round = 0; round < devops_names.size(); round++) {
                    while (cur_line < num_lines) {
                        getline(file, line);

                        size_t pos_start = 0, pos_end, delim_len = 1;
                        std::string token;
                        items.clear();
                        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
                            token = line.substr(pos_start, pos_end - pos_start);
                            pos_start = pos_end + delim_len;
                            items.push_back(token);
                        }
                        items.push_back(line.substr(pos_start));

                        names.clear();
                        values.clear();
                        for (size_t i = 1; i < items.size(); i++) {
                            pos_end = items[i].find("=");
                            names.push_back(items[i].substr(0, pos_end));
                            values.push_back(items[i].substr(pos_end + 1));
                        }

                        for (size_t i = 0; i < devops[round].size(); i++) {
                            auto app = head_->appender();
                            tsdb::label::Labels lset;
                            for (size_t j = 0; j < names.size(); j++)
                                lset.emplace_back(names[j], values[j]);
                            lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
                            std::sort(lset.begin(), lset.end());

                            auto r = app->add(lset, tfluc[0], tfluc[0]);
                            if (r.first != ((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                                           (uint64_t)(ts_counter % PRESERVED_BLOCKS)) {
                                std::cout << tsdb::label::lbs_string(lset) << std::endl;
                                std::cout << "TSDBTest::load_devops wrong id exp:" << ts_counter
                                          << " got:" << r.first << std::endl;
                            }

                            for (int k = 1; k < tuple_size; k++)
                                app->add_fast(((uint64_t)(ts_counter / PRESERVED_BLOCKS) << 32) +
                                              (uint64_t)(ts_counter % PRESERVED_BLOCKS),
                                              st + k * interval * 1000 + tfluc[k], tfluc[k]);
                            app->commit();

                            ts_counter++;
                        }
                        cur_line++;
                    }
                    for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
                    cur_line = 0;
                }
                return ts_counter;
            }

            // Devops labels.
            void head_add3(int64_t st, int64_t interval) {
                int num_lines = num_ts / 100;
                int tscounter;
                if (num_lines > 100000) {
                    tscounter =
                            load_devops(100000, "../test/devops100000.txt", st, interval, 0);
                    tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                                            st, interval, tscounter);
                } else
                    tscounter =
                            load_devops(num_lines, "../test/devops100000.txt", st, interval, 0);
                std::cout << "head_add3: " << tscounter << std::endl;
            }

            int load_devops(int num_lines, const std::string& name,
                            std::vector<label::Labels>* lsets, int ts_counter) {
                std::ifstream file(name);
                std::string line;
                int cur_line = 0;

                std::vector<std::string> items, names, values;
                for (size_t round = 0; round < devops_names.size(); round++) {
                    while (cur_line < num_lines) {
                        getline(file, line);

                        size_t pos_start = 0, pos_end, delim_len = 1;
                        std::string token;
                        items.clear();
                        while ((pos_end = line.find(",", pos_start)) != std::string::npos) {
                            token = line.substr(pos_start, pos_end - pos_start);
                            pos_start = pos_end + delim_len;
                            items.push_back(token);
                        }
                        items.push_back(line.substr(pos_start));

                        names.clear();
                        values.clear();
                        for (size_t i = 1; i < items.size(); i++) {
                            pos_end = items[i].find("=");
                            names.push_back(items[i].substr(0, pos_end));
                            values.push_back(items[i].substr(pos_end + 1));
                        }

                        for (size_t i = 0; i < devops[round].size(); i++) {
                            tsdb::label::Labels lset;
                            for (size_t j = 0; j < names.size(); j++)
                                lset.emplace_back(names[j], values[j]);
                            lset.emplace_back("__name__", devops_names[round] + devops[round][i]);
                            std::sort(lset.begin(), lset.end());

                            lsets->push_back(std::move(lset));

                            ts_counter++;
                        }
                        cur_line++;
                    }
                    for (int i = 0; i < 100000 - cur_line; i++) getline(file, line);
                    cur_line = 0;
                }
                return ts_counter;
            }

            void get_devops_labels(std::vector<label::Labels>* lsets) {
                int num_lines = num_ts / 100;
                int tscounter;
                if (num_lines > 100000) {
                    tscounter = load_devops(100000, "../test/devops100000.txt", lsets, 0);
                    tscounter = load_devops(num_lines - 100000, "../test/devops100000-2.txt",
                                            lsets, tscounter);
                } else
                    tscounter = load_devops(num_lines, "../test/devops100000.txt", lsets, 0);
                std::cout << "get_devops_labels: " << tscounter << std::endl;
            }

            void head_add_fast1(int64_t st, int64_t interval) {
                // auto app = head_->TEST_appender();
                Timer t;
                // int count = 0;
                int64_t d, last_t;
                t.start();
                auto app = head_->appender();
                std::vector<int64_t> tfluc;
                for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                for (int i = 0; i < num_ts; i++) {
                    for (int k = 0; k < tuple_size; k++)
                        app->add_fast(((uint64_t)(i / PRESERVED_BLOCKS) << 32) +
                                      (uint64_t)(i % PRESERVED_BLOCKS),
                                      st + k * interval * 1000 + tfluc[k], tfluc[k]);
                    app->commit();
                }

            }

            leveldb::Status setup(const std::string& dbpath, const std::string& log_path) {
                //=================TreeSeries==========
                std::string  path = "/mnt/nvme/tree_series/tree_series_test";
                int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
                slab::Setting *setting = new slab::Setting();
                setting->ssd_device_ = "/mnt/nvme/tree_series/tree_series_test";
                std::string info_path = "/mnt/nvme/tree_series/tree_series_info_test";
                int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
                setting->ssd_slab_info_ = "/mnt/nvme/tree_series/tree_series_info_test";
                tree_series_ = new slab::TreeSeries(*setting);

                //==========LevelDB============
//                std::string dbpath = "/tmp/tsdb_big";
//                std::string dbpath = "/mnt/HDD/tree_head_test";

                boost::filesystem::remove_all(dbpath);
                boost::filesystem::remove_all(log_path);

                leveldb::Options options;
                options.create_if_missing = true;
                options.max_imm_num = 3;
                options.write_buffer_size = 256 * 1024 * 1024;
                options.max_file_size = 256 * 1024 * 1024;
                options.use_log = false;
                leveldb::Status st = leveldb::DB::Open(options, dbpath, &db_);
                if (!st.ok())return st;

                boost::filesystem::remove_all(dbpath);
                head_ = new head::TreeHead(dbpath, log_path,"",db_,tree_series_);
                db_->SetTreeHead(head_);
                return st;
            }

            int num_ts;
            int tuple_size;
            int num_tuple;
//            std::unique_ptr<::tsdb::head::Head> head_;

            slab::TreeSeries* tree_series_;
            head::TreeHead* head_;
            leveldb::DB* db_;

            std::vector<tsdb::label::EqualMatcher> matchers1;
            std::vector<tsdb::label::EqualMatcher> matchers2;
            std::vector<tsdb::label::EqualMatcher> matchers3;
        };

        void mem_usage(double& vm_usage, double& resident_set) {
            vm_usage = 0.0;
            resident_set = 0.0;
            std::ifstream stat_stream("/proc/self/stat",
                                      std::ios_base::in);  // get info from proc directory
            // create some variables to get info
            std::string pid, comm, state, ppid, pgrp, session, tty_nr;
            std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
            std::string utime, stime, cutime, cstime, priority, nice;
            std::string O, itrealvalue, starttime;
            unsigned long vsize;
            long rss;
            stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr >>
                        tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt >> utime >>
                        stime >> cutime >> cstime >> priority >> nice >> O >> itrealvalue >>
                        starttime >> vsize >> rss;  // don't care about the rest
            stat_stream.close();
            long page_size_kb = sysconf(_SC_PAGE_SIZE) /
                                1024;  // for x86-64 is configured to use 2MB pages
            vm_usage = vsize / 1024.0;
            resident_set = rss * page_size_kb;
        }

        void TestParallelInsertAndQuery(int thread_num, int num, int interval = 30,
                                         int numtuple = 90) {
            printf("TestParallelInsertAndQuery200\n");

            bool bg = true;
            size_t sz = sizeof(bg);
            mallctl("opt.background_thread", NULL, 0, &bg, sz);
            ssize_t t = 0;
            sz = sizeof(t);
            mallctl("opt.dirty_decay_ms", NULL, 0, &t, sz);
            mallctl("opt.muzzy_decay_ms", NULL, 0, &t, sz);

            std::string dbpath = "/mnt/nvme/tsdb_big";
            std::string log_path = dbpath;

            TSDBTest tsdbtest;
            tsdbtest.set_parameters(num, 32, numtuple);
            tsdbtest.setup(dbpath, log_path);

            Thread_Pool pool(thread_num);
            base::WaitGroup wg;
            std::vector<std::unique_ptr<db::AppenderInterface>> apps;
            apps.reserve(thread_num);

            std::vector<label::Labels> lsets;
            lsets.reserve(num);
            tsdbtest.get_devops_labels(&lsets);

            tsdbtest.head_->enable_concurrency();

            tsdbtest.head_->bg_flush_data();

            leveldb::Options ms_opts;
            ms_opts.create_if_missing = true;
            ms_opts.use_log = false;
            tsdbtest.head_->set_mergeset_manager(dbpath + "/mergeset", ms_opts);

            int64_t last_t = 0, d;
            double vm, rss;
            mem_usage(vm, rss);
            std::cout << "Virtual Memory: " << (vm / 1024)
                      << "MB\nResident set size: " << (rss / 1024) << "MB\n"
                      << std::endl;
            Timer timer;
            timer.start();

            auto func = [](db::AppenderInterface* app,
                           std::vector<tsdb::label::Labels>* _lsets, int left, int right,
                           base::WaitGroup* _wg, uint64_t num_tuple, uint64_t tuple_size,
                           uint64_t interval, Timer* timer, int tid) {
                std::vector<uint64_t> tsids;
                std::vector<uint64_t> sgids;
                std::vector<uint16_t >mids;
                tsids.reserve(right - left);

                std::vector<int64_t> tfluc;
                for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                uint64_t sgid;
                uint16_t mid;
                for (int i = left; i < right; i++) {
                    auto r = app->add(std::move(_lsets->at(i)), tfluc[0], tfluc[0],sgid,mid);
                    for (int k = 1; k < tuple_size; k++)
                        app->add_fast(sgid,mid, k * interval * 1000 + tfluc[k], tfluc[k]);
                    app->commit();
                    sgids.push_back(sgid);
                    mids.push_back(mid);
                }

                int64_t d = timer->since_start_nano(), last_t = 0;
                printf(
                        "[thread id]:%d [Labels Insertion duration (ms)]:%ld [throughput]:%f\n",
                        tid, d / 1000000,
                        (double)(right - left) * (double)(tuple_size) / (double)(d)*1000000000);

                for (uint64_t tuple = 1; tuple < num_tuple; tuple++) {
                    tfluc.clear();
                    for (int j = 0; j < tuple_size; j++) tfluc.push_back(rand() % 200);
                    int64_t st = tuple * tuple_size * 1000 * interval;
                    for (int i = 0; i < right - left; i++) {
                        for (int k = 0; k < tuple_size; k++)
                            app->add_fast(sgids[i],mids[i], st + k * interval * 1000 + tfluc[k],
                                          tfluc[k]);
                        app->commit();
                    }

                    if ((tuple + 1) % 15 == 0) {
                        d = timer->since_start_nano();
                        printf(
                                "[thread id]:%d [#tuples]:%d [st]:%d [Insertion duration (ms)]:%ld "
                                "[throughput]:%f\n",
                                tid, tuple + 1, tuple * tuple_size * 1000, (d - last_t) / 1000000,
                                (double)(right - left) * (double)(15) * (double)(tuple_size) /
                                (double)(d - last_t) * 1000000000);
                        last_t = d;
                    }
                }

                _wg->done();
            };

            for (int i = 0; i < thread_num; i++) {
                wg.add(1);
                apps.push_back(std::move(tsdbtest.head_->appender()));
                pool.enqueue(std::bind(
                        func, apps.back().get(), &lsets, i * lsets.size() / thread_num,
                        (i + 1) * lsets.size() / thread_num, &wg, tsdbtest.num_tuple,
                        tsdbtest.tuple_size, interval, &timer, i));
            }
            wg.wait();

            d = timer.since_start_nano();
            std::cout << "[TestParallelInsertAndQuery:Total Insertion duration (ms)]:" << (d / 1000000)
                      << " [#TS]:" << tsdbtest.num_ts << " [#samples]:"
                      << (double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                         (double)(tsdbtest.tuple_size)
                      << " [throughput]:"
                      << ((double)(tsdbtest.num_ts) * (double)(tsdbtest.num_tuple) *
                          (double)(tsdbtest.tuple_size) / (double)(d)*1000000000)
                      << std::endl;

//            sleep(60);
//            tsdbtest.head_->set_mem_to_disk_migration_threshold(2lu * 1024lu * 1024lu *
//                                                                1024lu);
//            tsdbtest.head_->enable_migration();
//            sleep(60);
//            tsdbtest.head_->disable_migration();
            // tsdbtest.head_->full_migrate();
            sleep(60);
//            tsdbtest.db_->PrintLevel();

            tsdbtest.head_->stop_bg_flush_data();

            double vm2, rss2;
            mem_usage(vm2, rss2);
            std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
                      << std::endl;
            std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
                      << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
            std::cout << "inverted index size:"
                      << tsdbtest.head_->inverted_index()->mem_postings()->mem_size()
                      << std::endl;

            uint64_t epoch = 1;
            sz = sizeof(epoch);
            mallctl("epoch", &epoch, &sz, &epoch, sz);

            size_t allocated, active, mapped;
            sz = sizeof(size_t);
            mallctl("stats.allocated", &allocated, &sz, NULL, 0);
            mallctl("stats.active", &active, &sz, NULL, 0);
            mallctl("stats.mapped", &mapped, &sz, NULL, 0);
            printf("allocated/active/mapped (MB): %zu/%zu/%zu\n", allocated/1024/1024, active/1024/1024, mapped/1024/1024);

            // todo query
        }

    }  // namespace db
}  // namespace tsdb

int main(int argc, char* argv[]) {
    if (argc == 5){
        tsdb::db::TestParallelInsertAndQuery(
                std::stoi(argv[1]), std::stoi(argv[2]), std::stoi(argv[3]),
                std::stoi(argv[4]));
    }
}

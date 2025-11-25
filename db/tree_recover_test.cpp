#define GLOBAL_VALUE_DEFINE
#include "head/TreeHead.h"
#include "testutil/label_generator.h"
#include "leveldb/third_party/thread_pool.h"

namespace tsdb::head {
    class RecoverTest : public testing::Test {
    public:
        leveldb::Status setup() {
//            //=================TreeSeries==========
//            std::string  path = "/home/dell/project/SSD/parallel_wal_test";
//            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
//            slab::Setting *setting = new slab::Setting();
//            setting->ssd_device_ = "/home/dell/project/SSD/parallel_wal_test";
//            std::string info_path = "/home/dell/project/SSD/parallel_wal_info_test";
//            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
//            setting->ssd_slab_info_ = "/home/dell/project/SSD/parallel_wal_info_test";
//            tree_series_ = new slab::TreeSeries(*setting);

            init_tree_series();

            //==========LevelDB============
            std::string hd_path = "/tmp/parallel_wal_test";
            hd_path_ = hd_path;
//            leveldb::Options options;
//            options.create_if_missing = true;
//            options.max_imm_num = 3;
//            options.write_buffer_size = 256 * 1024 * 1024;
//            options.max_file_size = 256 * 1024 * 1024;
//            options.use_log = false;
//            leveldb::Status st = leveldb::DB::Open(options, hd_path_, &db_);
//            if (!st.ok())return st;

            leveldb::Status st = init_db();

            boost::filesystem::remove_all(hd_path_);
//            hd_ = new head::TreeHead(hd_path_,"",db_,tree_series_);
//            db_->SetTreeHead(hd_);

            init_tree_head();

            return st;
        }

        void init_tree_series() {
            //=================TreeSeries==========
            std::string  path = "/home/dell/project/SSD/parallel_wal_test";
            int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
            slab::Setting *setting = new slab::Setting();
            setting->ssd_device_ = "/home/dell/project/SSD/parallel_wal_test";
            std::string info_path = "/home/dell/project/SSD/parallel_wal_info_test";
            int info_fd = ::open(info_path.c_str(), O_WRONLY | O_CREAT, 0644);
            setting->ssd_slab_info_ = "/home/dell/project/SSD/parallel_wal_info_test";
            tree_series_ = new slab::TreeSeries(*setting);
        }

        leveldb::Status init_db() {
            leveldb::Options options;
            options.create_if_missing = true;
            options.max_imm_num = 3;
            options.write_buffer_size = 256 * 1024 * 1024;
            options.max_file_size = 256 * 1024 * 1024;
            options.use_log = false;
            leveldb::Status st = leveldb::DB::Open(options, hd_path_, &db_);
            return st;
        }

        void init_tree_head() {
            hd_ = new head::TreeHead(hd_path_,"",db_,tree_series_);
            db_->SetTreeHead(hd_);
        }

        void load_labels(uint64_t num_ts, std::vector<tsdb::label::Labels>* lsets) {
            testutil::get_devops_labels(num_ts, lsets);
        }

        void insert_samples(uint64_t num_ts, uint64_t num_samples) {
            std::vector<label::Labels> lsets;
            lsets.reserve(num_ts);
            load_labels(num_ts, &lsets);
            auto app = hd_->appender();
            for (uint64_t i = 0; i < num_ts; i++) {
                uint64_t sgid;
                uint16_t mid;
                auto r = app->add(lsets[i], 1, 1, sgid, mid);
                for (uint64_t j = 0; j < num_samples; j++) {
                    int64_t t = (j+1) * 1000;
                    double v = (j+1) * 100;
                    leveldb::Status s = app->add_fast(sgid, mid, t, v);
                    ASSERT_TRUE(s.ok());
                }
                leveldb::Status s = app->commit();
                ASSERT_TRUE(s.ok());
            }
        }

        void insert_samples_parallel(std::vector<label::Labels>& lsets, uint64_t num_ts, uint64_t num_samples, uint64_t num_thread) {
//            std::vector<label::Labels> lsets;
//            lsets.reserve(num_ts);
//            load_labels(num_ts, &lsets);
            auto func = [](head::TreeHead* _h, std::vector<label::Labels>& lsets, uint64_t left, uint64_t right, uint64_t num_samples, base::WaitGroup* _wg) {
                auto app = _h->appender();
                for (uint64_t i = left; i < right; i++) {
                    MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16+i);
                    uint64_t sgid;
                    uint16_t mid;
                    auto r = app->add(lsets[i], 1, 1, sgid, mid);
                    for (uint64_t j = 0; j < num_samples; j++) {
                        int64_t t = (j+1) * 1000;
                        double v = (j+1) * 100;
                        app->add_fast(sgid, mid, t, v);
                    }
                    app->commit();
                }
                _wg->done();
            };
            ThreadPool pool(num_thread);
            base::WaitGroup wg;
            for (uint64_t i = 0; i < num_thread; i++) {
                wg.add(1);
                pool.enqueue(std::bind(func, hd_, lsets, i*num_ts/num_thread, (i+1)*num_ts/num_thread, num_samples, &wg));
            }
            wg.wait();
        }

        ~RecoverTest() {
            delete db_;
            delete tree_series_;
            delete hd_;
        }

        slab::TreeSeries* tree_series_;
        leveldb::DB* db_;
        head::TreeHead* hd_;
        std::string hd_path_;
    };

    TEST_F(RecoverTest, RecoverTest) {
        ASSERT_TRUE(setup().ok());

        hd_->bg_flush_data();
        hd_->bg_stop_clean_samples_logs();

        uint64_t num_ts = 1000000;
        uint64_t num_samples = 180;
        uint64_t num_thread = 32;
        std::vector<label::Labels> lsets;
        lsets.reserve(num_ts);
        load_labels(num_ts, &lsets);
        insert_samples_parallel(lsets, num_ts, num_samples, num_thread);

        hd_->flush_wal();

        uint64_t last_sgid = hd_->get_last_source_group_id();
        uint16_t last_mid = hd_->get_last_metric_id();
        std::cout<<"last_mid: "<<last_mid<< " " <<"last_sgid: "<<last_sgid<<std::endl;

        sleep(5);

        delete tree_series_;
        delete db_;
        delete hd_;

        init_tree_series();
        leveldb::Status st = init_db();
        ASSERT_TRUE(st.ok());
        init_tree_head();
        hd_->bg_flush_data();
        hd_->bg_stop_clean_samples_logs();

        Timer timer;
        timer.start();

        hd_->recover_parallel();
//        hd_->recover_serial();

        int64_t duration = timer.since_start_nano();
        printf("series num: %ld,\t samples num: %ld,\t parallel recover duration (ms) : %lf\n", num_ts, num_samples,  double(duration) / double(1000000));

        ASSERT_EQ(last_sgid, hd_->get_last_source_group_id());
        ASSERT_EQ(last_mid, hd_->get_last_metric_id());
        for (uint64_t i = 1; i <= last_sgid; i++) {
            for (uint16_t j = 1; j <= last_mid; j++) {
                auto tms = hd_->get_from_forward_index(i, j);
                ASSERT_NE(tms, nullptr);
//                std::cout<<"mid: "<<tms->metric_id_<<" sgid: "<<tms->source_id_<<" num samples: "<<tms->sample_txn_-1<<std::endl;
//                ASSERT_EQ(tms->max_time_, num_samples*1000);
            }
        }
        printf("series num: %ld,\t samples num: %ld,\t parallel recover duration (ms) : %lf\n", num_ts, num_samples,  double(duration) / double(1000000));
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
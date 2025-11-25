#define GLOBAL_VALUE_DEFINE
#include "db/TreeRemoteDB.h"
#include <gperftools/profiler.h>
#include <snappy.h>

#include "db/DB.hpp"
#include "db/HttpParser.hpp"
#include "label/EqualMatcher.hpp"
#include "leveldb/db.h"
#include "leveldb/third_party/thread_pool.h"
#include "third_party/httplib.h"
#include "util/testutil.h"
#include <memory>

namespace tsdb {
    namespace db {

        int _test_num_ts = 10000;

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

        class TreeRemoteDBTest : public testing::Test {
        public:
            void head_add_proto(httplib::Client* cli, int64_t st, int series_num) {
                MasstreeWrapper<slab::SlabInfo>::ti = threadinfo::make(threadinfo::TI_PROCESS, 16);
                prometheus::WriteRequest writeRequest;
                double cnt = 0;
                for (int i = 0; i < series_num; i++) {
                    prometheus::TimeSeries* timeSeries = writeRequest.add_timeseries();
                    tsdb::label::Labels lset;
                    lset.emplace_back("__name__", "metric");
                    for (int j = 0; j < 10; j++) {
                        lset.emplace_back("label_" + std::to_string(j), "value_" + std::to_string(j) + "_" + std::to_string(i));
                    }
                    lset.emplace_back("label_all", "value_all");
                    AddLabels(timeSeries, lset);
                    AddSample(timeSeries, st, cnt);
                    cnt++;
                }
                std::string data, compressData;
                writeRequest.SerializeToString(&data);
                snappy::Compress(data.data(), data.size(), &compressData);
                cli->Post("/insert", compressData, "text/plain");
            }

        };

        TEST_F(TreeRemoteDBTest, TestReomteWrite) {
            std::string dbpath = "/tmp/tsdb2";
            std::string logpath = dbpath;

            tsdb::db::TreeRemoteDB db(dbpath, logpath);
            sleep(1);

            httplib::Client cli("192.168.1.102", 9966);
            for (int i = 0; i < 1000; i++) {
                head_add_proto(&cli, i * 10, 100);
            }

            querier::TreeQuerier* q = db.querier(0, 100 * 10);
            std::vector<std::shared_ptr<tsdb::label::MatcherInterface>> matchers({
                std::shared_ptr<tsdb::label::MatcherInterface>(new tsdb::label::EqualMatcher("__name__", "metric")),
                std::shared_ptr<tsdb::label::MatcherInterface>(new tsdb::label::EqualMatcher("label_all", "value_all"))});
            std::unique_ptr<tsdb::querier::SeriesSetInterface> ss = q->select(matchers);
            uint64_t tsid = 0;
            while (ss->next()) {
                std::unique_ptr<tsdb::querier::SeriesInterface> series = ss->at();

                tsdb::label::Labels lset;
                lset.emplace_back("__name__", "metric");
                for (int j = 0; j < 10; j++) {
                    lset.emplace_back("label_" + std::to_string(j), "value_" + std::to_string(j) + "_" + std::to_string(tsid));
                }
                lset.emplace_back("label_all", "value_all");

//                ASSERT_EQ(lset, series->labels());

                std::unique_ptr<tsdb::querier::SeriesIteratorInterface> it = series->iterator();
                int i = 0;
                while (it->next()) {
                    auto p = it->at();
                    ASSERT_EQ(i * 10, p.first);
             //       ASSERT_EQ(double(tsid), p.second);
                    i++;
                }

                tsid++;
            }
            ASSERT_EQ(tsid, 100);
            delete q;
        }

        TEST_F(TreeRemoteDBTest, RemoteRead) {
            std::string dbpath = "/tmp/tsdb2";
            std::string logpath = dbpath;

            tsdb::db::TreeRemoteDB db(dbpath, logpath);
            sleep(1);

            httplib::Client cli("192.168.1.102", 9966);
            for (int i = 0; i < 1000; i++) {
                head_add_proto(&cli, i * 10, 100);
            }

            prometheus::ReadRequest readRequest;
            prometheus::Query* query = readRequest.add_queries();
            prometheus::LabelMatcher* matcher1 = query->add_matchers();
            matcher1->set_name("__name__");
            matcher1->set_value("metric");
            prometheus::LabelMatcher* matcher2 = query->add_matchers();
            matcher2->set_name("label_all");
            matcher2->set_value("value_all");
            query->set_start_timestamp_ms(0);
            query->set_end_timestamp_ms(100 * 10);
            //
            // std::cout<<readRequest.queries().size()<<std::endl;
            // std::cout<<readRequest.queries(0).matchers().size()<<std::endl;
            // std::cout<<readRequest.queries(0).matchers(1).value()<<std::endl;

            std::string request_data, compressed_request_data;
            readRequest.SerializeToString(&request_data);
            snappy::Compress(request_data.data(), request_data.size(), &compressed_request_data);
            httplib::Result result = cli.Post("/query", compressed_request_data, "text/plain");

            std::string response_data;
            prometheus::ReadResponse readResponse;
            snappy::Uncompress(result->body.data(), result->body.size(), &response_data);
            readResponse.ParseFromString(response_data);

            ASSERT_EQ(readResponse.results_size(), 1);

            uint64_t tsid = 0;
            for (auto& timeseries : readResponse.results(0).timeseries()) {
                tsdb::label::Labels lset_resp;
                tsdb::label::Labels lset_want;
                lset_want.emplace_back("__name__", "metric");
                for (int j = 0; j < 10; j++) {
                    lset_want.emplace_back("label_" + std::to_string(j), "value_" + std::to_string(j) + "_" + std::to_string(tsid));
                }
                lset_want.emplace_back("label_all", "value_all");
                for (auto& lb : timeseries.labels()) {
                    lset_resp.emplace_back(lb.name(), lb.value());
                }
                ASSERT_EQ(lset_want.size(), lset_resp.size());

                int i = 0;
                ASSERT_EQ(timeseries.samples_size(), 100);
                for (auto& sample : timeseries.samples()) {
                    ASSERT_EQ(sample.timestamp(), i * 10);
                    // ASSERT_EQ(double(tsid), sample.value());
                    i++;
                }

                tsid++;
            }
            ASSERT_EQ(tsid, 100);
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
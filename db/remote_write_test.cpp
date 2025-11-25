#define GLOBAL_VALUE_DEFINE

#include "protobuf/types.pb.h"
#include "protobuf/remote.pb.h"
#include <snappy.h>
#include "db/DB.hpp"
#include "db/RemoteDB.h"
#include "db/TreeRemoteDB.h"
#include "gtest/gtest.h"

class RemoteWriteTest : public testing::Test{};


TEST_F(RemoteWriteTest, ReceiveTest) {
    std::string dbpath = "/mnt/nvme/tsdb2";
    boost::filesystem::remove_all(dbpath);
    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 10 << 14;  // 10KB.
    options.max_file_size = 10 << 10;      // 10KB.

    leveldb::DB* ldb;
    ASSERT_TRUE(leveldb::DB::Open(options, dbpath, &ldb).ok());

//    tsdb::db::DB db(dbpath, ldb);
    tsdb::db::RemoteDB db(dbpath, ldb);
    sleep(1);

    for(;;) {
//        sleep(1);
    }

}

TEST_F(RemoteWriteTest, TreeReceiveTest) {
    std::string dbpath = "/tmp/tsdb2";
    std::string logpath = dbpath;

    tsdb::db::TreeRemoteDB db(dbpath, logpath);
    sleep(1);

    for(;;) {
//        sleep(1);
    }

}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
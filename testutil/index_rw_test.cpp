#define GLOBAL_VALUE_DEFINE

#include "label_generator.h"
#include "mem/go_art.h"
#include "third_party/thmap/thmap.h"
#include "third_party/thread_pool.h"

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

int main() {
//    uint64_t series_num = 10000000;
//    std::vector<tsdb::label::Labels> lsets;
//    lsets.reserve(series_num);
//    tsdb::testutil::get_devops_labels(series_num, &lsets);
//
////    for (auto& it : lsets) {
////        std::cout<<tsdb::label::lbs_string(it)<<std::endl;
////    }
////    std::cout<<"lsets size: "<<lsets.size()<<std::endl;
//
//    std::cout<<"\nthmap:\n"<<std::endl;
//
//    double vm, rss;
//    mem_usage(vm, rss);
//    std::cout << "Virtual Memory: " << (vm / 1024)
//              << "MB\nResident set size: " << (rss / 1024) << "MB\n"
//              << std::endl;
//
//    thmap* tmap = thmap_create(0, nullptr, 0);
//    for (uint64_t i = 0; i < lsets.size(); i++) {
//        std::string lbs = tsdb::label::lbs_string(lsets[i]);
//        uint64_t * val = new uint64_t(i);
//        thmap_put(tmap, lbs.c_str(), lbs.size(), val);
//    }
//    sleep(5);
//
//    double vm2, rss2;
//    mem_usage(vm2, rss2);
//    std::cout << "VM:" << (vm2 / 1024) << "MB RSS:" << (rss2 / 1024) << "MB"
//              << std::endl;
//    std::cout << "VM(diff):" << ((vm2 - vm) / 1024)
//              << " RSS(diff):" << ((rss2 - rss) / 1024) << std::endl;
//
//    sleep(3);
//
//    std::cout<<"\nART\n"<<std::endl;
//
//    double vm3, rss3;
//    mem_usage(vm3, rss3);
//    std::cout << "Virtual Memory: " << (vm3 / 1024)
//              << "MB\nResident set size: " << (rss3 / 1024) << "MB\n"
//              << std::endl;
//
//    tsdb::mem::art_tree art;
//    tsdb::mem::art_tree_init(&art);
//    for (uint64_t i = 0; i < lsets.size(); i++) {
//        std::string lbs = tsdb::label::lbs_string(lsets[i]);
//        uint64_t * val = new uint64_t(i);
//        tsdb::mem::art_insert(&art, reinterpret_cast<const unsigned char*>(lbs.c_str()), lbs.size(), (void*)(val));
//    }
//    sleep(5);
//
//    double vm4, rss4;
//    mem_usage(vm4, rss4);
//    std::cout << "VM:" << (vm4 / 1024) << "MB RSS:" << (rss4 / 1024) << "MB"
//              << std::endl;
//    std::cout << "VM(diff):" << ((vm4 - vm3) / 1024)
//              << " RSS(diff):" << ((rss4 - rss3) / 1024) << std::endl;



    int thread_num = 32;
    Thread_Pool pool(thread_num);

    auto thmap_insert = [](tsdb::base::WaitGroup* wg, thmap* tmap, const std::vector<tsdb::label::Labels>& lsets,
            const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            uint64_t * val = new uint64_t(i);
            thmap_put(tmap, (lstrsets)[i].c_str(), (lstrsets)[i].size(), val);
        }
        wg->done();
    };

    auto thmap_query = [](tsdb::base::WaitGroup* wg, thmap* tmap, const std::vector<tsdb::label::Labels>& lsets,
    const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            auto val = thmap_get(tmap, (lstrsets)[i].c_str(), (lstrsets)[i].size());
        }
        wg->done();
    };

    auto thmap_update = [](tsdb::base::WaitGroup* wg, thmap* tmap, const std::vector<tsdb::label::Labels>& lsets,
                           const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            uint64_t * val = new uint64_t(i+1);
            thmap_put(tmap, (lstrsets)[i].c_str(), (lstrsets)[i].size(), val);
        }
        wg->done();
    };

    auto art_insert = [](tsdb::base::WaitGroup* wg, tsdb::mem::art_tree* artTree, const std::vector<tsdb::label::Labels>& lsets,
                         const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            uint64_t * val = new uint64_t(i);
            tsdb::mem::art_insert(artTree, reinterpret_cast<const unsigned char*>((lstrsets)[i].c_str()), (lstrsets)[i].size(), (void*)(val));
        }
        wg->done();
    };

    auto art_query = [](tsdb::base::WaitGroup* wg, tsdb::mem::art_tree* artTree, const std::vector<tsdb::label::Labels>& lsets,
                        const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            auto val = tsdb::mem::art_search(artTree, reinterpret_cast<const unsigned char*>((lstrsets)[i].c_str()), (lstrsets)[i].size());
        }
        wg->done();
    };

    auto art_update = [](tsdb::base::WaitGroup* wg, tsdb::mem::art_tree* artTree, const std::vector<tsdb::label::Labels>& lsets,
                         const std::vector<std::string>& lstrsets, uint64_t left, uint64_t right) {
        for (uint64_t i = left; i < right; i++) {
            uint64_t * val = new uint64_t(i+1);
            tsdb::mem::art_insert(artTree, reinterpret_cast<const unsigned char*>(lstrsets[i].c_str()), lstrsets[i].size(), (void*)(val));
        }
        wg->done();
    };

    int64_t d1;
    for (int i = 1; i <= 10; i++) {
//        uint64_t i = 1;
        uint64_t series_num = i * 1000000;
        std::vector<tsdb::label::Labels> lsets;
        lsets.reserve(series_num);
        tsdb::testutil::get_devops_labels(series_num, &lsets);

        std::vector<std::string> lstrsets;
        lstrsets.reserve(series_num);
        for (uint64_t i = 0; i < lsets.size(); i++) {
            std::string lbs = tsdb::label::lbs_string(lsets[i]);
            lstrsets.push_back(lbs);
        }

        std::cout<<"series num: "<<series_num<<std::endl;

        thmap* tmap = thmap_create(0, nullptr, 0);
        Timer timer1;
        timer1.start();
        tsdb::base::WaitGroup wg;
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(thmap_insert, &wg, tmap, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"Thmap insert: "<<d1<<" ns"<<std::endl;
        sleep(1);

        timer1.start();
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(thmap_query, &wg, tmap, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"Thmap search: "<<d1<<" ns"<<std::endl;
        sleep(1);

        timer1.start();
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(thmap_update, &wg, tmap, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"Thmap update: "<<d1<<" ns"<<std::endl;
        sleep(1);

        thmap_destroy(tmap);


        tsdb::mem::art_tree art;
        tsdb::mem::art_tree_init(&art);
        timer1.start();
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(art_insert, &wg, &art, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"ART insert: "<<d1<<" ns"<<std::endl;
        sleep(1);

        timer1.start();
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(art_query, &wg, &art, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"ART search: "<<d1<<" ns"<<std::endl;
        sleep(1);

        timer1.start();
        for (int j = 0; j < thread_num; j++) {
            wg.add(1);
            pool.enqueue(std::bind(art_update, &wg, &art, lsets, lstrsets, j*series_num/thread_num, (j+1)*series_num/thread_num));
        }
        wg.wait();
        d1 = timer1.since_start_nano();
        std::cout<<"ART update: "<<d1<<" ns"<<std::endl;
        sleep(1);

        tsdb::mem::art_tree_destroy(&art);

        sleep(3);
        std::cout<<std::endl;
    }
}

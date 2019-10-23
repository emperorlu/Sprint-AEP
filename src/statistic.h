#pragma once

#include <chrono>
#include <ratio>
#include <iostream>
using namespace std;

// ATTENTION: only for single thread!!!

class Statistic{
public:
    Statistic();
    ~Statistic() = default;

    void start();

    void Initail() {
        statisticcstart_ = chrono::high_resolution_clock::now();
    }

    void end();

    void add_search();

    void add_write();

    void add_comp_lat();

    void add_comp_num(){
        comp_num_++;
        total_comp_num_++;
    };

    void add_entries_num();
    
    void add_split_num();
    
    void print_latency();

    void clear_period();

    void add_node_search(){node_search_++;}

    void add_tree_level(int treeLevel) {tree_level_ += treeLevel;}

    void print_cur(){
        chrono::duration<double, std::nano> diff = end_ - start_;
        chrono::duration<double, std::nano> diff_start = start_ - statisticcstart_;
        chrono::duration<double, std::nano> diff_end = end_ - statisticcstart_;
        printf("total_time: %lf s, start %lf s, end %lf s\n", 
                diff.count() * 1e-9, diff_start.count() * 1e-9, diff_end.count() * 1e-9);
        // cout<<"total_time: "<<diff.count() * 1e-9<<"s\n";
    }

    void add_put() {
        function_count ++;
        chrono::duration<double, std::nano> diff = end_ - start_;
        put_ += diff.count();
    }

    void add_get() {
        function_count ++;
        chrono::duration<double, std::nano> diff = end_ - start_;
        get_ += diff.count();
    }

    void add_delete() {
        function_count ++;
        chrono::duration<double, std::nano> diff = end_ - start_;
        delete_ += diff.count();
    }

    void add_scan() {
        function_count ++;
        chrono::duration<double, std::nano> diff = end_ - start_;
        scan_ += diff.count();
    }

    void print_put() {
        cout
        <<"num "<<num_
        <<" period_put_search_latency(s) "<< read_ * 1e-9
        <<" average_put_search_latency(ns) "<< read_ / num_
        <<" period_put_write_latency(ns) "<< write_ * 1e-9
        <<" average_put_write_latency(ns) "<< write_ / num_
        <<"\n";
    }

private:
    double read_;
    double write_;
    double total_read_;
    double total_write_;

    double comp_lat_;
    double total_comp_lat_;
    uint64_t comp_num_;
    uint64_t total_comp_num_;

    double put_;
    double get_;
    double delete_;
    double scan_;
    uint64_t function_count;

    chrono::high_resolution_clock::time_point start_;
    chrono::high_resolution_clock::time_point end_;
    chrono::high_resolution_clock::time_point statisticcstart_;
    uint64_t num_;
    uint64_t total_num_;
    uint64_t split_num_;

    uint64_t node_search_;
    uint64_t tree_level_;
};


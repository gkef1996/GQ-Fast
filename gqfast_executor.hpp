#ifndef gqfast_executor_hpp_
#define gqfast_executor_hpp_

#include <iostream>
#include <fstream>
#include <dlfcn.h>             // dll functions
#include <utility>             // std::pair
#include "gqfast_index.hpp"
#include <chrono>

#define MAX_THREADS 8

extern chrono::steady_clock::time_point benchmark_t1;
extern chrono::steady_clock::time_point benchmark_t2;

//Keep query parameters
int query_parameters[10];

// Pre-declared index pointers
extern GqFastIndex<uint32_t>** idx;

struct args_threading
{
    uint32_t start;
    uint32_t end;
    int thread_id;
};

extern pthread_t threads[MAX_THREADS];
extern pthread_spinlock_t* r_spin_locks;

template <typename T>

class sort_comparator
{
public:
    inline bool operator() (const pair<uint32_t, T> & a, const pair<uint32_t, T> & b)
    {
        return a.second > b.second;
    }
};

template <typename T>
pair<uint32_t, T> * top_k(T* result, int k, int domain)
{

    vector<pair<uint32_t, T> > pairs;
    pairs.resize(domain);

    for (int i=0; i < domain; i++)
    {
        pairs[i].first = i;
        pairs[i].second = result[i];
    }

    sort(pairs.begin(), pairs.end(), sort_comparator<T>());

    pair<uint32_t, T> * result_pairs = new pair<uint32_t,T>[k];
    for (int i=0; i<k; i++)
    {
        result_pairs[i].first = 0;
        result_pairs[i].second = 0;
    }

    for (int i = 0; i < k;  i++)
    {
        if (pairs[i].second > 0)
        {
            result_pairs[i].first = pairs[i].first;
            result_pairs[i].second = pairs[i].second;
        }
        else
        {
            result_pairs[i].first = 0;
            result_pairs[i].second = 0;
        }
    }

    return result_pairs;
}



template <typename T>
void write_result_to_file(T* result, int* null_checks, int domain, string func_name, chrono::duration<double> & time_span, uint32_t num_results)
{

    vector<pair<uint32_t,T> > result_pairs;
    // Select result elements
    for (int i=0; i<domain; i++)
    {
        if (null_checks[i])
        {
            pair<uint32_t, T> temp;
            temp.first = i;
            temp.second = result[i];
            result_pairs.push_back(temp);
        }
    }

    // Sort the elements
    sort(result_pairs.begin(), result_pairs.end(), sort_comparator<T>());

    // Write to file
    ofstream myfile;
    myfile.open ("./Result/"+func_name+".result");
    myfile << "Number of results: " << result_pairs.size() << "\n";
    myfile << "Query execution time: " << time_span.count()*1000 << " ms\n";
    if (result_pairs.size() < num_results)
    {
        num_results = result_pairs.size();
        myfile << "Detailed results (id, aggregation value): \n";
    }
    else
    {
        myfile << "Detailed results on first " << num_results << " (id, aggregation value): \n";
    }

    for (uint32_t i=0; i<num_results; i++)
    {
        myfile << result_pairs[i].first << "," << result_pairs[i].second << "\n";
    }
    myfile.close();

}



template <typename T>
void run_query(string func_name, uint32_t domain, uint32_t num_results)
{

    //int* cold_checks;
    int* null_checks;
    int count = 0;

    // load the symbol
    cout << "Opening " << func_name << "\n";
    cout << "Acknowledging first parameter as:" << query_parameters[0] << "\n";

    string library_name = "./Code/" + func_name + ".so";
    void* handle = dlopen(library_name.c_str(), RTLD_NOW);
    if (!handle)
    {
        cerr << "Cannot open library: " << dlerror() << '\n';
        return;
    }

    cout << "Loading symbol query_type...\n";
    typedef T* (*query_type)(int **);

    // reset errors
    dlerror();

    query_type query = (query_type) dlsym(handle, func_name.c_str());
    const char *dlsym_error = dlerror();
    if (dlsym_error)
    {
        cerr << "Cannot load symbol 'query_type': " << dlsym_error <<
             '\n';
        dlclose(handle);
        return;
    }

    //T* cold_result = query(&cold_checks);
    T* result = query(&null_checks);

    for (uint32_t i=0; i<domain; i++)
    {
        if (null_checks[i])
        {
            count++;
            if (count == 1)
            {
                benchmark_t2 = chrono::steady_clock::now();
            }
        }
    }

    // close the library
    cout << "Closing library...\n";
    dlclose(handle);

    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(benchmark_t2 - benchmark_t1);
    cout << "Query " << func_name << " processed in " << time_span.count()*1000 << " ms.\n\n";


    pair<uint32_t, T> * tops_result = top_k(result, 20, domain);

    cout.precision(17);
    for (int i=0; i<20; i++)
    {
        cout << "Position " << tops_result[i].first << ": " << tops_result[i].second << "\n";
    }

    delete[] tops_result;

    write_result_to_file(result, null_checks, domain, func_name, time_span, num_results);

    delete[] result;
    //delete[] cold_result;

    //delete[] cold_checks;
    delete[] null_checks;

}



#endif

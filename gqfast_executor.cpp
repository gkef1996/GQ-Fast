#ifndef gqfast_executor_cpp_
#define gqfast_executor_cpp_

#include <iostream>
//#include <climits>
#include <fstream>
#include <sstream>
#include <dlfcn.h>             // dll functions
#include <utility>             // std::pair
#include <unordered_map>
#include <map>
#include <set>
#include <unordered_set>
#include "serialization.hpp"
#include "gqfast_executor.hpp"

chrono::steady_clock::time_point benchmark_t1;
chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
GqFastIndex<uint32_t>** idx;

pthread_t threads[MAX_THREADS];
pthread_spinlock_t* r_spin_locks;

static int num_indexes;
static uint32_t agg_domain;
static unordered_map<string,int> index_positions_map;

static std::unordered_set<string> compiled_queries;
//static int query_parameters[10];

void init_globals()
{

    idx = new GqFastIndex<uint32_t>*[num_indexes];
    // Globals are initially null/0
    for (int i=0; i<num_indexes; i++)
    {
        idx[i] = nullptr;
    }

    r_spin_locks = new pthread_spinlock_t[agg_domain];
    for (uint64_t i=0; i<agg_domain; i++)
    {
        pthread_spin_init(&r_spin_locks[i], PTHREAD_PROCESS_PRIVATE);
    }

}

void delete_globals()
{
    for (int i=0; i<num_indexes; i++)
    {
        if (idx[i])
        {
            delete idx[i];
        }
    }
    delete[] idx;
    //delete[] r_spin_locks;
}



void load_agg_domain(string exec_settings_string)
{

    string line;
    ifstream myfile(exec_settings_string);

    // First line is number of indices to load
    getline(myfile, line);
    //num_indexes = atoi(line.c_str());
    getline(myfile, line);
    agg_domain = atoi(line.c_str());
//    init_globals();
//    cout << "Begin reading indexes from disk...\n" << flush;
//    for (int i=0; i<num_indexes; i++)
  //  {
    //    getline(myfile, line);
      //  cout << "Loading index " << line << "...\n" << flush;
       // line = "./Index/index_" + line + ".gqfast";
       // load_index(idx[i], line.c_str());

    //}

    myfile.close();

    //chrono::steady_clock::time_point t2 = chrono::steady_clock::now();
    //chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);

    //cout << "Loading indexes took " << time_span.count() << " seconds.\n" << flush;


}


void handle_indices()
{

    map<int, string> index_mapping;
    string line;
    ifstream myfile("./MetaData/indexes.gqfast");
    int counter = 1;
    while (getline(myfile, line))
    {
        line = line.substr(6);
        line = line.substr(0, line.length()-7);
        index_mapping[counter++] = line;
    }

    if (index_mapping.size() < 1)
    {
        cout << "\nThere are no indexes available for loading. Try building indexes first using 'gqfast_build'\n";
        exit(0);
    }

    myfile.close();

    bool valid = false;
    set<int> numerical_selections;
    while (!valid)
    {
        cout << "\nAvailable indexes: \n";
        for (auto it = index_mapping.begin(); it != index_mapping.end(); ++it)
        {
            cout << it->first << ": " << it->second << "\n";
        }

        cout << "\nPlease select the indexes you would like to load from the listing above.";
        cout << " Select using numbers separated by commas. \nExample:\n";
        cout << "1,2,4,6\n\n";

        string selections;
        cin >> selections;

        stringstream lineStream(selections);
        string cell;

        while(getline(lineStream,cell,','))
        {
            valid = true;
            try
            {
                int i = std::stoi(cell);
                if (index_mapping.find(i) != index_mapping.end())
                {
                    numerical_selections.emplace(i);
                }
                else
                {
                    valid = false;
                    numerical_selections.clear();
                    break;
                }
            }
            catch(std::exception const & e)
            {
                valid = false;
                numerical_selections.clear();
                break;
            }
        }

        if (!valid)
        {
            cout << "\nError! Invalid input detected, please try again\n";
        }
        else
        {
            bool valid_y_or_n = false;
            while (!valid_y_or_n)
            {
                valid_y_or_n = true;
                cout << "\nYou have selected the following indexes for loading: \n";
                for (auto sit= numerical_selections.begin(); sit != numerical_selections.end(); ++sit)
                {
                    cout << index_mapping[*sit] << "\n";
                }
                string proceed;
                cout << "\nIf you wish to proceed with these selections, enter 'y' or 'yes'.\n";
                cout << "If you would like to re-enter selections, enter 'n' or 'no'.\n\n";
                cin >> proceed;

                if (proceed.compare("y") && proceed.compare("yes") && proceed.compare("n") && proceed.compare("no"))
                {
                    valid_y_or_n = false;
                    cout << "\nError! Invalid input detected, please try again\n";
                }
                else if (!proceed.compare("n") || !proceed.compare("no"))
                {
                    valid = false;
                    numerical_selections.clear();
                }
            }
        }
    }

    num_indexes = numerical_selections.size();
    idx = new GqFastIndex<uint32_t>*[num_indexes];

    chrono::steady_clock::time_point t1 = chrono::steady_clock::now();
    cout << "Begin reading indexes from disk...\n" << flush;
    int i =0;
    for (auto sit = numerical_selections.begin(); sit != numerical_selections.end(); ++sit)
    {
        string curr_index = index_mapping[*sit];
        // Position is +1 so that we can easily check existence (there is no existing position that maps to 0)
        index_positions_map[curr_index] = i+1;

        cout << "Loading index " << curr_index << "...\n" << flush;
        curr_index = "./Index/index_" + curr_index + ".gqfast";
        load_index(idx[i++], curr_index.c_str());



    }


    chrono::steady_clock::time_point t2 = chrono::steady_clock::now();
    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);
    cout << "Loading indexes took " << time_span.count() << " seconds.\n" << flush;

    ofstream outfile("./MetaData/index_positions_temp.gqfast");
    for (auto sit = numerical_selections.begin(); sit != numerical_selections.end(); ++sit)
    {
        outfile << index_mapping[*sit] << "\n";
    }

    outfile.close();
}

void parameterize_query(string code_file, int d1, int d2){
	ifstream code (code_file);
	ofstream new_code;
	new_code.open("Code/temp.cpp");
	string line;
	string p1 = to_string(d1);
	string p2 = to_string(d2);
	std::size_t found;
	while(getline(code,line)){
		found = line.find(p1);
		if(found != std::string::npos){
			line.replace(found,p1.length(),"query_parameters[0]");
		}
		found = line.find(p2);
		if(found != std::string::npos){
			line.replace(found,p2.length(),"query_parameters[1]");
		}
		new_code << line << "\n";
	}
	new_code.close();
}

void handle_queries()
{

    vector<string> available_queries;

    if (index_positions_map["dt_doc"] && index_positions_map["dt_term"])
    {
        available_queries.push_back("SD");
    }
    if (index_positions_map["dt_doc"] && index_positions_map["dt_term"] && index_positions_map["document_doc"])
    {
        available_queries.push_back("FSD");
    }
    if (index_positions_map["dt_term"] && index_positions_map["da_doc"])
    {
        available_queries.push_back("AD");
    }
    if (index_positions_map["dt_doc"] && index_positions_map["dt_term"] && index_positions_map["da_author"] && index_positions_map["da_doc"] && index_positions_map["document_doc"])
    {
        available_queries.push_back("AS");
    }
    if (index_positions_map["cs_cid"] && index_positions_map["cs_csid"] && index_positions_map["pa_csid"] && index_positions_map["pa_pid"] && index_positions_map["sp_pid"] && index_positions_map["sp_sid"])
    {
        available_queries.push_back("CS");
    }
    if (index_positions_map["dt_term"])
    {
        available_queries.push_back("Q1");
    }
    if (index_positions_map["dt_term"] && index_positions_map["da_doc"])
    {
        available_queries.push_back("Q2");
    }
    if (index_positions_map["dt_term"] && index_positions_map["document_doc"])
    {
        available_queries.push_back("Q3");
    }
    if (index_positions_map["dt_doc"] && index_positions_map["dt_term"] && index_positions_map["da_doc"])
    {
        available_queries.push_back("Q4");
    }

    if (available_queries.size() < 1)
    {
        cout << "\nThere are no queries that can be run with the indexes you have loaded.\n";
        cout << "Please re-run program and load additional indexes to allow query execution\n";
        exit(0);
    }


    cout << "\nYou can now execute test queries on the loaded indexes.\n";

    bool valid = false;
    int query_index = 0;
    while (!valid)
    {
        cout << "List of available queries:\n";
        for (int i=0; i<available_queries.size(); i++)
        {
            cout << i+1 << ": " << available_queries[i] << "\n";
        }
        cout << "\n";
        valid = true;
        string query_selection;
        cout << "Please enter the number of the query you wish to run:\n";
        cin >> query_selection;
        try {
            query_index = std::stoi(query_selection);
            if (query_index<1 || query_index>available_queries.size())
            {
                valid = false;
            }
        }
        catch(std::exception const & e)
        {
            valid = false;
        }
        if (!valid)
        {
            cout << "\nError! Invalid input detected, please try again\n";
        }
        else
        {
            string proceed;
            bool valid_y_or_n = false;
            while (!valid_y_or_n)
            {
                valid_y_or_n = true;
                cout << "\nYou have selected query " << available_queries[query_index-1] << "\n";
                cout << "If this is correct, press 'y' or 'yes' to execute the query.\n";
                cout << "If you would like to select a different query, press 'n' or 'no'.\n";
                cin >> proceed;

                if (proceed.compare("y") && proceed.compare("yes") && proceed.compare("n") && proceed.compare("no"))
                {
                    valid_y_or_n = false;
                    cout << "\nError! Invalid input detected, please try again\n";
                }
                else if (!proceed.compare("n") || !proceed.compare("no"))
                {
                    valid = false;
                    cout << "\n";
                }
            }
        }

    }

    string query = available_queries[query_index-1];
    valid = false;
    int id1 = 0;
    int id2 = 0;
    cout << "\n";
    while (!valid)
    {
        valid = true;
        if (query.compare("AD") && query.compare("Q3"))
        {
            string id_string;
            cout << "Enter the selection ID for query " << query << "\n";
            cin >> id_string;
            try {
                id1 = std::stoi(id_string);
                if (id1 < 1)
                {
                    cout << "\nError! ID must be a positive integer, please try again\n";
                    valid = false;
                }
            }
            catch(std::exception const & e)
            {
                cout << "\nError! Invalid input detected, please try again\n";
                valid = false;
            }
        }
        else
        {
            string id_string;
            cout << "Enter the first selection ID for query " << query << "\n";
            cin >> id_string;
            try {
                id1 = std::stoi(id_string);
                if (id1 < 1)
                {
                    cout << "\nError! ID must be a positive integer, please try again\n";
                    valid = false;
                    continue;
                }
            }
            catch(std::exception const & e)
            {
                cout << "\nError! Invalid input detected, please try again\n";
                valid = false;
                continue;
            }
            cout << "Enter the second selection ID for query " << query << "\n";
            cin >> id_string;
            try {
                id2 = std::stoi(id_string);
                if (id2 < 1)
                {
                    cout << "\nError! ID must be a positive integer, please try again\n";
                    valid = false;
                }
            }
            catch(std::exception const & e)
            {
                cout << "\nError! Invalid input detected, please try again\n";
                valid = false;
            }
        }
    }

    cout << "\nExecuting query " << query << "\n";

    query_parameters[0] = id1;
    query_parameters[1] = id2;
    printf("Initializing 1st parameter as: %d \n",query_parameters[0]);

	if (compiled_queries.find(query) == compiled_queries.end()) {
		compiled_queries.insert(query);
		stringstream parser_execution_stream;
		id1 = - 99999;
		parser_execution_stream << "./run_parser.sh " << query << " " << id1;
		if (id2 != 0){
			id2 = - 88888;
			parser_execution_stream << " " << id2;
		}

		id2 = - 88888;
		string parser_execution_string = parser_execution_stream.str();

		cout << parser_execution_string << "\n";
		int check = system(parser_execution_string.c_str());
		//int check;

		//Replace sentinel values with user parameters
		string code_file = "Code/"+query+".cpp";
		parameterize_query(code_file,id1, id2);

		string compilation_string = "./compile_query.sh " + query;

		check = system(compilation_string.c_str());
	}

    string exec_settings_string = "./MetaData/" + query + ".setting";

    load_agg_domain(exec_settings_string);

    r_spin_locks = new pthread_spinlock_t[agg_domain];
    for (uint64_t i=0; i<agg_domain; i++)
    {
        pthread_spin_init(&r_spin_locks[i], PTHREAD_PROCESS_PRIVATE);
    }
    run_query<uint32_t>(query, agg_domain, 1000);

    delete[] r_spin_locks;
    r_spin_locks = nullptr;

}



int main(int argc, char ** argv)
{

    handle_indices();

    bool running = true;
    while (running)
    {
        handle_queries();
        cout << "\nResults can be found in the 'Results' directory.\n";
        bool valid = false;
        while (!valid)
        {
            valid = true;
            cout << "Press 'y' or 'yes' to run another query\n";
            cout << "Otherwise, press 'q' or 'quit' to quit\n";
            string entry;
            cin >> entry;
            if (entry.compare("y") && entry.compare("yes") && entry.compare("q") && entry.compare("quit"))
            {
                cout << "\nError! Invalid input detected, please try again\n";
                valid = false;
            }
            else if (entry.compare("y") && entry.compare("yes"))
            {
                running = false;
            }
        }
    }

    //load_indexes(exec_settings_string);



    /*
        char c;
        char* exec_settings_filename;
        char* query_filename;
        string help = "usage: %s [-h] [-s executor_setting_filename] [-q query_library_name] \n\n"
                     "-h\tShow list of parameters.\n\n"
                     "-s\tSpecify executor settings filename.\n\n"
                     "-q\tSpecify query .so filename\n\n";
        while ((c = getopt(argc, argv, "hs:q:")) != -1)
        {
            switch (c)
            {
            case 'h':
                cout << "\n\n" << help << "\n\n";
                exit(0);
                break;
            case 's':
                if (optarg)
                {
                    exec_settings_filename = optarg;
                }
                else
                {
                    cerr << "Error: missing exec settings filename!\n";
                }
                break;
            case 'q':
                if (optarg)
                {
                    query_filename = optarg;
                }
                else
                {
                    cerr << "Error: missing query filename!\n";
                }
                break;
            }
        }

        string exec_settings_string(exec_settings_filename);
        exec_settings_string = "./MetaData/" + exec_settings_string + ".setting";
        load_indexes(exec_settings_string);
    */
    //  string query_string(query_filename);

    delete_globals();

    return 0;
}




#endif

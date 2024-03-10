#include <iostream>
#include <yaml-cpp/yaml.h>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <vector>
#include <cstdlib>
#include <typeinfo>
#include <unistd.h>
#include <stack>
#include <sys/wait.h>


bool dfs(const std::string& job, const YAML::Node& jobs, std::unordered_map<std::string, std::unordered_set<std::string>>& graph){
    static std::unordered_set<std::string> visited;
    static std::unordered_set<std::string> recursionStack;

    if (recursionStack.count(job))
            return true;

        if (visited.count(job))
            return false;

        visited.insert(job);
        recursionStack.insert(job);

        for (const auto& dependent : graph[job]) {
            if (dfs(dependent, jobs, graph))
                return true;
        }

        recursionStack.erase(job);
        return false;

}


bool hasCycle(const YAML::Node& jobs, std::unordered_map<std::string, std::unordered_set<std::string>>& graph) {
    for (const auto& entry : jobs) {
        const std::string& job = entry.first.as<std::string>();
        if (dfs(job, jobs, graph))
            return true;
    }

    return false;
}


bool isConnected(const YAML::Node& jobs, std::unordered_map<std::string, std::unordered_set<std::string>>& graph) {
    std::unordered_set<std::string> visited;
    std::queue<std::string> q;


    std::vector<std::string> startJobs;

    for (const auto& entry : jobs) {
    
        const std::string& job = entry.first.as<std::string>();
        
        if(entry.second["dep"].size() == 0){
            startJobs.push_back(job);
        }
    }
    //std::cout << startJobs.size() << '\n';

    // BFS traversal to check connectivity
    for(const auto& startJob : startJobs){
        q.push(startJob);
        visited.insert(startJob);

        while (!q.empty()) {
            std::string curr = q.front();
            q.pop();
            //std::cout << curr << '\n'; 
            for (const auto& dependent : graph[curr]) {    
                 
                if (!visited.count(dependent)) {
                    visited.insert(dependent);
                    q.push(dependent);
                }
            }
        }
    }
    //std::cout << visited.size() << ' ' << jobs.size() << '\n';
    return visited.size() == jobs.size();
}

int main() {
    YAML::Node jobs = YAML::LoadFile("job.yaml");

    if (!jobs.IsMap()) {
        std::cerr << "Error: Invalid YAML file format!" << std::endl;
        exit(EXIT_FAILURE);
    }

    std::unordered_map<std::string,int> finished; //for tracking how many of the dependancies are completed for the execution part


    std::unordered_map<std::string, std::unordered_set<std::string>> graph;
    for (const auto& entry : jobs) {
        const std::string& job = entry.first.as<std::string>();
        finished[job] = 0;
        for (const auto& dep : entry.second["dep"]) {

            graph[dep.as<std::string>()].insert(job);
        }
    }
/*
    for (const auto& entry : graph) {
        std::cout << entry.first << ": ";
        for (const auto& dep : entry.second) {
            std::cout << dep << ' ';
        }
        std::cout << '\n';
    }
*/

    if (!isConnected(jobs, graph)) {
        std::cerr << "Error: Jobs are not connected!" << std::endl;
        exit(EXIT_FAILURE);
    }

    if (hasCycle(jobs, graph)) {
        std::cerr << "Error: Detected cycle in job dependencies!" << std::endl;
        exit(EXIT_FAILURE);
    }
/*
    for (const auto& entry : graph) {
        std::cout << entry.first << ": ";
        for (const auto& dep : entry.second) {
            std::cout << dep << ' ';
        }
        std::cout << '\n';
    }
*/
    std::cout << "DAG is valid." << std::endl;



    std::queue<std::string> current_jobs;
    std::unordered_set<std::string> pending_jobs;
 
    for (const auto& entry : jobs) {
        if(entry.second["dep"].size() == 0){
            current_jobs.push(entry.first.as<std::string>());
        }
    }


    int completed_jobs = 0;
    std::vector<int> job_pids; 

    while(completed_jobs != jobs.size()){
        std::string key;
        YAML::Node value;
        key = current_jobs.front();
        value = jobs[current_jobs.front()];

        for(const auto& dependant : graph[key]){
            pending_jobs.insert(dependant);
        }
        
        current_jobs.pop();

        job_pids.push_back(fork());

        //for(const auto pid : job_pids){
        
        if(job_pids.back() == 0){//child
            std::system(value["command"].as<std::string>().c_str());
            exit(EXIT_SUCCESS);
        }
        if(job_pids.back() > 0){//parent
            
            if(current_jobs.empty()){

                std::cout << '\n';
                for(const auto pid : job_pids){
                    int status;
                    waitpid(pid, &status, 0);
                    if(WIFEXITED(status)){
                        completed_jobs++;
                    }
                    else{
                        std::cout << "the command \"" << value["command"].as<std::string>() << "\" failed " << WEXITSTATUS(status) << '\n';
                        return 0;
                    }
                }
                for(const auto& job : pending_jobs){
                    current_jobs.push(job);
                }
                pending_jobs.clear();
                
                job_pids.clear();
            }
       
                  
        }
        
    }


    return 0;
}
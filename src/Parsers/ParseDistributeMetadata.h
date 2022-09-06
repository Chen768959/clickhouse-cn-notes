#pragma once

#include <string>
#include <common/logger_useful.h>

namespace DB
{
    using namespace std;
    class ParseDistributeMetadata{
    private:
        string clusterStr;
        string databaseStr;
        string tableStr;
        bool res = false;

    public:
        ParseDistributeMetadata(string distributeMetadataPath);
        string getClusterStr();
        string getDatabaseStr();
        string getTableStr();
        bool parseRes();
    };

}


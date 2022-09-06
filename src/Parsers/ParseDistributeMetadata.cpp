#include "ParseDistributeMetadata.h"
#include <vector>
#include <fstream>
#include <regex>

namespace DB
{

    using namespace std;

    const regex regexLine("\'.*\'");
    const regex regexDetail(",");
    const int INDEX_MAX = 1000;

    string formatStr(string str){
        return str.erase(0,str.find('\'')+1).erase(str.find_last_not_of('\'') + 1);
    }

    ParseDistributeMetadata::ParseDistributeMetadata(string distributeMetadataPath) {
        ifstream fin(distributeMetadataPath.c_str());
        int index = 1;
        string strLine;
        while (getline(fin, strLine) && index < INDEX_MAX)
        {
            if ('E' == *strLine.c_str()){
                smatch m;
                bool found = regex_search(strLine, m, regexLine);
                if(found && m.size()>0)
                {
                    string infoStr = m.str(0);

                    vector<string> elems(sregex_token_iterator(infoStr.begin(), infoStr.end(), regexDetail, -1), sregex_token_iterator());

                    if (elems.size() >= 3){
                        clusterStr = formatStr(elems[0]);
                        databaseStr = formatStr(elems[1]);
                        tableStr = formatStr(elems[2]);
                        res = true;
                        break;
                    }
                }
            }
            index ++;
        }
        if (index == INDEX_MAX){
            LOG_ERROR(&Poco::Logger::get("ParseDistributeMetadata"),"ParseDistributeMetadata FALSE, Metadata line rather than "+ to_string(INDEX_MAX)
                                                                    + " distributeMetadataPath: " + distributeMetadataPath);
        }
        fin.close();
    }

    string ParseDistributeMetadata::getClusterStr() {
        return clusterStr;
    }

    string ParseDistributeMetadata::getDatabaseStr() {
        return databaseStr;
    }

    string ParseDistributeMetadata::getTableStr() {
        return tableStr;
    }

    bool ParseDistributeMetadata::parseRes() {
        return res;
    }

}


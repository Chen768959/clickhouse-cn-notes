#include "parseDistributeTable.h"

#include <Parsers/ParseDistributeMetadata.h>

namespace DB
{

bool parseDistributeClusterAndDbAndTableName(ContextMutablePtr * context_, String & cluster_str, String & database_str, String & table_str){
    if (!context_){
        return false;
    }

    String metadataTablePath = context_->get()->getPath() + "metadata/"+database_str+"/"+table_str+".sql";
    ParseDistributeMetadata parseDistributeMetadata(metadataTablePath);
    if (parseDistributeMetadata.parseRes()){
        cluster_str = parseDistributeMetadata.getClusterStr();
        database_str = parseDistributeMetadata.getDatabaseStr();
        table_str = parseDistributeMetadata.getTableStr();
        return true;
    }

    return false;
}

}

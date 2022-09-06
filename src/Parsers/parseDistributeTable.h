#pragma once
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

namespace DB
{
    bool parseDistributeClusterAndDbAndTableName(ContextMutablePtr * context_, String & cluster_str, String & database_str, String & table_str);
}

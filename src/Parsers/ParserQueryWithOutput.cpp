#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserShowTablesQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserShowProcesslistQuery.h>
#include <Parsers/ParserCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserKillQueryQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ParserShowAccessEntitiesQuery.h>
#include <Parsers/ParserShowAccessQuery.h>
#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ParserShowPrivilegesQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/QueryWithOutputSettingsPushDownVisitor.h>


namespace DB
{

bool ParserQueryWithOutput::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput POS_BE:"+std::string(pos.get().begin)+"...POS_EN:"+std::string(pos.get().end));

    ParserShowTablesQuery show_tables_p;
    ParserSelectWithUnionQuery select_p;
    ParserTablePropertiesQuery table_p;
    ParserDescribeTableQuery describe_table_p;
    ParserShowProcesslistQuery show_processlist_p;
    ParserCreateQuery create_p;
    ParserAlterQuery alter_p;
    ParserRenameQuery rename_p;
    ParserDropQuery drop_p;
    ParserCheckQuery check_p;
    ParserOptimizeQuery optimize_p;
    ParserKillQueryQuery kill_query_p;
    ParserWatchQuery watch_p;
    ParserShowAccessQuery show_access_p;
    ParserShowAccessEntitiesQuery show_access_entities_p;
    ParserShowCreateAccessEntityQuery show_create_access_entity_p;
    ParserShowGrantsQuery show_grants_p;
    ParserShowPrivilegesQuery show_privileges_p;
    ParserExplainQuery explain_p(end);

    ASTPtr query;

    bool parsed =
           explain_p.parse(pos, query, expected) // 校验当前初始pos关键字是否为 EXPLAIN等系列关键字，不是则返回false
        || select_p.parse(pos, query, expected)
        || show_create_access_entity_p.parse(pos, query, expected) /// should be before `show_tables_p`
        || show_tables_p.parse(pos, query, expected)
        || table_p.parse(pos, query, expected)
        || describe_table_p.parse(pos, query, expected)
        || show_processlist_p.parse(pos, query, expected)
        || create_p.parse(pos, query, expected)
        || alter_p.parse(pos, query, expected)
        || rename_p.parse(pos, query, expected)
        || drop_p.parse(pos, query, expected)
        || check_p.parse(pos, query, expected)
        || kill_query_p.parse(pos, query, expected)
        || optimize_p.parse(pos, query, expected)
        || watch_p.parse(pos, query, expected)
        || show_access_p.parse(pos, query, expected)
        || show_access_entities_p.parse(pos, query, expected)
        || show_grants_p.parse(pos, query, expected)
        || show_privileges_p.parse(pos, query, expected);

    if (!parsed){// 所有解析器都解析不通
        LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput false END");
        return false;
    }


    /// FIXME: try to prettify this cast using `as<>()`
    auto & query_with_output = dynamic_cast<ASTQueryWithOutput &>(*query);

    ParserKeyword s_into_outfile("INTO OUTFILE");
    if (s_into_outfile.ignore(pos, expected))
    {
        ParserStringLiteral out_file_p;
        if (!out_file_p.parse(pos, query_with_output.out_file, expected)){
            LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput false2 END");
            return false;
        }

        query_with_output.children.push_back(query_with_output.out_file);
    }

    ParserKeyword s_format("FORMAT");

    if (s_format.ignore(pos, expected))
    {
        ParserIdentifier format_p;

        if (!format_p.parse(pos, query_with_output.format, expected)){
            LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput false3 END");
            return false;
        }
        setIdentifierSpecial(query_with_output.format);

        query_with_output.children.push_back(query_with_output.format);
    }

    // SETTINGS key1 = value1, key2 = value2, ...
    ParserKeyword s_settings("SETTINGS");
    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);
        if (!parser_settings.parse(pos, query_with_output.settings_ast, expected)){
            LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput false4 END");
            return false;
        }
        query_with_output.children.push_back(query_with_output.settings_ast);

        // SETTINGS after FORMAT is not parsed by the SELECT parser (ParserSelectQuery)
        // Pass them manually, to apply in InterpreterSelectQuery::initSettings()
        if (query->as<ASTSelectWithUnionQuery>())
        {
            QueryWithOutputSettingsPushDownVisitor::Data data{query_with_output.settings_ast};
            QueryWithOutputSettingsPushDownVisitor(data).visit(query);
        }
    }

    node = std::move(query);

    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserQueryWithOutput END");
    return true;
}

}

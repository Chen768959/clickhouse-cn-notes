#include <Parsers/ParserExplainQuery.h>

#include <Parsers/ASTExplainQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserQuery.h>

#include <common/logger_useful.h>
namespace DB
{

bool ParserExplainQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserExplainQuery POS_BE:"+std::string(pos.get().begin)+"...POS_EN:"+std::string(pos.get().end));

    ASTExplainQuery::ExplainKind kind;

    ParserKeyword s_ast("AST");
    ParserKeyword s_explain("EXPLAIN");
    ParserKeyword s_syntax("SYNTAX");

    ParserKeyword s_pipeline("PIPELINE");
    ParserKeyword s_plan("PLAN");

    // 校验当前pos关键字是否为“EXPLAIN” ,不是则直接返回false
    if (s_explain.ignore(pos, expected))
    {
        kind = ASTExplainQuery::QueryPlan;

        if (s_ast.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::ParsedAST;
        else if (s_syntax.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::AnalyzedSyntax;
        else if (s_pipeline.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPipeline;
        else if (s_plan.ignore(pos, expected))
            kind = ASTExplainQuery::ExplainKind::QueryPlan; //-V1048
    }
    else{
        LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserExplainQuery false END");
        return false;
    }


    auto explain_query = std::make_shared<ASTExplainQuery>(kind);

    {
        ASTPtr settings;
        ParserSetQuery parser_settings(true);

        auto begin = pos;
        if (parser_settings.parse(pos, settings, expected))
            explain_query->setSettings(std::move(settings));
        else
            pos = begin;
    }

    ParserCreateTableQuery create_p;
    ParserSelectWithUnionQuery select_p;
    ASTPtr query;
    if (kind == ASTExplainQuery::ExplainKind::ParsedAST)
    {
        ParserQuery p(end);
        if (p.parse(pos, query, expected))
            explain_query->setExplainedQuery(std::move(query));
        else{
            LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserExplainQuery false2 END");
            return false;
        }

    }
    else if (select_p.parse(pos, query, expected) ||
        create_p.parse(pos, query, expected))
        explain_query->setExplainedQuery(std::move(query));
    else{
        LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserExplainQuery false3 END");
        return false;
    }

    node = std::move(explain_query);
    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserExplainQuery END");
    return true;
}

}

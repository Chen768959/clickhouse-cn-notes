#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserSelectWithUnionQuery POS_BE:"+std::string(pos.get().begin)+"...POS_EN:"+std::string(pos.get().end));
    // bool ParserSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    if (!ParserSubquery().parse(pos, node, expected) && !ParserSelectQuery().parse(pos, node, expected)){
        LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserSelectWithUnionQuery false END");
        return false;
    }

    if (const auto * ast_subquery = node->as<ASTSubquery>())
        node = ast_subquery->children.at(0);

    LOG_DEBUG(&Poco::Logger::get("Parser"),"CUSTOM_TRACE ParserSelectWithUnionQuery END");
    return true;
}

}

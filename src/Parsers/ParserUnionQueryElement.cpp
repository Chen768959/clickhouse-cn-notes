#include <Parsers/ASTSubquery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>
#include <Common/typeid_cast.h>


namespace DB
{

//select 解析器
bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /**
     * ParserSubquery().parse()：先检查当前是否存在“（ SELECT”开头的子查询。如果不存在，则继续后面的校验
     * ParserSelectQuery().parse()：正常 SELECT 关键字的查询校验
     */
    if (!ParserSubquery().parse(pos, node, expected) && !ParserSelectQuery().parse(pos, node, expected)){
        // 既不是子查询，也不是一般查询，直接返回false
        return false;
    }

    // 写入node
    if (const auto * ast_subquery = node->as<ASTSubquery>())
        node = ast_subquery->children.at(0);

    return true;
}

}

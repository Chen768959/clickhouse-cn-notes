#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserCreateQuotaQuery.h>
#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ParserCreateRowPolicyQuery.h>
#include <Parsers/ParserCreateSettingsProfileQuery.h>
#include <Parsers/ParserCreateUserQuery.h>
#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSetRoleQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>


namespace DB
{

/**
 *
 * @param pos Token迭代器，其中每个Token都是一个sql字符
 * @param node ast语法树，解析结果会放进该树中
 * @param expected 1
 * @return 是否解析成功
 */
bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserQueryWithOutput query_with_output_p(end, context);
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;
    ParserCreateUserQuery create_user_p;
    ParserCreateRoleQuery create_role_p;
    ParserCreateQuotaQuery create_quota_p;
    ParserCreateRowPolicyQuery create_row_policy_p;
    ParserCreateSettingsProfileQuery create_settings_profile_p;
    ParserDropAccessEntityQuery drop_access_entity_p;
    ParserGrantQuery grant_p;
    ParserSetRoleQuery set_role_p;
    ParserExternalDDLQuery external_ddl_p;

    /**
     * 依次调用以下解析器的parseImpl()方法，尝试解析sql。
     * 以下解析器可大致分成两类：
     * 1、有结果的sql，query_with_output_p
     * 2、剩下的都是无结果的sql类型
     *
     * 以最常用的有结果类sql解析器展开：
     * 其内部还有各种具体的sql解析器，如select解析器、show解析器等
     * 这些解析器第一步都会“列出当前查询可能会出现的关键字”
     *
     */
    bool res = query_with_output_p.parse(pos, node, expected) // 有结果的输出
        || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_role_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || create_user_p.parse(pos, node, expected)
        || create_role_p.parse(pos, node, expected)
        || create_quota_p.parse(pos, node, expected)
        || create_row_policy_p.parse(pos, node, expected)
        || create_settings_profile_p.parse(pos, node, expected)
        || drop_access_entity_p.parse(pos, node, expected)
        || grant_p.parse(pos, node, expected)
        || external_ddl_p.parse(pos, node, expected);

    return res;
}

}

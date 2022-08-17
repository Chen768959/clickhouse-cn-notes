#include <Common/StringUtils/StringUtils.h>
#include <Parsers/CommonParsers.h>
#include <common/find_symbols.h>
#include <IO/Operators.h>

#include <string.h>        /// strncmp, strncasecmp

#include <common/logger_useful.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ParserKeyword::ParserKeyword(const char * s_) : s(s_)
{
}


const char * ParserKeyword::getName() const
{
    return s;
}

bool ParserKeyword::parseImpl(Pos & pos, ASTPtr & /*node*/, Expected & expected)
{
    if (pos->type != TokenType::BareWord)
        return false;

    /**
     * 该方法目的为 “判断当前pos 是否为current_word 所指定的关键字”
     *
     * s为初始化ParserKeyword对象时传入。
     * 当前pos可以匹配到关键字，则返回true，否则返回false
     */
    const char * current_word = s;

    size_t s_length = strlen(s);
    if (!s_length)
        throw Exception("Logical error: keyword cannot be empty string", ErrorCodes::LOGICAL_ERROR);

    const char * s_end = s + s_length;

    while (true)
    {
        expected.add(pos, current_word);
        if (pos->type != TokenType::BareWord)
            return false;

        const char * next_whitespace = find_first_symbols<' ', '\0'>(current_word, s_end);
        size_t word_length = next_whitespace - current_word;

        // 先判断current_word关键字长度 和当前pos关键字长度是否一致，不一致直接返回false
        if (word_length != pos->size())
            return false;

        // 再判断当前pos关键字 和current_word关键字 是否相同，不相同直接返回false
        if (0 != strncasecmp(pos->begin, current_word, word_length))
            return false;

        // 至此current_word已成功匹配，原始sql pos后移
        ++pos;

        if (!*next_whitespace)
            break;

        current_word = next_whitespace + 1;
    }

    return true;
}

}

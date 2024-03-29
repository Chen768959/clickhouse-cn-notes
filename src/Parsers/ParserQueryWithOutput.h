#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Parse queries supporting [INTO OUTFILE 'file_name'] [FORMAT format_name] [SETTINGS key1 = value1, key2 = value2, ...] suffix.
class ParserQueryWithOutput : public IParserBase
{
protected:
    const char * end;
    ContextMutablePtr * context = nullptr;
    const char * getName() const override { return "Query with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    ParserQueryWithOutput(const char * end_) : end(end_) {}
    ParserQueryWithOutput(const char * end_, ContextMutablePtr * context_) : end(end_),context(context_) {}
};

}

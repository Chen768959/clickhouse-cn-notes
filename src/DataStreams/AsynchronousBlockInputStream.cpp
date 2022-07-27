#include "AsynchronousBlockInputStream.h"
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>
#include <common/logger_useful.h>

namespace DB
{

Block AsynchronousBlockInputStream::readImpl()
{
    /// If there were no calculations yet, calculate the first block synchronously
    if (!started)
    {
        calculate();
        started = true;
    }
    else    /// If the calculations are already in progress - wait for the result
        pool.wait();

    if (exception)
        std::rethrow_exception(exception);

    Block res = block;
    if (!res)
        return res;

    /// Start the next block calculation
    block.clear();
    next();

    return res;
}


void AsynchronousBlockInputStream::next()
{
    ready.reset();

    pool.scheduleOrThrowOnError([this, thread_group = CurrentThread::getGroup()]()
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

        try
        {
            if (first)
                setThreadName("AsyncBlockInput");

            /// AsynchronousBlockInputStream is used in Client which does not create queries and thread groups
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);
        }
        catch (...)
        {
            exception = std::current_exception();
            ready.set();
            return;
        }

        calculate();
    });
}


void AsynchronousBlockInputStream::calculate()
{
    try
    {
        if (first)
        {
            LOG_TRACE(log, "CUSTOM_TRACE START first BI READ：" + children.back()->getName());
            first = false;
            children.back()->readPrefix();
            LOG_TRACE(log, "CUSTOM_TRACE END   first BI READ：" + children.back()->getName());
        }

        LOG_TRACE(log, "CUSTOM_TRACE START BI READ：" + children.back()->getName());
        block = children.back()->read();
        LOG_TRACE(log, "CUSTOM_TRACE END   BI READ：" + children.back()->getName());
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    ready.set();
}

}


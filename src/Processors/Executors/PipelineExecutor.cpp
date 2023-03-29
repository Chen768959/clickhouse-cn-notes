#include <queue>
#include <IO/WriteBufferFromString.h>
#include <Common/EventCounter.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTracker.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/printPipeline.h>
#include <Processors/ISource.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <common/scope_guard_safe.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>

#ifndef NDEBUG
    #include <Common/Stopwatch.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXPIRED;
    extern const int QUERY_WAS_CANCELLED;
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXPIRED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

PipelineExecutor::PipelineExecutor(Processors & processors_, QueryStatus * elem)
    : processors(processors_)
    , cancelled(false)
    , finished(false)
    , num_processing_executors(0)
    , expand_pipeline_task(nullptr)
    , process_list_element(elem)
{
    try
    {
        graph = std::make_unique<ExecutingGraph>(processors);
    }
    catch (Exception & exception)
    {
        /// If exception was thrown while pipeline initialization, it means that query pipeline was not build correctly.
        /// It is logical error, and we need more information about pipeline.
        WriteBufferFromOwnString buf;
        printPipeline(processors, buf);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());

        throw;
    }
}

void PipelineExecutor::addChildlessProcessorsToStack(Stack & stack)
{
    UInt64 num_processors = processors.size();
    for (UInt64 proc = 0; proc < num_processors; ++proc)
    {
        if (graph->nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            graph->nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }
}

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}

void PipelineExecutor::addJob(ExecutingGraph::Node * execution_state)
{
    auto job = [execution_state]()
    {
        try
        {
            // Stopwatch watch;
            executeJob(execution_state->processor);
            // execution_state->execution_time_ns += watch.elapsed();

            ++execution_state->num_executed_jobs;
        }
        catch (...)
        {
            execution_state->exception = std::current_exception();
        }
    };

    execution_state->job = std::move(job);
}

bool PipelineExecutor::expandPipeline(Stack & stack, UInt64 pid)
{
    auto & cur_node = *graph->nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return false;
    }

    {
        std::lock_guard guard(processors_mutex);
        processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    }

    uint64_t num_processors = processors.size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < graph->nodes.size(); ++node)
    {
        direct_edge_sizes[node] = graph->nodes[node]->direct_edges.size();
        back_edges_sizes[node] = graph->nodes[node]->back_edges.size();
    }

    auto updated_nodes = graph->expandPipeline(processors);

    for (auto updated_node : updated_nodes)
    {
        auto & node = *graph->nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return true;
}

bool PipelineExecutor::tryAddProcessorToStackIfUpdated(ExecutingGraph::Edge & edge, Queue & queue, Queue & async_queue, size_t thread_number)
{
    /// In this method we have ownership on edge, but node can be concurrently accessed.

    auto & node = *graph->nodes[edge.to];

    std::unique_lock lock(node.status_mutex);

    ExecutingGraph::ExecStatus status = node.status;

    if (status == ExecutingGraph::ExecStatus::Finished)
        return true;

    if (edge.backward)
        node.updated_output_ports.push_back(edge.output_port_number);
    else
        node.updated_input_ports.push_back(edge.input_port_number);

    if (status == ExecutingGraph::ExecStatus::Idle)
    {
        node.status = ExecutingGraph::ExecStatus::Preparing;
        return prepareProcessor(edge.to, thread_number, queue, async_queue, std::move(lock));
    }
    else
        graph->nodes[edge.to]->processor->onUpdatePorts();

    return true;
}

/**
 * 执行*graph->nodes[pid].processor （Transform）算子的prepare()方法（该方法会从上游算子的inputPort中获取chunk数据块）
 * 并获取当前的算子状态，
 * 如果算子状态为Ready，则还会将该算子交由queue队列中，等待后续交由线程触发work逻辑
 *
 * 调用graph中Transform算子的prepare方法，获取当前该算子的”状态“，
 * 根据算子状态的不同，附以不同的node状态，
 * 如果算子状态为Ready，则将该node交予线程队列，执行该node中算子的work逻辑。
 *
 * @param pid 从graph中获取该pid index 的node
 * @param thread_number unknow
 * @param queue 将上面获取到的node装入此queue中（注意，只有ready状态的node才会直接存入queue）
 * @param async_queue unknow
 * @param node_lock unknow
 */
bool PipelineExecutor::prepareProcessor(UInt64 pid, size_t thread_number, Queue & queue, Queue & async_queue, std::unique_lock<std::mutex> node_lock)
{
    /// In this method we have ownership on node.
    auto & node = *graph->nodes[pid];

    bool need_expand_pipeline = false;

    std::vector<ExecutingGraph::Edge *> updated_back_edges;
    std::vector<ExecutingGraph::Edge *> updated_direct_edges;

    {
#ifndef NDEBUG
        Stopwatch watch;
#endif

        std::unique_lock<std::mutex> lock(std::move(node_lock));

        try
        {
            node.last_processor_status = node.processor->prepare(node.updated_input_ports, node.updated_output_ports);
        }
        catch (...)
        {
            node.exception = std::current_exception();
            return false;
        }

#ifndef NDEBUG
        node.preparation_time_ns += watch.elapsed();
#endif

        node.updated_input_ports.clear();
        node.updated_output_ports.clear();

        switch (node.last_processor_status)
        {
            case IProcessor::Status::NeedData:
            case IProcessor::Status::PortFull:
            {
                node.status = ExecutingGraph::ExecStatus::Idle;
                break;
            }
            case IProcessor::Status::Finished:
            {
                node.status = ExecutingGraph::ExecStatus::Finished;
                break;
            }
            case IProcessor::Status::Ready:
            {
                node.status = ExecutingGraph::ExecStatus::Executing;
                queue.push(&node);
                break;
            }
            case IProcessor::Status::Async:
            {
                node.status = ExecutingGraph::ExecStatus::Executing;
                async_queue.push(&node);
                break;
            }
            case IProcessor::Status::ExpandPipeline:
            {
                need_expand_pipeline = true;
                break;
            }
        }

        {
            for (auto & edge_id : node.post_updated_input_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_back_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            for (auto & edge_id : node.post_updated_output_ports)
            {
                auto * edge = static_cast<ExecutingGraph::Edge *>(edge_id);
                updated_direct_edges.emplace_back(edge);
                edge->update_info.trigger();
            }

            node.post_updated_input_ports.clear();
            node.post_updated_output_ports.clear();
        }
    }

    {
        for (auto & edge : updated_direct_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }

        for (auto & edge : updated_back_edges)
        {
            if (!tryAddProcessorToStackIfUpdated(*edge, queue, async_queue, thread_number))
                return false;
        }
    }

    // 只有ExpandPipeline状态的node会进此逻辑，并发执行pipline时不会进
    if (need_expand_pipeline)
    {
        Stack stack;

        executor_contexts[thread_number]->task_list.emplace_back(&node, &stack);

        ExpandPipelineTask * desired = &executor_contexts[thread_number]->task_list.back();
        ExpandPipelineTask * expected = nullptr;

        while (!expand_pipeline_task.compare_exchange_strong(expected, desired))
        {
            if (!doExpandPipeline(expected, true))
                return false;

            expected = nullptr;
        }

        if (!doExpandPipeline(desired, true))
            return false;

        /// Add itself back to be prepared again.
        stack.push(pid);

        while (!stack.empty())
        {
            auto item = stack.top();
            if (!prepareProcessor(item, thread_number, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[item]->status_mutex)))
                return false;

            stack.pop();
        }
    }

    return true;
}

bool PipelineExecutor::doExpandPipeline(ExpandPipelineTask * task, bool processing)
{
    std::unique_lock lock(task->mutex);

    if (processing)
        ++task->num_waiting_processing_threads;

    task->condvar.wait(lock, [&]()
    {
        return task->num_waiting_processing_threads >= num_processing_executors || expand_pipeline_task != task;
    });

    bool result = true;

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (expand_pipeline_task == task)
    {
        result = expandPipeline(*task->stack, task->node_to_expand->processors_id);

        expand_pipeline_task = nullptr;

        lock.unlock();
        task->condvar.notify_all();
    }

    return result;
}

void PipelineExecutor::cancel()
{
    cancelled = true;
    finish();

    std::lock_guard guard(processors_mutex);
    for (auto & processor : processors)
        processor->cancel();
}

void PipelineExecutor::finish()
{
    {
        std::lock_guard lock(task_queue_mutex);
        finished = true;
        async_task_queue.finish();
    }

    std::lock_guard guard(executor_contexts_mutex);

    for (auto & context : executor_contexts)
    {
        {
            std::lock_guard lock(context->mutex);
            context->wake_flag = true;
        }

        context->condvar.notify_one();
    }
}

void PipelineExecutor::execute(size_t num_threads)
{
    try
    {
        // 核心执行逻辑
        executeImpl(num_threads);

        /// Execution can be stopped because of exception. Check and rethrow if any.
        for (auto & node : graph->nodes)
            if (node->exception)
                std::rethrow_exception(node->exception);

        /// Exception which happened in executing thread, but not at processor.
        for (auto & executor_context : executor_contexts)
            if (executor_context->exception)
                std::rethrow_exception(executor_context->exception);
    }
    catch (...)
    {
#ifndef NDEBUG
        LOG_TRACE(log, "Exception while executing query. Current state:\n{}", dumpPipeline());
#endif
        throw;
    }

    finalizeExecution();
}

bool PipelineExecutor::executeStep(std::atomic_bool * yield_flag)
{
    if (finished)
        return false;

    if (!is_execution_initialized)
        initializeExecution(1);

    executeStepImpl(0, 1, yield_flag);

    if (!finished)
        return true;

    /// Execution can be stopped because of exception. Check and rethrow if any.
    for (auto & node : graph->nodes)
        if (node->exception)
            std::rethrow_exception(node->exception);

    finalizeExecution();

    return false;
}

void PipelineExecutor::finalizeExecution()
{
    if (process_list_element && process_list_element->isKilled())
        throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);

    if (cancelled)
        return;

    bool all_processors_finished = true;
    for (auto & node : graph->nodes)
    {
        if (node->status != ExecutingGraph::ExecStatus::Finished)
        {
            /// Single thread, do not hold mutex
            all_processors_finished = false;
            break;
        }
    }

    if (!all_processors_finished)
        throw Exception("Pipeline stuck. Current state:\n" + dumpPipeline(), ErrorCodes::LOGICAL_ERROR);
}

void PipelineExecutor::wakeUpExecutor(size_t thread_num)
{
    std::lock_guard guard(executor_contexts[thread_num]->mutex);
    executor_contexts[thread_num]->wake_flag = true;
    executor_contexts[thread_num]->condvar.notify_one();
}

// 单线程查询会进入此处，
// 多线程并发查询也是线程池中每个线程执行此处
void PipelineExecutor::executeSingleThread(size_t thread_num, size_t num_threads)
{
    executeStepImpl(thread_num, num_threads);

#ifndef NDEBUG
    auto & context = executor_contexts[thread_num];
    LOG_TRACE(log, "Thread finished. Total time: {} sec. Execution time: {} sec. Processing time: {} sec. Wait time: {} sec.", (context->total_time_ns / 1e9), (context->execution_time_ns / 1e9), (context->processing_time_ns / 1e9), (context->wait_time_ns / 1e9));
#endif
}

/**
 * 当前逻辑由线程池异步执行。
 * 从task_queue队列中获取待执行的node节点，（prepareProcessor()方法遍历整个graph图中的每一个node，并prepare()处理完后将需要执行的node放入此task_queue队列）
 * 执行该node中processor(Transorm算子)的work()逻辑，执行完毕后继续循环执行该node后续node中算子的work逻辑。
 * （算子的work()逻辑包含了算子针对prepare消费到的chunk的逻辑处理，也就是说每一个算子的work都用来处理上一个算子的输出数据。）
 *
 * 单线程查询会进入此处，
 * 多线程并发查询也是线程池中每个线程执行此处
 * @param thread_num 当前线程在整个线程队列中的index下标位置
 * @param num_threads 并发度，也是线程队列总数
 * @param yield_flag null
 */
void PipelineExecutor::executeStepImpl(size_t thread_num, size_t num_threads, std::atomic_bool * yield_flag)
{
#ifndef NDEBUG
    Stopwatch total_time_watch;
#endif

    auto & context = executor_contexts[thread_num];
    auto & node = context->node;
    bool yield = false;

    while (!finished && !yield)
    {
        /// First, find any processor to execute.
        /// Just traverse graph and prepare any processor.
        // 准备node----------------------------------------
        // 每次循环都从当前index线程中对应的task_queue中取出一个node，
        // 当取出一个node后跳出此循环
        while (!finished && node == nullptr)
        {
            {
                std::unique_lock lock(task_queue_mutex);

                if (!context->async_tasks.empty())
                {
                    node = context->async_tasks.front();
                    context->async_tasks.pop();
                    --num_waiting_async_tasks;

                    if (context->async_tasks.empty())
                        context->has_async_tasks = false;
                }
                else if (!task_queue.empty()){
                    // task_queue：”各个线程的task队列“ 的队列
                    // 找到thread_num下标线程对应的task队列，然后取出一个node可执行节点。
                    // （具体threads_queue来历及内容参考：initializeExecution(num_threads);）
                    node = task_queue.pop(thread_num);
                }

                if (node)
                {
                    if (!task_queue.empty() && !threads_queue.empty())
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();
                        wakeUpExecutor(thread_to_wake);
                    }

                    break;
                }

                if (threads_queue.size() + 1 == num_threads && async_task_queue.empty() && num_waiting_async_tasks == 0)
                {
                    lock.unlock();
                    finish();
                    break;
                }

#if defined(OS_LINUX)
                if (num_threads == 1)
                {
                    /// If we execute in single thread, wait for async tasks here.
                    auto res = async_task_queue.wait(lock);
                    if (!res)
                    {
                        /// The query had been cancelled (finished is also set)
                        if (finished)
                            break;
                        throw Exception("Empty task was returned from async task queue", ErrorCodes::LOGICAL_ERROR);
                    }

                    node = static_cast<ExecutingGraph::Node *>(res.data);
                    break;
                }
#endif

                threads_queue.push(thread_num);
            }

            {
                std::unique_lock lock(context->mutex);

                context->condvar.wait(lock, [&]
                {
                    return finished || context->wake_flag;
                });

                context->wake_flag = false;
            }
        }

        if (finished)
            break;

        // 执行node ---------------------------------------------------------------
        // 每次执行完当前node操作后，会判断是否执行后续node节点
        // （由yield控制，并发执行中，默认yield永远为false，即会不断执行当前node的后续节点）
//        int tmp_pd = 0;
//        if(node){
//            tmp_pd = node->processors_id;
//            LOG_DEBUG(log,"CUSTOM_TRACE START executeStep："+std::to_string(static_cast<int>(thread_num))+ "...proId："+std::to_string(static_cast<int>(tmp_pd))+"...proName:"+node->processor->getName());
//        }

        // 每次node执行完毕判断是否还有后续node，还有的话，则继续执行此循环
        while (node && !yield)
        {
            if (finished)
                break;

            addJob(node);

            {
#ifndef NDEBUG
                Stopwatch execution_time_watch;
#endif
                // 执行该node对应的process逻辑
                node->job();
#ifndef NDEBUG
                context->execution_time_ns += execution_time_watch.elapsed();
#endif
            }

            if (node->exception)
                cancel();

            if (finished)
                break;

#ifndef NDEBUG
            Stopwatch processing_time_watch;
#endif

            /// Try to execute neighbour processor.
            {
                Queue queue;
                Queue async_queue;

                ++num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, true);

                /// Prepare processor after execution.
                {
                    auto lock = std::unique_lock<std::mutex>(node->status_mutex);
                    // 将node->processors_id的下一个node装入queue
                    if (!prepareProcessor(node->processors_id, thread_num, queue, async_queue, std::move(lock)))
                        finish();
                }

                node = nullptr;

                /// Take local task from queue if has one.
                if (!queue.empty() && !context->has_async_tasks)
                {
                    node = queue.front();
                    queue.pop();
                }

                /// Push other tasks to global queue.
                if (!queue.empty() || !async_queue.empty())
                {
                    std::unique_lock lock(task_queue_mutex);

#if defined(OS_LINUX)
                    while (!async_queue.empty() && !finished)
                    {
                        async_task_queue.addTask(thread_num, async_queue.front(), async_queue.front()->processor->schedule());
                        async_queue.pop();
                    }
#endif

                    while (!queue.empty() && !finished)
                    {
                        task_queue.push(queue.front(), thread_num);
                        queue.pop();
                    }

                    if (!threads_queue.empty() && !task_queue.empty() && !finished)
                    {
                        auto thread_to_wake = task_queue.getAnyThreadWithTasks(thread_num + 1 == num_threads ? 0 : (thread_num + 1));

                        if (threads_queue.has(thread_to_wake))
                            threads_queue.pop(thread_to_wake);
                        else
                            thread_to_wake = threads_queue.popAny();

                        lock.unlock();

                        wakeUpExecutor(thread_to_wake);
                    }
                }

                --num_processing_executors;
                while (auto * task = expand_pipeline_task.load())
                    doExpandPipeline(task, false);
            }

#ifndef NDEBUG
            context->processing_time_ns += processing_time_watch.elapsed();
#endif

            /// We have executed single processor. Check if we need to yield execution.
            // 并发执行的情况下，不进入此逻辑，单个node执行完后，继续执行后续node
            if (yield_flag && *yield_flag)
                yield = true;
        }
//        LOG_DEBUG(log,"CUSTOM_TRACE END executeStep："+std::to_string(static_cast<int>(thread_num))+ "...proId："+std::to_string(static_cast<int>(tmp_pd)));
    }

#ifndef NDEBUG
    context->total_time_ns += total_time_watch.elapsed();
    context->wait_time_ns = context->total_time_ns - context->execution_time_ns - context->processing_time_ns;
#endif
}

void PipelineExecutor::initializeExecution(size_t num_threads)
{
    is_execution_initialized = true;

    // 线程队列
    threads_queue.init(num_threads);
    // 每个线程都对应一个task队列，此处定义一共有num_threads个task队列
    task_queue.init(num_threads);

    {
        std::lock_guard guard(executor_contexts_mutex);

        executor_contexts.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            executor_contexts.emplace_back(std::make_unique<ExecutorContext>());
    }

    Stack stack;
    // 获取一共存在多少个processors（也可以说是存在多少个graph->nodes），
    // 然后从0开始，递增模拟把这些index下标都推进stack
    // 后续根据此stack，就可以从第0位开始，取出graph中所有的node节点了
    addChildlessProcessorsToStack(stack);

    {
        std::lock_guard lock(task_queue_mutex);

        Queue queue;
        Queue async_queue;
        size_t next_thread = 0;

        while (!stack.empty())
        {
            UInt64 proc = stack.top();
            stack.pop();

            // 从stack中获取一个index（proc），然后找到该index在graph中的对应node，然后再装入queue队列中
            prepareProcessor(proc, 0, queue, async_queue, std::unique_lock<std::mutex>(graph->nodes[proc]->status_mutex));

            while (!queue.empty())
            {
                /**
                 * 将node，写入到next_thread线程对应的task队列中
                 */
                task_queue.push(queue.front(), next_thread);
                queue.pop();

                // 每次循环下一条线程index，超过总数就从头再开始，
                // 保证每条线程对应的task队列中的node数量基本一致
                ++next_thread;
                if (next_thread >= num_threads)
                    next_thread = 0;
            }

            while (!async_queue.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                                async_queue.front()->processor->getName());
        }
    }
}

void PipelineExecutor::executeImpl(size_t num_threads)
{
    OpenTelemetrySpanHolder span("PipelineExecutor::executeImpl()");

    /**
     * 首先：
     * Processor：他们是pipline里真正执行各种逻辑的对象
     * node：抽象化的执行节点，一个node对应一个Processor操作
     * *Graph：每次查询存在一个这种位图，里面包含了各阶段的node对象
     * task队列：每个线程都有一个对应的task队列，这个task队列里面包含了需要执行的node
     *
     * 此处初始化逻辑就是：
     * 创建了两个队列，所有线程存放的队列。  所有“task队列“ 的队列（每个线程对应一个task队列）
     * 利用“*Graph”，从中顺序取出node，
     * 挨个放入个线程的task队列队列中，
     * 尽量保证每一个线程的task队列中node的数量都一样多
     */
    initializeExecution(num_threads);

    using ThreadsData = std::vector<ThreadFromGlobalPool>;
    ThreadsData threads;
    threads.reserve(num_threads);

    bool finished_flag = false;

    SCOPE_EXIT_SAFE(
        if (!finished_flag)
        {
            finish();

            for (auto & thread : threads)
                if (thread.joinable())
                    thread.join();
        }
    );

    // 主逻辑 单线程则直接调用"executeSingleThread()"，多线程则创建线程池，线程池中每条线程执行"executeSingleThread()"
    if (num_threads > 1)
    {
        auto thread_group = CurrentThread::getGroup();

        // 根据并发线程数，创建线程并发执行
        for (size_t i = 0; i < num_threads; ++i)
        {
            // 创建一个ThreadFromGlobalPool对象
            // 包含当前executor对象，以及自身线程编号等
            threads.emplace_back([this, thread_group, thread_num = i, num_threads]
            {
                /// ThreadStatus thread_status;

                setThreadName("QueryPipelineEx");

                if (thread_group)
                    CurrentThread::attachTo(thread_group);

                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachQueryIfNotDetached();
                );

                try
                {
                    // （单线程执行是进入的也是此方法）
                    executeSingleThread(thread_num, num_threads);
                }
                catch (...)
                {
                    /// In case of exception from executor itself, stop other threads.
                    finish();
                    executor_contexts[thread_num]->exception = std::current_exception();
                }
            });
        }

#if defined(OS_LINUX)
        {
            /// Wait for async tasks.
            std::unique_lock lock(task_queue_mutex);
            while (auto task = async_task_queue.wait(lock))
            {
                auto * node = static_cast<ExecutingGraph::Node *>(task.data);
                executor_contexts[task.thread_num]->async_tasks.push(node);
                executor_contexts[task.thread_num]->has_async_tasks = true;
                ++num_waiting_async_tasks;

                if (threads_queue.has(task.thread_num))
                {
                    threads_queue.pop(task.thread_num);
                    wakeUpExecutor(task.thread_num);
                }
            }
        }
#endif

        for (auto & thread : threads)
            if (thread.joinable())
                thread.join();
    }
    else
        executeSingleThread(0, num_threads);

    finished_flag = true;
}

String PipelineExecutor::dumpPipeline() const
{
    for (const auto & node : graph->nodes)
    {
        {
            WriteBufferFromOwnString buffer;
            buffer << "(" << node->num_executed_jobs << " jobs";

#ifndef NDEBUG
            buffer << ", execution time: " << node->execution_time_ns / 1e9 << " sec.";
            buffer << ", preparation time: " << node->preparation_time_ns / 1e9 << " sec.";
#endif

            buffer << ")";
            node->processor->setDescription(buffer.str());
        }
    }

    std::vector<IProcessor::Status> statuses;
    std::vector<IProcessor *> proc_list;
    statuses.reserve(graph->nodes.size());
    proc_list.reserve(graph->nodes.size());

    for (const auto & node : graph->nodes)
    {
        proc_list.emplace_back(node->processor);
        statuses.emplace_back(node->last_processor_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(processors, statuses, out);
    out.finalize();

    return out.str();
}

}

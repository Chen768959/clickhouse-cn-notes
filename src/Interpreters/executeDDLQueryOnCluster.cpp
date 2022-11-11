#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/queryToString.h>
#include <Access/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNFINISHED;
    extern const int QUERY_IS_PROHIBITED;
    extern const int LOGICAL_ERROR;
}

bool isSupportedAlterType(int type)
{
    assert(type != ASTAlterCommand::NO_TYPE);
    static const std::unordered_set<int> unsupported_alter_types{
        /// It's dangerous, because it may duplicate data if executed on multiple replicas. We can allow it after #18978
        ASTAlterCommand::ATTACH_PARTITION,
        /// Usually followed by ATTACH PARTITION
        ASTAlterCommand::FETCH_PARTITION,
        /// Logical error
        ASTAlterCommand::NO_TYPE,
    };

    return unsupported_alter_types.count(type) == 0;
}


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, ContextPtr context)
{
    return executeDDLQueryOnCluster(query_ptr_, context, {});
}

BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, ContextPtr context, const AccessRightsElements & query_requires_access)
{
    return executeDDLQueryOnCluster(query_ptr, context, AccessRightsElements{query_requires_access});
}

/**
 * 将分布式ddl封装后上传到zk对应节点。
 * 后续由各ck的ddlworker线程异步获取并执行。
 * 此处根据当前ddl任务的zk节点，判断当前ddl任务的执行状况
 * @param query_ptr_ 此次查询的ast对象
 * @param context 上下文
 * @param query_requires_access 此次查询所必要的权限项
 */
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr_, ContextPtr context, AccessRightsElements && query_requires_access)
{
    /// Remove FORMAT <fmt> and INTO OUTFILE <file> if exists
    ASTPtr query_ptr = query_ptr_->clone();
    ASTQueryWithOutput::resetOutputASTIfExist(*query_ptr);

    // XXX: serious design flaw since `ASTQueryWithOnCluster` is not inherited from `IAST`!
    // 只有继承了ASTQueryWithOnCluster接口的ast类型，才能进行ddl操作
    auto * query = dynamic_cast<ASTQueryWithOnCluster *>(query_ptr.get());
    if (!query)
    {
        throw Exception("Distributed execution is not supported for such DDL queries", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (!context->getSettingsRef().allow_distributed_ddl)
        throw Exception("Distributed DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    if (const auto * query_alter = query_ptr->as<ASTAlterQuery>())
    {
        for (const auto & command : query_alter->command_list->children)
        {
            if (!isSupportedAlterType(command->as<ASTAlterCommand&>().type))
                throw Exception("Unsupported type of ALTER query", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    // 获取此次查询的cluster对象
    query->cluster = context->getMacros()->expand(query->cluster);
    ClusterPtr cluster = context->getCluster(query->cluster);

    /**
     * 所有分布式ddl操作都是通过DDLWorker异步执行的。
     * 其内部存在队列，会判断执行顺序
     */
    DDLWorker & ddl_worker = context->getDDLWorker();

    /// Enumerate hosts which will be used to send query.
    // 获取此cluster涉及的所有主机地址
    Cluster::AddressesWithFailover shards = cluster->getShardsAddresses();
    std::vector<HostID> hosts;
    for (const auto & shard : shards)
    {
        for (const auto & addr : shard)
            hosts.emplace_back(addr);
    }

    if (hosts.empty())
        throw Exception("No hosts defined to execute distributed DDL query", ErrorCodes::LOGICAL_ERROR);

    /// The current database in a distributed query need to be replaced with either
    /// the local current database or a shard's default database.
    // 判断此次查询是否指定了database，如果未指定，则为true
    bool need_replace_current_database = std::any_of(
        query_requires_access.begin(),
        query_requires_access.end(),
        [](const AccessRightsElement & elem) { return elem.isEmptyDatabase(); });

    bool use_local_default_database = false;
    const String & current_database = context->getCurrentDatabase();

    // 因为此次查询未指定database，
    // 所以尝试用配置中指定的shard replica的 “default_database”
    // 如果也未指定，则使用default库
    if (need_replace_current_database)
    {
        Strings shard_default_databases;
        for (const auto & shard : shards)
        {
            for (const auto & addr : shard)
            {
                if (!addr.default_database.empty())
                    shard_default_databases.push_back(addr.default_database);
                else
                    use_local_default_database = true;
            }
        }
        std::sort(shard_default_databases.begin(), shard_default_databases.end());
        shard_default_databases.erase(std::unique(shard_default_databases.begin(), shard_default_databases.end()), shard_default_databases.end());
        assert(use_local_default_database || !shard_default_databases.empty());

        if (use_local_default_database && !shard_default_databases.empty())
            throw Exception("Mixed local default DB and shard default DB in DDL query", ErrorCodes::NOT_IMPLEMENTED);

        if (use_local_default_database)
        {
            query_requires_access.replaceEmptyDatabase(current_database);
        }
        else
        {
            for (size_t i = 0; i != query_requires_access.size();)
            {
                auto & element = query_requires_access[i];
                if (element.isEmptyDatabase())
                {
                    query_requires_access.insert(query_requires_access.begin() + i + 1, shard_default_databases.size() - 1, element);
                    for (size_t j = 0; j != shard_default_databases.size(); ++j)
                        query_requires_access[i + j].replaceEmptyDatabase(shard_default_databases[j]);
                    i += shard_default_databases.size();
                }
                else
                    ++i;
            }
        }
    }

    AddDefaultDatabaseVisitor visitor(current_database, !use_local_default_database);
    visitor.visitDDL(query_ptr);

    /// Check access rights, assume that all servers have the same users config
    // 检查用户是否拥有“query_requires_access”中必须的所有权限项
    context->checkAccess(query_requires_access);

    DDLLogEntry entry;
    entry.hosts = std::move(hosts);
    entry.query = queryToString(query_ptr);
    entry.initiator = ddl_worker.getCommonHostID();
    entry.setSettingsIfRequired(context);

    /**
     * 此处只是在zk上创建此次ddl请求的对应task路径，并创建其下的finished和active两个子路径。
     * task名称格式为“query-xxxxxxx”，新增task 名称后缀的数字不断递增。
     * 创建好后返回节点在zk上的路径，后续可根据该路劲内的信息，判断该ddl task执行状况与是否全部完成。
     */
    String node_path = ddl_worker.enqueueQuery(entry);

    // 将专门的分布式ddl-InputStream封装进BlockIO，
    // inputStream的readImpl接口方法会不断查询该node_path（ddl task任务地址）下的信息，
    // 直到知道该ddl任务在全局成功或失败
    return getDistributedDDLStatus(node_path, entry, context);
}

 /**
  * 返回BlockIO
  * 其中包含inputStream对象，后续可对其操作，来根据zk中的node_path ddl任务路径中的信息，判断当前ddl任务在全局中的执行状态
  * @param node_path 当前ddl请求在zk上的对应node路径
  * @param entry 包含query ast的封装ddl对象
  * @param context context
  * @return 一般情况下为 DDLQueryStatusInputStream （distributed_ddl_output_mode=null时，则创建NullAndDoCopyBlockInputStream，不输出任何响应结果）
  */
BlockIO getDistributedDDLStatus(const String & node_path, const DDLLogEntry & entry, ContextPtr context, const std::optional<Strings> & hosts_to_wait)
{
    BlockIO io;
    if (context->getSettingsRef().distributed_ddl_task_timeout == 0)
        return io;

    /**
     * 创建专门的分布式ddl查询的InputStream，
     * 该stream的readImpl接口方法会不断查询zk上该ddl task的节点状况，
     * 判断该ddl 任务是否执行完成或者失败
     */
    BlockInputStreamPtr stream = std::make_shared<DDLQueryStatusInputStream>(node_path, entry, context, hosts_to_wait);
    // 如果distributed_ddl_output_mode参数设置的none，
    // 则不对外返回ddl查询结果（有报错的话，还是会抛出异常）
    if (context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::NONE)
    {
        /// Wait for query to finish, but ignore output
        auto null_output = std::make_shared<NullBlockOutputStream>(stream->getHeader());
        stream = std::make_shared<NullAndDoCopyBlockInputStream>(std::move(stream), std::move(null_output));
    }

    io.in = std::move(stream);
    return io;
}

DDLQueryStatusInputStream::DDLQueryStatusInputStream(const String & zk_node_path, const DDLLogEntry & entry, ContextPtr context_,
                                                     const std::optional<Strings> & hosts_to_wait)
    : node_path(zk_node_path)
    , context(context_)
    , watch(CLOCK_MONOTONIC_COARSE)
    , log(&Poco::Logger::get("DDLQueryStatusInputStream"))
{
    if (context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::THROW ||
        context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::NONE)
        throw_on_timeout = true;
    else if (context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::NULL_STATUS_ON_TIMEOUT ||
             context->getSettingsRef().distributed_ddl_output_mode == DistributedDDLOutputMode::NEVER_THROW)
        throw_on_timeout = false;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown output mode");

    auto maybe_make_nullable = [&](const DataTypePtr & type) -> DataTypePtr
    {
        if (throw_on_timeout)
            return type;
        return std::make_shared<DataTypeNullable>(type);
    };

    sample = Block{
        {std::make_shared<DataTypeString>(),                         "host"},
        {std::make_shared<DataTypeUInt16>(),                         "port"},
        {maybe_make_nullable(std::make_shared<DataTypeInt64>()),     "status"},
        {maybe_make_nullable(std::make_shared<DataTypeString>()),    "error"},
        {std::make_shared<DataTypeUInt64>(),                         "num_hosts_remaining"},
        {std::make_shared<DataTypeUInt64>(),                         "num_hosts_active"},
    };

    if (hosts_to_wait)
    {
        waiting_hosts = NameSet(hosts_to_wait->begin(), hosts_to_wait->end());
        by_hostname = false;
        sample.erase("port");
    }
    else
    {
        for (const HostID & host : entry.hosts)
            waiting_hosts.emplace(host.toString());
    }

    addTotalRowsApprox(waiting_hosts.size());

    timeout_seconds = context->getSettingsRef().distributed_ddl_task_timeout;
}

std::pair<String, UInt16> DDLQueryStatusInputStream::parseHostAndPort(const String & host_id) const
{
    String host = host_id;
    UInt16 port = 0;
    if (by_hostname)
    {
        auto host_and_port = Cluster::Address::fromString(host_id);
        host = host_and_port.first;
        port = host_and_port.second;
    }
    return {host, port};
}

Block DDLQueryStatusInputStream::readImpl()
{
    Block res;
    bool all_hosts_finished = num_hosts_finished >= waiting_hosts.size();
    /// Seems like num_hosts_finished cannot be strictly greater than waiting_hosts.size()
    assert(num_hosts_finished <= waiting_hosts.size());
    if (all_hosts_finished || timeout_exceeded)
    {
        bool throw_if_error_on_host = context->getSettingsRef().distributed_ddl_output_mode != DistributedDDLOutputMode::NEVER_THROW;
        if (first_exception && throw_if_error_on_host)
            throw Exception(*first_exception);

        return res;
    }

    auto zookeeper = context->getZooKeeper();
    size_t try_number = 0;

    while (res.rows() == 0)
    {
        if (isCancelled())
        {
            bool throw_if_error_on_host = context->getSettingsRef().distributed_ddl_output_mode != DistributedDDLOutputMode::NEVER_THROW;
            if (first_exception && throw_if_error_on_host)
                throw Exception(*first_exception);

            return res;
        }

        if (timeout_seconds >= 0 && watch.elapsedSeconds() > timeout_seconds)
        {
            size_t num_unfinished_hosts = waiting_hosts.size() - num_hosts_finished;
            size_t num_active_hosts = current_active_hosts.size();

            constexpr const char * msg_format = "Watching task {} is executing longer than distributed_ddl_task_timeout (={}) seconds. "
                                                "There are {} unfinished hosts ({} of them are currently active), "
                                                "they are going to execute the query in background";
            if (throw_on_timeout)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, msg_format,
                                node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);

            timeout_exceeded = true;
            LOG_INFO(log, msg_format, node_path, timeout_seconds, num_unfinished_hosts, num_active_hosts);

            NameSet unfinished_hosts = waiting_hosts;
            for (const auto & host_id : finished_hosts)
                unfinished_hosts.erase(host_id);

            /// Query is not finished on the rest hosts, so fill the corresponding rows with NULLs.
            MutableColumns columns = sample.cloneEmptyColumns();
            for (const String & host_id : unfinished_hosts)
            {
                auto [host, port] = parseHostAndPort(host_id);
                size_t num = 0;
                columns[num++]->insert(host);
                if (by_hostname)
                    columns[num++]->insert(port);
                columns[num++]->insert(Field{});
                columns[num++]->insert(Field{});
                columns[num++]->insert(num_unfinished_hosts);
                columns[num++]->insert(num_active_hosts);
            }
            res = sample.cloneWithColumns(std::move(columns));
            return res;
        }

        if (num_hosts_finished != 0 || try_number != 0)
        {
            sleepForMilliseconds(std::min<size_t>(1000, 50 * (try_number + 1)));
        }

        if (!zookeeper->exists(node_path))
        {
            throw Exception(ErrorCodes::UNFINISHED,
                            "Cannot provide query execution status. The query's node {} has been deleted by the cleaner since it was finished (or its lifetime is expired)",
                            node_path);
        }

        Strings new_hosts = getNewAndUpdate(getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "finished"));
        ++try_number;
        if (new_hosts.empty())
            continue;

        current_active_hosts = getChildrenAllowNoNode(zookeeper, fs::path(node_path) / "active");

        MutableColumns columns = sample.cloneEmptyColumns();
        for (const String & host_id : new_hosts)
        {
            ExecutionStatus status(-1, "Cannot obtain error message");
            {
                String status_data;
                if (zookeeper->tryGet(fs::path(node_path) / "finished" / host_id, status_data))
                    status.tryDeserializeText(status_data);
            }

            auto [host, port] = parseHostAndPort(host_id);

            if (status.code != 0 && first_exception == nullptr)
                first_exception = std::make_unique<Exception>(status.code, "There was an error on [{}:{}]: {}", host, port, status.message);

            ++num_hosts_finished;

            size_t num = 0;
            columns[num++]->insert(host);
            if (by_hostname)
                columns[num++]->insert(port);
            columns[num++]->insert(status.code);
            columns[num++]->insert(status.message);
            columns[num++]->insert(waiting_hosts.size() - num_hosts_finished);
            columns[num++]->insert(current_active_hosts.size());
        }
        res = sample.cloneWithColumns(std::move(columns));
    }

    return res;
}

Strings DDLQueryStatusInputStream::getChildrenAllowNoNode(const std::shared_ptr<zkutil::ZooKeeper> & zookeeper, const String & node_path)
{
    Strings res;
    Coordination::Error code = zookeeper->tryGetChildren(node_path, res);
    if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNONODE)
        throw Coordination::Exception(code, node_path);
    return res;
}

Strings DDLQueryStatusInputStream::getNewAndUpdate(const Strings & current_list_of_finished_hosts)
{
    Strings diff;
    for (const String & host : current_list_of_finished_hosts)
    {
        if (!waiting_hosts.count(host))
        {
            if (!ignoring_hosts.count(host))
            {
                ignoring_hosts.emplace(host);
                LOG_INFO(log, "Unexpected host {} appeared  in task {}", host, node_path);
            }
            continue;
        }

        if (!finished_hosts.count(host))
        {
            diff.emplace_back(host);
            finished_hosts.emplace(host);
        }
    }

    return diff;
}


}

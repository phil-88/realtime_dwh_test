
#include "vhash.h"
#include "utils.h"
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

#include "sink/csv.h"
#ifdef WITH_ORC
#   include "sink/orc.h"
#endif
#ifdef WITH_CH
#   include "sink/clickhouse.h"
#endif
#define ALL_PARTITIONS -1
#define REBALANCE_PARTITIONS -2
#define REBALANCE_AUTOCOMMIT true

#define POLL_TIMEOUT 30000
#define MAX_DURATION 300000
#define NOTINT INT_MAX


using namespace std;
using namespace cppkafka;


// kafka consumer

struct PartitionTask
{
    PartitionTask() : partition(INT_MAX), offset(-1), limit(0), isNull(true), commit(false) {}

    int partition;
    int64 offset;
    int64 limit;
    bool isNull;
    bool commit;
};

class KafkaSource
{
    Consumer *consumer;

    const std::string brokers, topic, group, securityProtocol, saslMechanisms, kafkaUser, kafkaPassword, caLocation;

    std::vector<PartitionTask> partitions;
    int64 limit;

    Sink *sink;

    const int pollTimeout;
    const int maxDuration;
    const std::string lockfile, autoOffsetReset;
    bool timedout;
    int duration;
    bool subscribe;

public:
    KafkaSource(std::string brokers, std::string topic, std::string group, std::string securityProtocol, std::string saslMechanisms,
                std::string kafkaUser, std::string kafkaPassword, std::string caLocation, std::string partitions,
                Sink *sink, long long pollTimeout, long long maxDuration, std::string lockfile,
                std::string autoOffsetReset)
        : consumer(NULL), brokers(brokers), topic(topic), group(group), securityProtocol(securityProtocol),
          saslMechanisms(saslMechanisms), kafkaUser(kafkaUser), kafkaPassword(kafkaPassword), caLocation(caLocation),
          limit(0), sink(sink), pollTimeout(pollTimeout), maxDuration(maxDuration), lockfile(lockfile),
          autoOffsetReset(autoOffsetReset)
    {
        parsePartitions(partitions);
    }

    void setup()
    {
        this->subscribe = partitions.size() == 1 && partitions[0].partition == REBALANCE_PARTITIONS;
        const bool autocommit = subscribe && REBALANCE_AUTOCOMMIT;
        const bool manualOffsets = partitions.size() > 0 && partitions[0].offset >= 0;

        this->timedout = false;
        this->duration = 0;

        Configuration config = {
            { "metadata.broker.list", brokers },
            { "group.id", group },
            { "enable.auto.commit", autocommit },
            { "enable.auto.offset.store", autocommit },
            { "auto.offset.reset", manualOffsets ? "error" : autoOffsetReset },
        };

        if (securityProtocol != "" && saslMechanisms != "" && kafkaUser != "" && kafkaPassword != "" && caLocation != "")
        {
            config.set("security.protocol", securityProtocol);
            config.set("sasl.mechanisms", saslMechanisms);
            config.set("sasl.username", kafkaUser);
            config.set("sasl.password", kafkaPassword);
            config.set("ssl.ca.location", caLocation);
            config.set("api.version.request", true);
        }

        consumer = new Consumer(config);

        // convert partition list to indexed map
        int topicPartitionCount = getPartitionCount(consumer->get_handle(), topic.c_str());
        std::vector<PartitionTask> partitionList = partitions;
        partitions.clear();
        partitions.resize(topicPartitionCount);

        for (PartitionTask p : partitionList)
        {
            int minPartition = p.partition >= 0 ? p.partition : 0;
            int maxPartition = p.partition >= 0 ? p.partition : topicPartitionCount - 1;
            for (int partitionNo = minPartition; partitionNo <= maxPartition; ++partitionNo)
            {
                p.partition = partitionNo;
                p.commit = p.commit && !autocommit;
                partitions[p.partition] = p;
            }
        }

        if (subscribe)
        {
            // substribe for partitions from broker
            consumer->set_assignment_callback([&](const TopicPartitionList& partitions) {

                std::vector<PartitionTask> assignedPartitions;
                int partitionCount = 1;

                for (TopicPartition p : partitions)
                {
                    PartitionTask t;
                    t.isNull = false;
                    t.partition = p.get_partition();
                    t.offset = -1000;
                    t.limit = 0;
                    t.commit = !REBALANCE_AUTOCOMMIT;
                    assignedPartitions.push_back(t);

                    partitionCount = max(partitionCount, t.partition + 1);
                    fprintf(stderr, "partition %d: rebalance assign %s\n", 
                            t.partition, t.commit ? "manual commit" : "autocommit");
                }

                this->partitions.clear();
                this->partitions.resize(partitionCount);
                for (PartitionTask p : assignedPartitions)
                {
                    this->partitions[p.partition] = p;
                }
            });

            consumer->set_revocation_callback([&](const TopicPartitionList& partitions) {
                this->commitOffsets();
            });

            consumer->subscribe({ topic });
        }
        else
        {
            // assign specific partitions
            TopicPartitionList offsets;
            for (PartitionTask &p : partitions)
            {
                if (!p.isNull)
                {
                    offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                    fprintf(stderr, "partition %d: assign %ld limit %ld %s\n", 
                            p.partition, p.offset, p.limit, p.commit ? "manual commit" : "no commit");
                }
            }

            consumer->assign(offsets);
        }
    }

    void destroy()
    {
        if (subscribe)
        {
            consumer->unsubscribe();
        }
    }

    virtual void process()
    {
        int64 currentLimit = getCurrentLimit();
        //fprintf(stderr, "Consuming messages from topic %s limit %lld", topic.c_str(), currentLimit);
        int batchSize = max(0, min((int)currentLimit, 100000));

        while (getCurrentLimit() > 0 && !timedout && duration < maxDuration &&
               (lockfile.empty() || fileExists(lockfile)))
        {
            auto start = std::chrono::steady_clock::now();
            std::vector<Message> msgs = consumer->poll_batch(batchSize, std::chrono::milliseconds(pollTimeout));
            auto end = std::chrono::steady_clock::now();

            auto d = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            timedout = (d >= pollTimeout);
            duration += d;

            fprintf(stderr, "%lu messages polled: %ld ms poll, %d ms total\n", msgs.size(), d, duration);
            start = std::chrono::steady_clock::now();
            for (Message &msg: msgs)
            {
                if (!msg)
                {
                    continue;
                }

                if (msg.get_error() && !msg.is_eof())
                {
                    fprintf(stderr, "error recieived: %s\n", msg.get_error().to_string().c_str());
                    continue;
                }

                int part = msg.get_partition();

                partitions[part].limit -= 1;
                partitions[part].offset = msg.get_offset() + 1;

                limit -= 1;
                sink->put(msg);
                if (sink->isFlushed())
                {
                    commitOffsets();
                }
            }
            end = std::chrono::steady_clock::now();
            duration += std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        }

        fprintf(stderr, timedout || duration > maxDuration ? "timeout\n" : "limit exceeded\n");
        sink->flush();
        commitOffsets();
    }

private:

    int getPartitionCount(rd_kafka_t* rdkafka, const char *topic_name)
    {
        int partitionCount = 0;

        rd_kafka_topic_t *rdtopic = rd_kafka_topic_new(rdkafka, topic_name, 0);
        const rd_kafka_metadata_t *rdmetadata;

        rd_kafka_resp_err_t err = rd_kafka_metadata(rdkafka, 0, rdtopic, &rdmetadata, 30000);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            for (int i = 0; i < rdmetadata->topic_cnt; ++i)
            {
                partitionCount = rdmetadata->topics[i].partition_cnt;
            }
            rd_kafka_metadata_destroy(rdmetadata);
        }

        rd_kafka_topic_destroy(rdtopic);

        return partitionCount;
    }

    void parsePartitions(const std::string &partFmt)
    {
        this->limit = 0;

        std::map<std::string, int> partConsts;
        partConsts["*"] = ALL_PARTITIONS;
        partConsts["%"] = REBALANCE_PARTITIONS;

        std::vector<std::string> fmtParts = splitString(partFmt, ',');

        std::vector<PartitionTask> partitionList;
        for (std::string part : fmtParts)
        {
            std::vector<std::string> tuple = splitString(part, ':');
            if (tuple.size() != 3)
            {
                printf("task format missmatch: [partition:offset:limit](,[partition:offset:limit])*");
            }

            std::vector<std::string> partInterval = splitString(tuple[0], '-');
            if (partInterval.size() > 2)
            {
                printf("partition format missmatch: [*|%%|digit|digit-digit]");
            }
            std::string intStart = partInterval[0];
            std::string intEnd = partInterval[partInterval.size() - 1];
            
            int minPartition = partConsts.count(intStart) ? partConsts[intStart] : toInt(intStart, -1);
            int maxPartition = partConsts.count(intEnd) ? partConsts[intEnd] : toInt(intEnd, -1);

            int partLimit = toInt(tuple[2]);
            this->limit += partLimit;

            PartitionTask t;
            t.isNull = false;
            t.partition = minPartition;
            t.offset = toInt(tuple[1]);
            t.limit = minPartition >= 0 && maxPartition == minPartition ? partLimit : 0;
            t.commit = t.offset < 0;
            
            if (t.partition < 0 && tuple[0] != std::string("*") && tuple[0] != std::string("%"))
            {
                vt_report_error(0, "partition number must be integer, '*' or '%%'");
            }
            else if (t.partition < 0 && fmtParts.size() > 1)
            {
                vt_report_error(0, "only one partition clause is expected for '*' or '%%'");
            }
            else if (t.offset == NOTINT || (t.offset < 0 && t.offset != -1 && t.offset != -2 && t.offset != -1000))
            {
                vt_report_error(0, "partition offset must be positive integer or -1 for latest or -2 for earlest or -1000 for last read");
            }
            else if (t.partition == REBALANCE_PARTITIONS && t.offset != -1000)
            {
                vt_report_error(0, "subscribe is only available with offset -1000 (last read)");
            }
            else if (t.limit == NOTINT || t.limit < 0)
            {
                vt_report_error(0, "partition limit must be positive integer");
            }
            else
            {
                for (int p = minPartition; p <= maxPartition; ++p)
                {
                    t.partition = p;
                    partitionList.push_back(t);
                }
            }
        }

        partitions = partitionList;
    }

    int64 getCurrentLimit() const
    {
        int64 partitionLimit = 0;
        for (PartitionTask p : partitions)
        {
            if (!p.isNull && p.limit > 0)
            {
                partitionLimit += p.limit;
            }
        }
        return max((int64)0, max(limit, partitionLimit));
    }

    void commitOffsets()
    {
        TopicPartitionList offsets;
        for (PartitionTask &p : partitions)
        {
            if (!p.isNull && p.commit && p.offset > 0)
            {
                offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                p.offset = -1;
            }
        }
        if (!offsets.empty())
        {
            consumer->store_offsets(offsets);
            consumer->commit(offsets);
        }
    }
};


int main(int argc, char **argv)
{
    string brokers;
    string topic;
    string group;
    string securityProtocol;
    string saslMechanisms;
    string kafkaUser;
    string kafkaPassword;
    string caLocation;
    string task;
    long long pollTimeout = POLL_TIMEOUT;
    long long maxDuration = MAX_DURATION;

    string filename;
    string format;
    string columns;
    string delim = "|";
    string newline = "\n";

    string host;
    int port = -1;
    string user;
    string password;
    string database;
    string table;
    string columnsTable = "system.columns";
    int batchSize = 500000;
    int threadCount = 1;

    string lockfile;
    string autoOffsetReset = "error";

    for (int i = 1; i < argc; ++i)
    {
        if (strcmp(argv[i], "--help") == 0)
        {
            printf("For ORC: \n"
                   "%s \\\n"
                   " --brokers clickstream-kafka01:9092 \\\n"
                   " --topic user-keyed-clickstream \\\n"
                   " --group test-group \\\n"
                   " --task \"%%:-1000:1000000\" \\\n"
                   " --filename file.orc \\\n"
                   " --format orc \\\n"
                   " --columns \"src_id:bigint,src:bigint,u:string,uid:bigint,eid:bigint,dt:bigint,dtm:float\" \n"
                   "\n"
                   "For CSV: \n"
                   "%s \\\n"
                   " --brokers clickstream-kafka01:9092 \\\n"
                   " --topic user-keyed-clickstream \\\n"
                   " --group test-group \\\n"
                   " --task \"%%:-1000:1000000\" \\\n"
                   " --format csv \\\n"
                   " --columns \"timestamp;columns:hash(uuid),hash(u),uid,eid,dt,dtm,src_id,src,hash(ua)\" \n"
                   "\n"
                   "For Clickhouse: \n"
                   "%s \\\n"
                   " --brokers clickstream-kafka01:9092 \\\n"
                   " --topic user-keyed-clickstream \\\n"
                   " --group test-group \\\n"
                   " --task \"%%:-1000:1000000\" \\\n"
                   " --format clickhouse \\\n"
                   " --host localhost \\\n"
                   " --port 9000 \\\n"
                   " --user username \\\n"
                   " --password pwd \\\n"
                   " --database dwh \\\n"
                   " --table dwh.clickstream \n"
                   , argv[0], argv[0], argv[0]);
        }
        else if (strcmp(argv[i], "--max-duration") == 0 && i + 1 < argc)
        {
            maxDuration = atoll(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--poll-timeout") == 0 && i + 1 < argc)
        {
            pollTimeout = atoll(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--brokers") == 0 && i + 1 < argc)
        {
            brokers = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--security-protocol") == 0 && i + 1 < argc)
        {
            securityProtocol = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--sasl-mechanisms") == 0 && i + 1 < argc)
        {
            saslMechanisms = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--kafka-user") == 0 && i + 1 < argc)
        {
            kafkaUser = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--kafka-password") == 0 && i + 1 < argc)
        {
            kafkaPassword = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--ssl-ca-location") == 0 && i + 1 < argc)
        {
            caLocation = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--start-earliest") == 0)
        {
            autoOffsetReset = "earliest";
        }
        else if (strcmp(argv[i], "--topic") == 0 && i + 1 < argc)
        {
            topic = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--group") == 0 && i + 1 < argc)
        {
            group = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--task") == 0 && i + 1 < argc)
        {
            task = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--filename") == 0 && i + 1 < argc)
        {
            filename = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--format") == 0 && i + 1 < argc)
        {
            format = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--columns") == 0 && i + 1 < argc)
        {
            columns = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--delimiter") == 0 && i + 1 < argc)
        {
            delim = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--newline") == 0 && i + 1 < argc)
        {
            newline = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc)
        {
            host = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
        {
            port = atol(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--database") == 0 && i + 1 < argc)
        {
            database = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--table") == 0 && i + 1 < argc)
        {
            table = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--columns-table") == 0 && i + 1 < argc)
        {
            columnsTable = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--batch-size") == 0 && i + 1 < argc)
        {
            batchSize = atol(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--thread-count") == 0 && i + 1 < argc)
        {
            threadCount = atol(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--user") == 0 && i + 1 < argc)
        {
            user = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--password") == 0 && i + 1 < argc)
        {
            password = string(argv[i + 1]);
        }
        else if (strcmp(argv[i], "--lockfile") == 0 && i + 1 < argc)
        {
            lockfile = string(argv[i + 1]);
        }
    }

    if (user.size() > 1 && user[0] == '$')
    {
        user = string(getenv(user.substr(1).c_str()));
    }

    if (password.size() > 1 && password[0] == '$')
    {
        password = string(getenv(password.substr(1).c_str()));
    }

    if (securityProtocol.size() > 1 && securityProtocol[0] == '$')
    {
        securityProtocol = string(getenv(securityProtocol.substr(1).c_str()));
    }
    if (saslMechanisms.size() > 1 && saslMechanisms[0] == '$')
    {
        saslMechanisms = string(getenv(saslMechanisms.substr(1).c_str()));
    }
    if (kafkaUser.size() > 1 && kafkaUser[0] == '$')
    {
        kafkaUser = string(getenv(kafkaUser.substr(1).c_str()));
    }
    if (kafkaPassword.size() > 1 && kafkaPassword[0] == '$')
    {
        kafkaPassword = string(getenv(kafkaPassword.substr(1).c_str()));
    }
    if (caLocation.size() > 1 && caLocation[0] == '$')
    {
        caLocation = string(getenv(caLocation.substr(1).c_str()));
    }

    Sink *sink = NULL;
    if (format == "csv")
    {
        sink = new CSVSink(columns, delim, newline);
    }

#ifdef WITH_ORC
    if (format == "orc")
    {
        sink = new ORCSink(columns, filename);
    }
#endif

#ifdef WITH_CH
    if (format == "clickhouse")
    {
        sink = new ClickhouseSink(table, columnsTable, host, port, database, user, password, batchSize, threadCount);
    }
#endif

    if (sink == NULL)
    {
        return -1;
    }
    KafkaSource src(brokers, topic, group, securityProtocol, saslMechanisms, kafkaUser, kafkaPassword,
                    caLocation, task, sink, pollTimeout, maxDuration, lockfile, autoOffsetReset);
    src.setup();
    src.process();
    src.destroy();
    return 0;
}

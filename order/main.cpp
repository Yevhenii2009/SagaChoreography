#include <iostream>
#include <string>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

int main()
{
    cout << "[ORDER] Service started!" << endl;

    string brokers = "kafka:9092";

    string outputTopic = "order-created";

    string successTopic = "delivery-checked";
    string failureTopic = "account-compensated";

    string errorString;

    // =========================
    // Create producer
    // =========================

    RdKafka::Conf* producerConfig =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    producerConfig->set(
        "bootstrap.servers",
        brokers,
        errorString
    );

    RdKafka::Producer* producer =
        RdKafka::Producer::create(
            producerConfig,
            errorString
        );

    if (!producer)
    {
        cerr << "[ORDER] Failed to create producer: "
             << errorString << endl;

        return 1;
    }

    // =========================
    // Create consumer
    // =========================

    RdKafka::Conf* consumerConfig =
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    consumerConfig->set(
        "bootstrap.servers",
        brokers,
        errorString
    );

    consumerConfig->set(
        "group.id",
        "order-failure-demo",
        errorString
    );

    consumerConfig->set(
        "auto.offset.reset",
        "latest",
        errorString
    );

    RdKafka::KafkaConsumer* consumer =
        RdKafka::KafkaConsumer::create(
            consumerConfig,
            errorString
        );

    if (!consumer)
    {
        cerr << "[ORDER] Failed to create consumer: "
             << errorString << endl;

        return 1;
    }

    // Subscribe to success and failure events
    RdKafka::ErrorCode result =
        consumer->subscribe({
            successTopic,
            failureTopic
        });

    if (result != RdKafka::ERR_NO_ERROR)
    {
        cerr << "[ORDER] Failed to subscribe: "
             << RdKafka::err2str(result) << endl;

        return 1;
    }

    cout << "[ORDER] Connected to Kafka!" << endl;

    // =========================
    // Wait for partition assignment
    // =========================

    cout << "[ORDER] Waiting for Kafka partition assignment..."
         << endl;

    while (true)
    {
        vector<RdKafka::TopicPartition*> partitions;

        consumer->assignment(partitions);

        if (!partitions.empty())
        {
            cout << "[ORDER] Partition assigned!" << endl;

            RdKafka::TopicPartition::destroy(partitions);
            break;
        }

        RdKafka::Message* message =
            consumer->consume(100);

        delete message;
    }

    // =========================
    // Create order
    // =========================

    string order = "ORDER_CREATED";

    cout << "[ORDER] Creating order..." << endl;

    result = producer->produce(
        outputTopic,
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(order.c_str()),
        order.size(),
        nullptr,
        0,
        0,
        nullptr
    );

    if (result == RdKafka::ERR_NO_ERROR)
    {
        cout << "[ORDER] Sent event: "
             << order << endl;
    }
    else
    {
        cerr << "[ORDER] Failed to send event: "
             << RdKafka::err2str(result) << endl;
    }

    producer->flush(5000);

    cout << "[ORDER] Waiting for Saga result..." << endl;

    // =========================
    // Wait for final event
    // =========================

    while (true)
    {
        RdKafka::Message* message =
            consumer->consume(1000);

        if (message->err() == RdKafka::ERR_NO_ERROR)
        {
            string event(
                static_cast<const char*>(message->payload()),
                message->len()
            );

            cout << "[ORDER] Received event: "
                 << event << endl;

            // =========================
            // Successful Saga
            // =========================

            if (event == "DELIVERY_CHECKED")
            {
                cout << "[ORDER] ORDER SUCCESS!" << endl;

                delete message;
                break;
            }

            // =========================
            // Failed Saga
            // =========================

            if (event == "ACCOUNT_COMPENSATED")
            {
                cout << "[ORDER] Compensation completed!"
                     << endl;

                cout << "[ORDER] ORDER FAILED!" << endl;

                delete message;
                break;
            }
        }
        else if (
            message->err() != RdKafka::ERR__TIMED_OUT &&
            message->err() != RdKafka::ERR__PARTITION_EOF
        )
        {
            cerr << "[ORDER] Kafka error: "
                 << message->errstr() << endl;
        }

        delete message;
    }

    cout << "[ORDER] Saga finished." << endl;

    delete consumer;
    delete consumerConfig;
    delete producer;
    delete producerConfig;

    return 0;
}
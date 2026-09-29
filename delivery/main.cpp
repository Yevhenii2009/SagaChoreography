#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

int main()
{
    cout << "[DELIVERY] Service started!" << endl;

    string brokers = "kafka:9092";

    string inputTopic = "product-checked";
    string outputTopic = "delivery-checked";
    string failedTopic = "delivery-failed";

    string errorString;

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
        "delivery-failure-demo",
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
        cerr << "[DELIVERY] Failed to create consumer: "
             << errorString << endl;

        return 1;
    }

    // Subscribe to product-checked
    RdKafka::ErrorCode result =
        consumer->subscribe({ inputTopic });

    if (result != RdKafka::ERR_NO_ERROR)
    {
        cerr << "[DELIVERY] Failed to subscribe: "
             << RdKafka::err2str(result) << endl;

        return 1;
    }

    cout << "[DELIVERY] Connected to Kafka!" << endl;
    cout << "[DELIVERY] Waiting for PRODUCT_CHECKED..." << endl;

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
        cerr << "[DELIVERY] Failed to create producer: "
             << errorString << endl;

        return 1;
    }

    // =========================
    // Main loop
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

            cout << "[DELIVERY] Received event: "
                 << event << endl;

            // Check driver availability
            cout << "[DELIVERY] Checking driver availability..."
                 << endl;

            // For this demo we intentionally create a failure
            bool driverAvailable = false;

            if (driverAvailable)
            {
                cout << "[DELIVERY] Driver available!" << endl;

                string nextEvent = "DELIVERY_CHECKED";

                result = producer->produce(
                    outputTopic,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(nextEvent.c_str()),
                    nextEvent.size(),
                    nullptr,
                    0,
                    0,
                    nullptr
                );

                if (result == RdKafka::ERR_NO_ERROR)
                {
                    cout << "[DELIVERY] Sent event: "
                         << nextEvent << endl;
                }
                else
                {
                    cerr << "[DELIVERY] Failed to send event: "
                         << RdKafka::err2str(result) << endl;
                }

                producer->flush(5000);
            }
            else
            {
                cout << "[DELIVERY] Driver NOT AVAILABLE!"
                     << endl;

                string failedEvent = "DELIVERY_FAILED";

                result = producer->produce(
                    failedTopic,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(failedEvent.c_str()),
                    failedEvent.size(),
                    nullptr,
                    0,
                    0,
                    nullptr
                );

                if (result == RdKafka::ERR_NO_ERROR)
                {
                    cout << "[DELIVERY] Sent event: "
                         << failedEvent << endl;
                }
                else
                {
                    cerr << "[DELIVERY] Failed to send failure event: "
                         << RdKafka::err2str(result) << endl;
                }

                producer->flush(5000);
            }
        }
        else if (
            message->err() != RdKafka::ERR__TIMED_OUT &&
            message->err() != RdKafka::ERR__PARTITION_EOF
        )
        {
            cerr << "[DELIVERY] Kafka error: "
                 << message->errstr() << endl;
        }

        delete message;
    }

    delete producer;
    delete producerConfig;
    delete consumer;
    delete consumerConfig;

    return 0;
}
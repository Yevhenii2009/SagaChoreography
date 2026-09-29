#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

int main()
{
    cout << "[PRODUCT] Service started!" << endl;

    string brokers = "kafka:9092";

    string inputTopic = "account-checked";
    string failedTopic = "delivery-failed";

    string outputTopic = "product-checked";
    string compensationTopic = "product-compensated";

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
        "product-failure-demo",
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
        cerr << "[PRODUCT] Failed to create consumer: "
             << errorString << endl;

        return 1;
    }

    // Subscribe to normal and failure events
    RdKafka::ErrorCode result =
        consumer->subscribe({
            inputTopic,
            failedTopic
        });

    if (result != RdKafka::ERR_NO_ERROR)
    {
        cerr << "[PRODUCT] Failed to subscribe: "
             << RdKafka::err2str(result) << endl;

        return 1;
    }

    cout << "[PRODUCT] Connected to Kafka!" << endl;
    cout << "[PRODUCT] Waiting for ACCOUNT_CHECKED or DELIVERY_FAILED..."
         << endl;

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
        cerr << "[PRODUCT] Failed to create producer: "
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

            string topic = message->topic_name();

            cout << "[PRODUCT] Received event: "
                 << event << endl;

            // =========================
            // Normal order processing
            // =========================

            if (topic == inputTopic &&
                event == "ACCOUNT_CHECKED")
            {
                cout << "[PRODUCT] Checking product availability..."
                     << endl;

                bool productAvailable = true;

                if (productAvailable)
                {
                    cout << "[PRODUCT] Product available!"
                         << endl;

                    string nextEvent = "PRODUCT_CHECKED";

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
                        cout << "[PRODUCT] Sent event: "
                             << nextEvent << endl;
                    }
                    else
                    {
                        cerr << "[PRODUCT] Failed to send event: "
                             << RdKafka::err2str(result) << endl;
                    }

                    producer->flush(5000);
                }
            }

            // =========================
            // Compensation
            // =========================

            else if (topic == failedTopic &&
                     event == "DELIVERY_FAILED")
            {
                cout << "[PRODUCT] Delivery failed!"
                     << endl;

                cout << "[PRODUCT] Starting compensation..."
                     << endl;

                // Return the product reservation
                cout << "[PRODUCT] Product reservation cancelled!"
                     << endl;

                string compensationEvent =
                    "PRODUCT_COMPENSATED";

                result = producer->produce(
                    compensationTopic,
                    RdKafka::Topic::PARTITION_UA,
                    RdKafka::Producer::RK_MSG_COPY,
                    const_cast<char*>(compensationEvent.c_str()),
                    compensationEvent.size(),
                    nullptr,
                    0,
                    0,
                    nullptr
                );

                if (result == RdKafka::ERR_NO_ERROR)
                {
                    cout << "[PRODUCT] Sent event: "
                         << compensationEvent << endl;
                }
                else
                {
                    cerr << "[PRODUCT] Failed to send compensation: "
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
            cerr << "[PRODUCT] Kafka error: "
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
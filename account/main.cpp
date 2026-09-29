#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

int main()
{
    cout << "[ACCOUNT] Service started!" << endl;

    string brokers = "kafka:9092";

    string inputTopic = "order-created";
    string compensationInputTopic = "product-compensated";

    string outputTopic = "account-checked";
    string compensationOutputTopic = "account-compensated";

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
        "account-failure-demo",
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
        cerr << "[ACCOUNT] Failed to create consumer: "
             << errorString << endl;

        return 1;
    }

    // Subscribe to normal and compensation events
    RdKafka::ErrorCode result =
        consumer->subscribe({
            inputTopic,
            compensationInputTopic
        });

    if (result != RdKafka::ERR_NO_ERROR)
    {
        cerr << "[ACCOUNT] Failed to subscribe: "
             << RdKafka::err2str(result) << endl;

        return 1;
    }

    cout << "[ACCOUNT] Connected to Kafka!" << endl;
    cout << "[ACCOUNT] Waiting for ORDER_CREATED or PRODUCT_COMPENSATED..."
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
        cerr << "[ACCOUNT] Failed to create producer: "
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

            cout << "[ACCOUNT] Received event: "
                 << event << endl;

            // =========================
            // Normal order processing
            // =========================

            if (topic == inputTopic &&
                event == "ORDER_CREATED")
            {
                cout << "[ACCOUNT] Checking balance..."
                     << endl;

                bool balanceOk = true;

                if (balanceOk)
                {
                    cout << "[ACCOUNT] Balance OK!"
                         << endl;

                    string nextEvent = "ACCOUNT_CHECKED";

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
                        cout << "[ACCOUNT] Sent event: "
                             << nextEvent << endl;
                    }
                    else
                    {
                        cerr << "[ACCOUNT] Failed to send event: "
                             << RdKafka::err2str(result) << endl;
                    }

                    producer->flush(5000);
                }
            }

            // =========================
            // Compensation
            // =========================

            else if (
                topic == compensationInputTopic &&
                event == "PRODUCT_COMPENSATED"
            )
            {
                cout << "[ACCOUNT] Product compensation received!"
                     << endl;

                cout << "[ACCOUNT] Starting compensation..."
                     << endl;

                // Return the reserved balance
                cout << "[ACCOUNT] Account reservation cancelled!"
                     << endl;

                string compensationEvent =
                    "ACCOUNT_COMPENSATED";

                result = producer->produce(
                    compensationOutputTopic,
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
                    cout << "[ACCOUNT] Sent event: "
                         << compensationEvent << endl;
                }
                else
                {
                    cerr << "[ACCOUNT] Failed to send compensation: "
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
            cerr << "[ACCOUNT] Kafka error: "
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
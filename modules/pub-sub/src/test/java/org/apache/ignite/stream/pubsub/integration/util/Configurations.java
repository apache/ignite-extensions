package org.apache.ignite.stream.pubsub.integration.util;

final class Configurations {

  static final String SERVER_CONFIG_JSON =
      "{\n"
          + "  \"port\": 8080,\n"
          + "  \"kafka\": {\n"
          + "    \"bootstrapServers\": [\"localhost:9094\", \"localhost:9095\", \"localhost:9096\"],\n"
          + "    \"producerProperties\": {\n"
          + "      \"linger.ms\": \"5\",\n"
          + "      \"batch.size\": \"1000000\",\n"
          + "      \"buffer.memory\": \"32000000\"\n"
          + "    },\n"
          + "    \"producerExecutors\": 4,\n"
          + "    \"consumerProperties\": {\n"
          + "      \"max.poll.records\": \"1000\"\n"
          + "    },\n"
          + "    \"consumersPerSubscription\": 4\n"
          + "  }\n"
          + "}";

  static final String PUBSUB_CONFIG_JSON =
      "{\n"
          + "   \"projects\":[\n"
          + "      {\n"
          + "         \"name\":\"test-project\",\n"
          + "         \"topics\":[\n"
          +"             {\n"
          + "               \"name\":\"pagevisits\",\n"
          + "               \"kafkaTopic\":\"pagevisits\",\n"
          + "               \"subscriptions\":[\n"
          + "                  {\n"
          + "                     \"name\":\"ignite_subscription\",\n"
          + "                     \"ackDeadlineSeconds\":10\n"
          + "                  }\n"
          + "               ]\n"
          + "            }\n"
          + "         ]\n"
          + "      }\n"
          + "   ]\n"
          + "}";
}

package org.apache.ignite.stream.pubsub;

import java.io.IOException;
import java.util.List;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class TestPubSub {

    /** Pub/Sub host. */
    private static final String LOCAL_PUBSUB_HOST = "localhost";

    /** Pub/Sub port. */
    private static final int LOCAL_PUBSUB_PORT = 8085;

    private ManagedChannel channel;
    private TransportChannelProvider channelProvider;
    private TopicAdminClient topicAdmin;
    private SubscriptionAdminClient subscriptionAdminClient;

    public TestPubSub() throws Exception {
        setupPubSubService();
    }

    /**
     * Sets up PubSub test server.
     *
     * @throws Exception If failed.
     */
    private void setupPubSubService() throws Exception {
        channel = ManagedChannelBuilder.forTarget(getPubSubAddress()).usePlaintext().build();
        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        topicAdmin = createTopicAdmin(credentialsProvider);
        subscriptionAdminClient = SubscriptionAdminClient.create(GrpcSubscriberStub.create(createSubscriberStub()));
    }

    public void createSubscription(ProjectTopicName topicName, String subscription) throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(topicName.getProject(), subscription);
        subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
    }

    public void deleteSubscription(ProjectTopicName topicName, String subscription) throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(topicName.getProject(), subscription);
        subscriptionAdminClient.deleteSubscription(subscriptionName);
    }

    public SubscriberStubSettings createSubscriberStub() throws IOException {
        CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
        ManagedChannel managedChannel = ManagedChannelBuilder.forTarget(getPubSubAddress()).usePlaintext().build();
        FixedTransportChannelProvider transportChannel = FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel));

        SubscriberStubSettings subscriberStubSettings = SubscriberStubSettings.newBuilder()
                                                                              .setTransportChannelProvider(transportChannel)
                                                                              .setCredentialsProvider(credentialsProvider)
                                                                              .build();
        return subscriberStubSettings;
    }


    /**
     *
     * @param topic
     */
    public void createTopic(ProjectTopicName topic) {
        topicAdmin.createTopic(topic);
    }

    /**
     *
     */
    public void deleteTopic(ProjectTopicName topic) {
        topicAdmin.deleteTopic(topic);
    }

    /**
     * Sends a message to Pub/Sub.
     *
     * @param pubsubMessages List of Pub/Sub Messages.
     * @return Producer used to send the message.
     */
    public void sendMessages(ProjectTopicName topic,List<PubsubMessage> pubsubMessages) throws Exception {
        Publisher publisher = createPublisher(topic, NoCredentialsProvider.create());

        for(PubsubMessage pubsubMessage: pubsubMessages) {
            String messageId = publisher.publish(pubsubMessage).get();
            System.out.println("Sent message "+messageId);
        }

        /**
         * Equivalent to flush
         */
        publisher.publishAllOutstanding();
        publisher.shutdown();
    }

    /**
     * Shuts down test Pub/Sub client.
     */
    public void shutdown() {
        channel.shutdownNow();
    }

    /**
     * Creates the topic admin
     * @param credentialsProvider
     * @return
     * @throws IOException
     */
    private TopicAdminClient createTopicAdmin(CredentialsProvider credentialsProvider) throws IOException {
        return TopicAdminClient.create(
                TopicAdminSettings.newBuilder()
                                  .setTransportChannelProvider(channelProvider)
                                  .setCredentialsProvider(credentialsProvider)
                                  .build()
        );
    }

    private Publisher createPublisher(ProjectTopicName topic, CredentialsProvider credentialsProvider) throws IOException {
        return Publisher.newBuilder(topic)
                        .setChannelProvider(channelProvider)
                        .setCredentialsProvider(credentialsProvider)
                        .build();
    }


    /**
     * Obtains Pub/Sub address.
     *
     * @return Pub/Sub address.
     */
    private String getPubSubAddress() {
        return LOCAL_PUBSUB_HOST+ ":" + LOCAL_PUBSUB_PORT;
    }





}

package com.store.shop;

import org.apache.kafka.clients.admin.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaAdmin {

    public static void list(AdminClient adminClient) throws InterruptedException, ExecutionException {
        ListTopicsResult topics = adminClient.listTopics();
        topics
          .names()
          .get()
          .forEach(System.out::println);
    }

    public static void create(String topicName, int partitions, short replications, AdminClient adminClient) {
        final NewTopic newTopic = new NewTopic(topicName, partitions, replications);
        List<NewTopic> topics = new ArrayList<NewTopic>();
        topics.add(newTopic);
        try {
            final CreateTopicsResult result = adminClient.createTopics(topics);
            result
              .all()
              .get();
        } catch (final Exception ex) {
            throw new RuntimeException("Failed to create topic: " + topicName, ex);
        }
    }

    public static void describe(String topicName, AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(topicName);
        DescribeTopicsResult topics = adminClient.describeTopics(topicNames);
        topics
           .all()
           .get()
           .forEach((x, y) -> System.out.println(x + " " + y.topicId() + " " + y.partitions()));
    }

    public static void delete(String topicName, AdminClient adminClient) {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(topicName);
        try {
            DeleteTopicsResult topics = adminClient.deleteTopics(topicNames);
            topics
               .all()
               .get();
        } catch (final Exception ex) {
            throw new RuntimeException("Failed to delete topic: " + topicName, ex);
        }
    }

    public static void listCG(AdminClient adminClient) throws InterruptedException, ExecutionException {
        ListConsumerGroupsResult cgr = adminClient.listConsumerGroups();
        cgr
          .all()
          .get()
          .forEach(cg -> System.out.println(cg.groupId()));
    }

    public static void deleteCG(String groupId, AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        List<String> groups = new ArrayList<>();
        groups.add(groupId);
        try {
            DeleteConsumerGroupsResult cgr = adminClient.deleteConsumerGroups(groups);
            cgr
               .all()
               .get();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to delete consumer group: " + groupId, ex);
        }
    }

    public static void describeCluster(AdminClient adminClient) throws InterruptedException, ExecutionException {
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        System.out.println(clusterResult.clusterId().get());
    }

}

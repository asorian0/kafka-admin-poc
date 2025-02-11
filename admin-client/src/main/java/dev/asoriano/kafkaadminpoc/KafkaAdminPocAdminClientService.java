package dev.asoriano.kafkaadminpoc;

import org.apache.coyote.BadRequestException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

@Service
public class KafkaAdminPocAdminClientService {

  private final AdminClient kafkaAdminClient;

  public KafkaAdminPocAdminClientService(AdminClient kafkaAdminClient) {
    this.kafkaAdminClient = kafkaAdminClient;
  }

  public void removeMember(final String consumerGroupId, final int partition) {
    try {
      final var description = kafkaAdminClient.describeConsumerGroups(Collections.singleton(consumerGroupId))
          .describedGroups().get(consumerGroupId).get();
      final var members = description.members().stream()
          .filter(memberDescription -> Arrays.stream(memberDescription.assignment().topicPartitions().toArray())
              .anyMatch(topicPartition -> ((TopicPartition) topicPartition).partition() == partition)).toList();

      if (members.isEmpty()) {
        throw new BadRequestException("No members found for the given partition");
      }

      final RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(
          members.stream().map(member ->
               new MemberToRemove(member.clientId(), member.consumerId())
                 // new MemberToRemove(member.clientId())
              )
              .collect(Collectors.toList()));
      options.reason("ADMIN_FORCE_DISCONNECT");

      var result = kafkaAdminClient.removeMembersFromConsumerGroup(consumerGroupId, options).all().get();
      String a = "";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

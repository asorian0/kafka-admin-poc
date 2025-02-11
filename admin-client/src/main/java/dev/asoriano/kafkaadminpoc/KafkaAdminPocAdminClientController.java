package dev.asoriano.kafkaadminpoc;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class KafkaAdminPocAdminClientController {

  private final KafkaAdminPocAdminClientService service;

  public KafkaAdminPocAdminClientController(final KafkaAdminPocAdminClientService service) {
    this.service = service;
  }

  @DeleteMapping("/{consumerGroupId}/{partition}")
  public void removeMember(
      @PathVariable final String consumerGroupId,
      @PathVariable final int partition
  ) {
    service.removeMember(consumerGroupId, partition);
  }
}

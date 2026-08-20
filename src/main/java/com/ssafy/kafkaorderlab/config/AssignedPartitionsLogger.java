package com.ssafy.kafkaorderlab.config;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

/**
 * rebalance 때 consumer group별 partition 할당 변화를 로그로 남긴다.
 */
@Component
@Slf4j
class AssignedPartitionsLogger implements ConsumerAwareRebalanceListener {

	@Override
	public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		log.info("Assigned partitions: group={}, memberId={}, partitions={}",
			consumer.groupMetadata().groupId(), consumer.groupMetadata().memberId(), partitions);
	}

	@Override
	public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		log.info("Revoked partitions: group={}, memberId={}, partitions={}",
			consumer.groupMetadata().groupId(), consumer.groupMetadata().memberId(), partitions);
	}
}

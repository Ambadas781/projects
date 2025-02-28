package com.target.batchtransactionprocessor.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.target.batchtransactionprocessor.constants.AppConstants;
import com.target.batchtransactionprocessor.utils.TokenManagerUtil;
import com.tgt.psm.domain.PSMTransaction;
import com.tgt.settlement.enums.ReprocessingAction;
import com.tgt.settlement.ere.EREKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.tgt.settlement.model.ErrorReprocessingEntity;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ErrorReprocessingEngineHandler {

    @Value("${ere.retry.topic}")
    String retryTopic;

    @Value("${kafka.consumer.batch.topic}")
    String batchTopic;

    @Value("${kafka.consumer.exception.topic}")
    String exceptionTopic;

    @Autowired
    AlertingService alertingService;

    @Autowired
    TokenManagerUtil tokenManagerUtil;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EREKafkaProducer kafkaProducer;

    public void postToERE(String consumerType, PSMTransaction transaction, ConsumerRecord<String, String> transactionMessage, Exception exception) {
        String postBody = null;
        try {
            ErrorReprocessingEntity errorReprocessingEntity = ErrorReprocessingEntity.builder()
                    .reprocessingId(transaction.getTenderUuid()+ "_BTP")
                    .psmModule(AppConstants.APPLICATION)
                    .errorMessage(exception.getMessage())
                    .maxRetry(5)
                    .partitionKey(transaction.getTenderUuid())
                    .tenderUUID(transaction == null ? null : transaction.getTenderUuid())
                    .reprocessingTimeInMins(5)
                    .kafkaPublishTopic(consumerType.equalsIgnoreCase(AppConstants.BATCH_CONSUMER) ? batchTopic : exceptionTopic)
                    .reprocessingAction(ReprocessingAction.KAFKA_PUBLISH)
                    .reprocessingPayload(objectMapper.readTree(transactionMessage.value()).toPrettyString()).build();
            log.info("Calling kafka producer for ere id {}", errorReprocessingEntity.getReprocessingId());
            callEREKafkaProducer(errorReprocessingEntity);
        } catch (Exception e) {
            String epeFail = "Exception occurred while preparing request & invoking EPE endpoint. EPE Exception is - " + e.getMessage();
            log.error("ErrorReprocessingEngineHandler : {} for request - {}", epeFail, postBody);
            alertingService.postAlert(epeFail, "Initial Exception was - " + alertingService.buildExceptionTransactionString(consumerType, transaction, transactionMessage) + " " + alertingService.buildExceptionDetailsString(exception), AppConstants.PROCESSING_INCIDENT_NAME);
        }
    }


    public void callEREKafkaProducer(ErrorReprocessingEntity errorReprocessingEntity) {
        try {
            log.info("in callEREKafkaProducer ere id {}", errorReprocessingEntity.getReprocessingId());
            // Publish the message to Kafka
            kafkaProducer.sendMessage(retryTopic, errorReprocessingEntity, tokenManagerUtil.getAuthToken());
            log.info("pushed to kafka for ere id {}", errorReprocessingEntity.getReprocessingId());
        } catch (Exception e) {
            log.error("Error publishing to ERE topic: {}", e.getMessage());
            String shortDescription = "ERE Publish Failed";
            String summary = String.format("Failed to publish to ERE for reprocessing ID %s. Exception: %s",
                    errorReprocessingEntity.getReprocessingId(), e.getMessage());

            String detailedSummary = String.join(
                    AppConstants.HYPHEN,
                    "Exception on processing EREPublish",
                    alertingService.buildExceptionDetailsString(e)
            );

            alertingService.postAlert(shortDescription, detailedSummary, "EREPublishFailed");

        }
    }


}




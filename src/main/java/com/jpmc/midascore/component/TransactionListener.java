package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) return;
        if (sender.getBalance() < transaction.getAmount()) return;

        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive", transaction, Incentive.class);
        float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);
        transactionRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount));

        logger.info("Processed transaction: {} -> {}, amount: {}, incentive: {}", sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);

        userRepository.findAll().forEach(user ->
                logger.info("User balance: {} = {}", user.getName(), user.getBalance())
        );
    }
}
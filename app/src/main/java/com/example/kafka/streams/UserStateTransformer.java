package com.example.kafka.streams;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import com.example.kafka.avro.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful transformer for user processing
 */
public class UserStateTransformer implements Processor<String, User, String, String> {
    
    private static final Logger logger = LoggerFactory.getLogger(UserStateTransformer.class);
    private static final String USER_STORE = "user-store";
    
    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> stateStore;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.stateStore = context.getStateStore(USER_STORE);
    }
    
    @Override
    public void process(Record<String, User> record) {
        try {
            String key = record.key();
            User user = record.value();
            
            // Get previous user state
            String previousUserJson = stateStore.get(key);
            User previousUser = null;
            
            if (previousUserJson != null) {
                previousUser = objectMapper.readValue(previousUserJson, User.class);
            }
            
            // Update state store
            String currentUserJson = objectMapper.writeValueAsString(user);
            stateStore.put(key, currentUserJson);
            
            // Create output record with state change information
            String outputValue = createStateChangeRecord(previousUser, user);
            
            context.forward(record.withValue(outputValue));
            
        } catch (Exception e) {
            logger.error("Error processing user state", e);
        }
    }
    
    private String createStateChangeRecord(User previousUser, User currentUser) {
        try {
            if (previousUser == null) {
                return String.format("{\"type\":\"USER_CREATED\",\"userId\":%d,\"email\":\"%s\"}", 
                    currentUser.getId(), currentUser.getEmail());
            } else {
                return String.format("{\"type\":\"USER_UPDATED\",\"userId\":%d,\"email\":\"%s\"}", 
                    currentUser.getId(), currentUser.getEmail());
            }
        } catch (Exception e) {
            logger.error("Error creating state change record", e);
            return "{}";
        }
    }
}

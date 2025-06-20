package com.example.kafka.avro;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * UserEvent class representing a user event
 * In a real implementation, this would be generated from Avro schema
 */
public class UserEvent {
    @JsonProperty("eventId")
    private String eventId;
    
    @JsonProperty("userId")
    private Long userId;
    
    @JsonProperty("eventType")
    private EventType eventType;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("metadata")
    private Map<String, String> metadata;
    
    @JsonProperty("sessionId")
    private String sessionId;
    
    @JsonProperty("region")
    private String region;
    
    @JsonProperty("country")
    private String country;
    
    // Default constructor
    public UserEvent() {}
    
    // Builder pattern for easy object creation
    public static Builder newBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private UserEvent event = new UserEvent();
        
        public Builder setEventId(String eventId) {
            event.eventId = eventId;
            return this;
        }
        
        public Builder setUserId(Long userId) {
            event.userId = userId;
            return this;
        }
        
        public Builder setEventType(EventType eventType) {
            event.eventType = eventType;
            return this;
        }
        
        public Builder setTimestamp(Long timestamp) {
            event.timestamp = timestamp;
            return this;
        }
        
        public Builder setMetadata(Map<String, String> metadata) {
            event.metadata = metadata;
            return this;
        }
        
        public Builder setSessionId(String sessionId) {
            event.sessionId = sessionId;
            return this;
        }
        
        public Builder setRegion(String region) {
            event.region = region;
            return this;
        }
        
        public Builder setCountry(String country) {
            event.country = country;
            return this;
        }
        
        public UserEvent build() {
            return event;
        }
    }
    
    // Getters and setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    
    public EventType getEventType() { return eventType; }
    public void setEventType(EventType eventType) { this.eventType = eventType; }
    
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    
    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    @Override
    public String toString() {
        return "UserEvent{" +
                "eventId='" + eventId + '\'' +
                ", userId=" + userId +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                ", metadata=" + metadata +
                ", sessionId='" + sessionId + '\'' +
                '}';
    }
}

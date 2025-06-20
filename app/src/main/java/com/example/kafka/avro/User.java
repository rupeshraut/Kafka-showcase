package com.example.kafka.avro;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * User class representing a user entity
 * In a real implementation, this would be generated from Avro schema
 */
public class User {
    @JsonProperty("id")
    private Long id;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("firstName")
    private String firstName;
    
    @JsonProperty("lastName")
    private String lastName;
    
    @JsonProperty("createdAt")
    private Long createdAt;
    
    @JsonProperty("isActive")
    private Boolean isActive;
    
    @JsonProperty("preferences")
    private Map<String, String> preferences;
    
    // Default constructor
    public User() {}
    
    // Builder pattern for easy object creation
    public static Builder newBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private User user = new User();
        
        public Builder setId(Long id) {
            user.id = id;
            return this;
        }
        
        public Builder setEmail(String email) {
            user.email = email;
            return this;
        }
        
        public Builder setFirstName(String firstName) {
            user.firstName = firstName;
            return this;
        }
        
        public Builder setLastName(String lastName) {
            user.lastName = lastName;
            return this;
        }
        
        public Builder setName(String name) {
            // Split name into first and last name
            String[] parts = name.split(" ", 2);
            user.firstName = parts[0];
            user.lastName = parts.length > 1 ? parts[1] : "";
            return this;
        }
        
        public Builder setCreatedAt(Long createdAt) {
            user.createdAt = createdAt;
            return this;
        }
        
        public Builder setIsActive(Boolean isActive) {
            user.isActive = isActive;
            return this;
        }
        
        public Builder setPreferences(Map<String, String> preferences) {
            user.preferences = preferences;
            return this;
        }
        
        public User build() {
            return user;
        }
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    
    public Long getCreatedAt() { return createdAt; }
    public void setCreatedAt(Long createdAt) { this.createdAt = createdAt; }
    
    public Boolean getIsActive() { return isActive; }
    public void setIsActive(Boolean isActive) { this.isActive = isActive; }
    
    public Map<String, String> getPreferences() { return preferences; }
    public void setPreferences(Map<String, String> preferences) { this.preferences = preferences; }
    
    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", email='" + email + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", createdAt=" + createdAt +
                ", isActive=" + isActive +
                ", preferences=" + preferences +
                '}';
    }
}

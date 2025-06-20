package com.example.kafka.avro;

/**
 * Enum representing different types of user events
 */
public enum EventType {
    LOGIN,
    LOGOUT,
    PURCHASE,
    PROFILE_UPDATE,
    PASSWORD_CHANGE,
    PAGE_VIEW,
    SEARCH,
    CART_ADD,
    CART_REMOVE
}

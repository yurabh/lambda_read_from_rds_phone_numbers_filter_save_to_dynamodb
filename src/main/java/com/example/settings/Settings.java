package com.example.settings;

public class Settings {
    private Settings() {
    }

    private static final String SOURCE_ACCESS_KEY = "SOURCE_ACCESS_KEY";
    private static final String SOURCE_SECRET_KEY = "SOURCE_SECRET_KEY";
    private static final String TOPIC_ARN = "TOPIC_ARN";
    private static final String USER_NAME = "USER_NAME";
    private static final String PASSWORD = "PASSWORD";
    private static final String RDS_URL = "RDS_URL";

    public static String getRdsUrl() {
        return getEnvVar(RDS_URL);
    }

    public static String getPassword() {
        return getEnvVar(PASSWORD);
    }

    public static String getUserName() {
        return getEnvVar(USER_NAME);
    }

    public static String getTopicArn() {
        return getEnvVar(TOPIC_ARN);
    }

    public static String getAccessKey() {
        return getEnvVar(SOURCE_ACCESS_KEY);
    }

    public static String getSecretKey() {
        return getEnvVar(SOURCE_SECRET_KEY);
    }

    private static String getEnvVar(String name) {
        return System.getenv(name);
    }
}

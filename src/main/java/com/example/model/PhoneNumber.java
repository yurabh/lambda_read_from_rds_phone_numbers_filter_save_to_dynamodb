package com.example.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PhoneNumber {
    private final List<Integer> phoneNumbers;

    public PhoneNumber() {
        phoneNumbers = new ArrayList<>();
    }

    public List<Integer> getPhoneNumbers() {
        return phoneNumbers;
    }

    public void addNumber(Integer phoneNumber) {
        phoneNumbers.add(phoneNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhoneNumber that = (PhoneNumber) o;
        return Objects.equals(phoneNumbers, that.phoneNumbers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phoneNumbers);
    }

    @Override
    public String toString() {
        return "PhoneNumber{" +
                "phoneNumbers=" + phoneNumbers +
                '}';
    }
}

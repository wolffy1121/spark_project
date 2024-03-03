package com.wolffy.spark.sparkcore.bean;

import java.io.Serializable;

public class User0 implements Serializable {
    private String name;
    private Integer age;

    public User0(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User0{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}

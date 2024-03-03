package com.wolffy.spark.structured.streming.project.mock.utils;

public class CityInfo {
    private long cityId;
    private String cityName;
    private String area;

    public CityInfo(long city_id, String city_name, String area) {
        this.cityId = city_id;
        this.cityName = city_name;
        this.area = area;
    }

    public long getCityId() {
        return cityId;
    }

    public void setCityId(long cityId) {
        this.cityId = cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }
}


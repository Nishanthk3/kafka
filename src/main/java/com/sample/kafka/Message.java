package com.sample.kafka;

import java.util.Date;

public class Message {
    private int id;
    private String message;
    private Date date;
    
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public Date getDate() {
        return date;
    }
    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "Message [id=" + id + ", message=" + message + ", date=" + date + "]";
    }
    
}

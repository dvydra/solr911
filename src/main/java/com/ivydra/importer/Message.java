package com.ivydra.importer;

import java.util.Date;

public class Message {
    public Long id;
    public Date timesent;
    public String network;
    public String messageId;
    public String code;
    public String type;
    public String body;

    public Message(Date timesent, String network, String messageId, String code, String type, String body) {
        this.timesent = timesent;
        this.network = network;
        this.messageId = messageId;
        this.code = code;
        this.type = type;
        this.body = body;
    }




    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", timesent=" + timesent +
                '}';
    }
}

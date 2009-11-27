package com.ivydra.importer;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.*;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImportMessagesIntoSolr {

    public static void main(String[] args) throws IOException, ParseException, SolrServerException {
        CommonsHttpSolrServer server = new CommonsHttpSolrServer("http://localhost:8983/solr");

        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

        BufferedReader in = new BufferedReader(new FileReader(args[0]));
        String inputLine;
        List<String> failed = new ArrayList<String>();
        int i = 0;
        int failCount = 0;
        while ((inputLine = in.readLine()) != null) {

            Message message = extractMessage(inputLine);
            if (message != null) {
                SolrInputDocument doc = createSolrDoc(message);
                server.add(doc);
            } else {
                failed.add(("FAILED TO PARSE LINE>"+ inputLine +""));
                failCount++;
            }
            i++;

            if (i == 0 || i % 1000 == 0) {
                System.out.println(String.format("processed %d messages - %d failed", i, failCount));
                server.commit();
            }
        }
        in.close();
        for (String l : failed) {
            System.out.println(l);
        }
    }

    private static SolrInputDocument createSolrDoc(Message message) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("timesent", message.timesent);
        doc.addField("network", message.network);
        doc.addField("messageid", message.messageId);
        doc.addField("code", message.code);
        doc.addField("type", message.type);
        doc.addField("body", message.body);

        return doc;
        
        
    }

    public static Message extractMessage(String inputLine) throws ParseException {
        Message message = null;
        String regex =  "(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d)\\s+" +      //date
                        "(\\w+)\\s+" +                                                 //network
                        "[\\[{]([\\d\\?]+)[\\]}]( \\d{1,2}:\\d\\d:\\d\\d [AP]M)?\\s+" +     //messageId
                        "(\\w)\\s+" +                                                  //code
                        "([\\w/]+)\\s+" +                                            //type
                        "(.*)";                                                        //body
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(inputLine);
        if (matcher.find()) {
            message = new Message(formatAsDate(matcher.group(1)),matcher.group(2), matcher.group(3), matcher.group(5), matcher.group(6),matcher.group(7));
        }

        return message;

    }

    private static Date formatAsDate(String s) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
        Date date = dateFormat.parse(s);
        return date;
    }
}

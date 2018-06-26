package edu.stsci.registry.solr;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.EventListener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class ImportEndListener implements EventListener {

    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private static final String USER_AGENT = "Mozilla/5.0";
    private static final String TRIGGER_URL = System.getProperty("dataimport.trigger_url");

    @Override
    public void onEvent(Context aContext) {
        System.out.println("TRIGGER_URL: " + TRIGGER_URL);
        String core = aContext.getSolrCore().getCoreDescriptor().getName();
        String process = aContext.currentProcess();
        logger.info("Finished " + process + " for core: " + core);
        Map<String, Object> requestParams = aContext.getRequestParameters();
        Boolean clean = requestParams.get("clean").toString().equals("true");
        if (clean) {
            try {
                logger.info("Triggering import hook");
                sendGET(core);
            } catch (IOException e) {
                logger.error("Could not trigger import hook");
            }
        }
    }

    private static void sendGET(String core) throws IOException {
        URL obj = new URL(TRIGGER_URL + "?core=" + core);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", USER_AGENT);
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            System.out.println(response.toString());
        } else {
            System.out.println("GET request not worked");
        }

    }
}

package edu.stsci.registry.solr;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.EventListener;

public class ImportEndListener implements EventListener {

    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());

    @Override
    public void onEvent(Context aContext) {
        String core = aContext.getSolrCore().getCoreDescriptor().getName();
        logger.info("Finished import for core: " + core);
    }

}

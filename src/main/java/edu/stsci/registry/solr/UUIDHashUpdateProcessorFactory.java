package edu.stsci.registry.solr;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.SimpleUpdateProcessorFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class UUIDHashUpdateProcessorFactory extends SimpleUpdateProcessorFactory {

    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private static final String FIELD_PARAM = "fieldName";

    protected String idFieldName = null;
    protected String valFieldName = null;

    @Override
    protected void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp) {
        if (StringUtils.isEmpty(idFieldName)) {
            SchemaField schemaField = req.getSchema().getUniqueKeyField();
            idFieldName = schemaField.getName();
        }

        if (StringUtils.isEmpty(valFieldName)) {
            valFieldName = getParam(FIELD_PARAM);
        }

        SolrInputDocument doc = cmd.getSolrInputDocument();
        Object val = doc.getFieldValue(valFieldName);

        if (val != null) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                byte[] messageDigest = md.digest(val.toString().getBytes());
                BigInteger number = new BigInteger(1, messageDigest);
                String uuid = number.toString(16);

                logger.info("UUID: " + uuid);
                doc.addField(idFieldName, uuid);

            } catch (NoSuchAlgorithmException x) {
                // do proper exception handling
            }
        }
    }
}

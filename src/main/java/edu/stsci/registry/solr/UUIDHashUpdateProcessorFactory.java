package edu.stsci.registry.solr;

import org.apache.commons.lang.StringUtils;
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
        String val = doc.getFieldValue(valFieldName).toString();

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(val.getBytes());
            BigInteger number = new BigInteger(1, messageDigest);
            String uuid = number.toString(16);

            doc.addField(idFieldName, uuid);

        } catch (NoSuchAlgorithmException x) {
            // do proper exception handling
        }

    }
}

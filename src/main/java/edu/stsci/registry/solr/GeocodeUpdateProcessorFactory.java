package edu.stsci.registry.solr;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.errors.ApiException;
import com.google.maps.model.GeocodingResult;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.SimpleUpdateProcessorFactory;

import java.sql.*;
import java.util.Collection;

public class GeocodeUpdateProcessorFactory extends SimpleUpdateProcessorFactory {
    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:/opt/solr/server/solr/geolocation_cache";

    private static String apiKey = null;
    private static final Logger logger = LogManager.getLogger(OAIPMHEntityProcessor.class.getName());
    private static final String APIKEY_PARAM = "geolocationApiKey";
    private static final String PLACENAME_PARAM = "placenameField";
    private static final String COORDINATES_PARAM = "coordinatesField";
    private static final String GEOJSON_PARAM = "geojsonField";

    protected String placenameField = null;
    protected String coordinatesField = null;
    protected String geojsonField = null;

    public static void main(String[] args) {
        String address = "Michigan";
        try {
            Coordinates coordinates = selectWithPreparedStatement("Michigan");
            if (coordinates == null) {
                GeoApiContext context = new GeoApiContext.Builder().apiKey(apiKey).build();
                GeocodingResult[] results = GeocodingApi.geocode(context, address).awaitIgnoreError();
                coordinates = insertWithPreparedStatement("Michigan", results[0].geometry.location.lat, results[0].geometry.location.lng, results[0].partialMatch);
            } else {
                System.out.println("Found cached location");
            }
            System.out.println(address + ": " + coordinates.lat + "," + coordinates.lng);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void process(AddUpdateCommand cmd, SolrQueryRequest req, SolrQueryResponse rsp) {
        if (StringUtils.isEmpty(apiKey)) {
            apiKey = getParam(APIKEY_PARAM);
            if (apiKey == null) {
                logger.info("Error loading API Key");
                return;
            }
        }
        if (StringUtils.isEmpty(placenameField)) {
            placenameField = getParam(PLACENAME_PARAM);
            if (placenameField == null) {
                logger.info("placenameField must be defined in solrconfig.xml");
                return;
            }
        }
        if (StringUtils.isEmpty(coordinatesField)) {
            coordinatesField = getParam(COORDINATES_PARAM);
            if (coordinatesField == null) {
                logger.info("coordinatesField must be defined in solrconfig.xml");
                return;
            }
        }
        if (StringUtils.isEmpty(geojsonField)) {
            geojsonField = getParam(GEOJSON_PARAM);
            if (geojsonField == null) {
                logger.info("geojsonField must be defined in solrconfig.xml");
                return;
            }
        }

        SolrInputDocument doc = cmd.getSolrInputDocument();
        Collection<Object> vals = doc.getFieldValues(placenameField);

        if (vals != null) {
            for (Object val : vals) {
                String address = val.toString();
                logger.info("Location: " + address);
                try {
                    Coordinates coordinates = selectWithPreparedStatement(address);
                    if (coordinates == null) {
                        logger.info("Geolocating: " + address);
                        GeoApiContext context = new GeoApiContext.Builder().apiKey(apiKey).build();
                        try {
                            GeocodingResult[] results = GeocodingApi.geocode(context, address).await();
                            if (results == null || results.length == 0) {
                                logger.info("Geolocation returned 0 results: " + address);
                                continue;
                            }
                            coordinates = insertWithPreparedStatement(address, results[0].geometry.location.lat, results[0].geometry.location.lng, results[0].partialMatch);
                        } catch (ApiException e) {
                            logger.info("Geocoding API error: " + e.getLocalizedMessage());
                        } catch (Exception e) {
                            logger.info("Geocoding error: " + e.getLocalizedMessage());
                        }
                    } else {
                        logger.info("Found cached location");
                    }
                    logger.info(address + ": " + coordinates.lat + "," + coordinates.lng);
                    if (coordinates.partial) {
                        logger.info("Partial geolocation result: " + address);
                    } else {
                        doc.addField(coordinatesField, coordinates.toString());
                        doc.addField(geojsonField, coordinates.toJSON());
                    }
                } catch (SQLException e) {
                    logger.info("Geolocation error: " + e.getLocalizedMessage());
                }
            }
        } else {
            logger.info("No location found");
        }
    }

    private static Coordinates selectWithPreparedStatement(String name) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement createPreparedStatement;
        PreparedStatement selectPreparedStatement;

        String CreateQuery = "CREATE TABLE IF NOT EXISTS GEOLOCATION(id int auto_increment primary key, name varchar(255), lat double, lng double, partial boolean)";
        String SelectQuery = "select * from GEOLOCATION WHERE name = ?";

        Coordinates coordinates = null;
        try {
            createPreparedStatement = connection.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            selectPreparedStatement = connection.prepareStatement(SelectQuery);
            selectPreparedStatement.setString(1, name);

            logger.info("Checking for cached location: " + name);
            ResultSet rs = selectPreparedStatement.executeQuery();
            if (rs.next()) {
                coordinates = new Coordinates(name, rs.getDouble("lat"), rs.getDouble("lng"), rs.getBoolean("partial"));
            }
            selectPreparedStatement.close();
        } catch (SQLException e) {
            logger.info("SQL Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            logger.info("Exception Message " + e.getLocalizedMessage());
        } finally {
            if (connection != null) {
                logger.info("Closing database connection");
                connection.close();
            } else {
                logger.info("No database connection to close");
            }
            return coordinates;
        }
    }

    private static Coordinates  insertWithPreparedStatement(String name, Double lat, Double lng, Boolean partial) throws SQLException {
        Connection connection = getDBConnection();
        PreparedStatement insertPreparedStatement;

        String InsertQuery = "INSERT INTO GEOLOCATION" + "(id, name, lat, lng, partial) values" + "(DEFAULT,?,?,?,?)";
        Coordinates coordinates = null;
        try {
            logger.info("Caching location: " + name + " (" + lat + "," + lng + ")");

            insertPreparedStatement = connection.prepareStatement(InsertQuery);
            insertPreparedStatement.setString(1, name);
            insertPreparedStatement.setDouble(2, lat);
            insertPreparedStatement.setDouble(3, lng);
            insertPreparedStatement.setBoolean(4, partial);
            insertPreparedStatement.executeUpdate();
            insertPreparedStatement.close();

            coordinates = new Coordinates(name, lat, lng, partial);
        } catch (SQLException e) {
            logger.info("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
            return coordinates;
        }
    }

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            logger.info("DB driver could not be loaded: " + e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION);
            return dbConnection;
        } catch (SQLException e) {
            logger.info("Could not connect to database: " + e.getMessage());
        }
        return dbConnection;
    }

    private static class Coordinates {
        String name;
        Double lat;
        Double lng;
        Boolean partial;

        public Coordinates(String newName, Double newLat, Double newLng, Boolean newPartial) {
            name = newName;
            lat = newLat;
            lng = newLng;
            partial = newPartial;
        }

        @Override
        public String toString() {
            return lat.toString() + "," + lng.toString();
        }

        public String toJSON() {
            return "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[" + lng.toString() + "," + lat.toString() + "]},\"properties\":{\"placename\":\"" + name + "\"}}";
        }
    }
}

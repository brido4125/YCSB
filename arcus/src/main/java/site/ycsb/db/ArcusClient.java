package site.ycsb.db;

import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import site.ycsb.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Concrete Arcus client implementation.
 */
public class ArcusClient extends DB {

  private final Logger logger = Logger.getLogger(getClass());
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private boolean checkOperationStatus;
  private int objectExpirationTime;
  private long shutdownTimeoutMillis;

  /**
   * The ArcusClient implementation that will be used to communicate
   * with the memcached server.
   */
  private net.spy.memcached.ArcusClient arcusClient;

  public static final String CHECK_OPERATION_STATUS_PROPERTY =
      "memcached.checkOperationStatus";
  public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

  public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY =
      "memcached.shutdownTimeoutMillis";
  public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

  public static final String OBJECT_EXPIRATION_TIME_PROPERTY =
      "memcached.objectExpirationTime";
  public static final String DEFAULT_OBJECT_EXPIRATION_TIME =
      String.valueOf(Integer.MAX_VALUE);
  private static final String CANCELLED_MSG = "cancelled";


  @Override
  public void init() throws DBException {
    arcusClient = createArcusClient();
    checkOperationStatus = Boolean.parseBoolean(
        getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY,
            CHECK_OPERATION_STATUS_DEFAULT));
    objectExpirationTime = Integer.parseInt(
        getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY,
            DEFAULT_OBJECT_EXPIRATION_TIME));
    shutdownTimeoutMillis = Integer.parseInt(
        getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY,
            DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
  }

  protected net.spy.memcached.ArcusClient createArcusClient() {
    return net.spy.memcached.ArcusClient
        .createArcusClient("ncp-4c4-001:2181", "long-running-community");
  }

  @Override
  public void cleanup() {
    arcusClient.shutdown(shutdownTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      GetFuture<Object> future = arcusClient.asyncGet(key);
      Object document = future.get();
      if (document != null) {
        fromJson((String) document, fields, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey,
                     int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future =
          arcusClient.replace(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future =
          arcusClient.add(key, objectExpirationTime, toJson(values));
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    try {
      OperationFuture<Boolean> future = arcusClient.delete(key);
      return getReturnCode(future);
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  protected Status getReturnCode(OperationFuture<Boolean> future) {
    if (!checkOperationStatus) {
      return Status.OK;
    }
    if (future.getStatus().isSuccess()) {
      return Status.OK;
    } else if (CANCELLED_MSG.equals(future.getStatus().getMessage())) {
      return new Status("CANCELLED_MSG", CANCELLED_MSG);
    }
    return new Status("ERROR", future.getStatus().getMessage());
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
      /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }
}

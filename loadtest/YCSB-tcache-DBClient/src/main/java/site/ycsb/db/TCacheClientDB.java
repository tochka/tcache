/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Concrete TCacheClientDB client implementation.
 */
public class TCacheClientDB extends DB {

  private TcacheClient client = null;
  private static final Logger LOGGER = Logger.getLogger(TCacheClientDB.class.getName());
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private boolean checkOperationStatus;
  private long shutdownTimeoutMillis;
  private int objectExpirationTime;

  public static final String HOSTS_PROPERTY = "tcache.hosts";

  public static final int DEFAULT_PORT = 30003;

  private static final String TEMPORARY_FAILURE_MSG = "Temporary failure";
  private static final String CANCELLED_MSG = "cancelled";

  public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY =
      "tcache.shutdownTimeoutMillis";
  public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

  public static final String OBJECT_EXPIRATION_TIME_PROPERTY =
      "tcache.objectExpirationTime";
  public static final String DEFAULT_OBJECT_EXPIRATION_TIME =
      String.valueOf(Integer.MAX_VALUE);

  public static final String CHECK_OPERATION_STATUS_PROPERTY =
      "tcache.checkOperationStatus";
  public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

  public static final String READ_BUFFER_SIZE_PROPERTY =
      "tcache.readBufferSize";
  public static final String DEFAULT_READ_BUFFER_SIZE = "3000000";

  public static final String OP_TIMEOUT_PROPERTY = "tcache.opTimeoutMillis";
  public static final String DEFAULT_OP_TIMEOUT = "60000";


  @Override
  public void init() throws DBException {
    try {
      client = createClient();
      checkOperationStatus = Boolean.parseBoolean(
          getProperties().getProperty(CHECK_OPERATION_STATUS_PROPERTY,
                                      CHECK_OPERATION_STATUS_DEFAULT));
      objectExpirationTime = Integer.parseInt(
          getProperties().getProperty(OBJECT_EXPIRATION_TIME_PROPERTY,
                                      DEFAULT_OBJECT_EXPIRATION_TIME));
      shutdownTimeoutMillis = Integer.parseInt(
          getProperties().getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY,
                                      DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  protected TcacheClient createClient()
      throws Exception {
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    String[] hosts = getProperties().getProperty(HOSTS_PROPERTY).split(",");
    for (String address : hosts) {
      int colon = address.indexOf(":");
      int port = DEFAULT_PORT;
      String host = address;
      if (colon != -1) {
        port = Integer.parseInt(address.substring(colon + 1));
        host = address.substring(0, colon);
      }
      addresses.add(new InetSocketAddress(host, port));
    }
    return new TcacheClient(addresses.get(0).getHostName(), addresses.get(0).getPort());
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      TcacheClient.Item item = client.get(key);
      fromJson(item.getValue(), fields, result);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      client.set(new TcacheClient.Item(key, toJson(values), objectExpirationTime));
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      client.set(new TcacheClient.Item(key, toJson(values), objectExpirationTime));
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error inserting value" + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    try {
      client.delete(key);
      return Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Error deleting value" + key, e);
      return Status.ERROR;
    }
  }
//
//  protected Status getReturnCode(OperationFuture<Boolean> future) {
//    if (!checkOperationStatus) {
//      return Status.OK;
//    }
//    if (future.getStatus().isSuccess()) {
//      return Status.OK;
//    } else if (TEMPORARY_FAILURE_MSG.equals(future.getStatus().getMessage())) {
//      return new Status("TEMPORARY_FAILURE", TEMPORARY_FAILURE_MSG);
//    } else if (CANCELLED_MSG.equals(future.getStatus().getMessage())) {
//      return new Status("CANCELLED_MSG", CANCELLED_MSG);
//    }
//    return new Status("ERROR", future.getStatus().getMessage());
//  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (client != null) {
        client.close();
      }
    }catch (Exception e){
      throw  new DBException(e.getMessage());
    }

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

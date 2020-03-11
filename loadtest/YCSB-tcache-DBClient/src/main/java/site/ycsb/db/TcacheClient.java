package site.ycsb.db;

import org.apache.commons.codec.binary.Hex;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;


/**
 * TcacheClient implement protocol between Tcache and Java.
 */
public class TcacheClient {
  private static final Logger LOGGER = Logger.getLogger(TcacheClient.class.getName());

  private Socket clientSocket;
  private OutputStream out;
  private InputStream in;

  public TcacheClient(String ip, int port) throws IOException {
    clientSocket = new Socket(ip, port);
    in = clientSocket.getInputStream();
    out = clientSocket.getOutputStream();
  }

  public void set(Item item) throws Exception {
    ItemIntrnal i = new ItemIntrnal(item.getKey(), item.getValue(), item.getExpiration());

//    LOGGER.log(Level.INFO, "in<-"+i.toString());

    Operation op = new Operation(OperationCode.SET, i);
    send(op);
  }

  public Item get(String key) throws Exception {

    Operation op = new Operation(OperationCode.GET, new BytesData(key.getBytes(StandardCharsets.UTF_8)));
    Operation res = send(op);

    ByteBuffer bbuf = ByteBuffer.wrap(res.payload);
    ItemIntrnal ii = new ItemIntrnal(bbuf);

//    LOGGER.log(Level.INFO, "out->"+ii.toString());

    return new Item(key, new String(ii.value, StandardCharsets.UTF_8), ii.expiration);
  }

  public void delete(String key) throws Exception {
    Operation op = new Operation(OperationCode.DEL, new BytesData(key.getBytes(StandardCharsets.UTF_8)));
    send(op);
  }

  /**
   * Data representation.
   */
  public static class Item {
    private String key;
    private String value;
    private int expiration;

    public Item(String key, String value, int expiration) {
      this.key = key;
      this.value = value;
      this.expiration = expiration;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public int getExpiration() {
      return expiration;
    }
  }

  public void close() throws IOException {
    in.close();
    out.close();
    clientSocket.close();
  }

  private Operation send(Operation op) throws Exception {
//    LOGGER.log(Level.INFO, "in<-"+op.toString());

    out.write(op.toBytes());

    byte[] header = new byte[5]; // opCode(1)+paloadLen(4)
    int n = in.read(header);
    if (n != 5) {
      throw new IOException("unexpected length");
    }

    ByteBuffer bbuf = ByteBuffer.wrap(header);
    OperationCode opCode = OperationCode.byte2OperationCode(bbuf.get());
    int payloadLen = bbuf.getInt();
    byte[] payload = new byte[0];
    if (payloadLen != 0) {
      payload = new byte[payloadLen];
      in.read(payload);
    }

    Operation res = new Operation(opCode, payload);
//    LOGGER.log(Level.INFO, "out->"+res.toString());

    if (res.opCode != OperationCode.SUCCESS) {
      String msg = new String(res.payload, StandardCharsets.UTF_8);
      throw new Exception(msg);
    }
    return res;
  }

  private enum OperationCode {
    SET((byte) 1),
    GET((byte) 2),
    DEL((byte) 3),

    SUCCESS((byte) 0xFF),
    ERROR((byte) 0xFE),
    ERRORNOTFOUND((byte) 0xFD);

    public static OperationCode byte2OperationCode(byte b) throws Exception {
      switch (b) {
      case 1:
        return SET;
      case 2:
        return GET;
      case (byte) 0xFF:
        return SUCCESS;
      case (byte)0xFE:
        return ERROR;
      case (byte)0xFD:
        return ERRORNOTFOUND;
      default:
        throw new Exception("unexpected value"+(int)b);
      }
    }

    private final byte value;

    OperationCode(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return value;
    }
  }


  private class Operation implements ConvertToBytes {
    private OperationCode opCode;
    private byte[] payload;

    public Operation(OperationCode op, ConvertToBytes p) {
      opCode = op;
      payload = p.toBytes();
    }

    public Operation(OperationCode op, byte[] payload) {
      this.opCode = op;
      this.payload = payload;
    }

    public byte[] toBytes() {
      ByteBuffer bbuf = ByteBuffer.allocate(1 + 4 + payload.length); // opCode+payloadLen
      bbuf.put(opCode.getValue());
      bbuf.putInt(payload.length);
      if (payload.length != 0) {
        bbuf.put(payload);
      }
      return bbuf.array();
    }
    public String toString(){
      return "op{code: " + opCode+" data len: "+payload.length+" data: "+ Hex.encodeHexString(payload) +"}";
    }
  }

  private class ItemIntrnal implements ConvertToBytes {
    private int expiration;
    private byte[] key;
    private byte[] value;

    public ItemIntrnal(String key, String value, int exp) {
      expiration = exp;
      this.key = key.getBytes(StandardCharsets.UTF_8);
      this.value = value.getBytes(StandardCharsets.UTF_8);
    }

    public ItemIntrnal(ByteBuffer bbuf) {
      expiration = bbuf.getInt();
      short keyLen = bbuf.getShort();
      int valLen = bbuf.getInt();
      if (keyLen != 0) {
        key = new byte[keyLen];
        bbuf.get(key);
      }
      if (valLen != 0) {
        value = new byte[valLen];
        bbuf.get(value);
      }
    }

    public byte[] toBytes() {
      ByteBuffer bbuf = ByteBuffer.allocate(4 + 2 + 4 + key.length + value.length); // expiration+keyLen+valueLen
      bbuf.putInt(expiration);
      bbuf.putShort((short) key.length);
      bbuf.putInt(value.length);
      if (key.length != 0) {
        bbuf.put(key);
      }
      if (value.length != 0) {
        bbuf.put(value);
      }
      return bbuf.array();
    }
    public String toString(){
      return "item{exp: " + expiration+" key len: "+key.length+" key: "+ Hex.encodeHexString(key)+
          " val len: "+value.length+" val: "+Hex.encodeHexString(value)+"}";
    }
  }

  private class BytesData implements ConvertToBytes {
    private byte[] data;

    public BytesData(byte[] data) {
      this.data = data;
    }

    public byte[] toBytes() {
      return data;
    }
  }

  private interface ConvertToBytes {
    byte[] toBytes();
  }
}

package com.nucleodb.memcache.data;

import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;

import java.io.Serializable;

@Table(dataEntryClass = KVDataEntry.class, tableName = "memcached")
public class KeyVal implements Serializable {
  private static final long serialVersionUID = 1;

  private String key;
  private String data;
  private int flags;
  private int targetTimeSec;

  public KeyVal() {
  }

  public KeyVal(String key, String data) {
    this.key = key;
    this.data = data;
  }

  public KeyVal(String key, String data, int flags, int targetTimeSec) {
    this.key = key;
    this.data = data;
    this.flags = flags;
    this.targetTimeSec = targetTimeSec;
  }

  public KeyVal(String data, int flags, int targetTimeSec) {
    this.data = data;
    this.flags = flags;
    this.targetTimeSec = targetTimeSec;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public int getFlags() {
    return flags;
  }

  public void setFlags(int flags) {
    this.flags = flags;
  }

  public int getTargetTimeSec() {
    return targetTimeSec;
  }

  public void setTargetTimeSec(int targetTimeSec) {
    this.targetTimeSec = targetTimeSec;
  }
}

package com.nucleodb.memcache.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;

public class KVDataEntry extends DataEntry<KeyVal> {
  public KVDataEntry(KeyVal obj) {
    super(obj);
    super.setKey(obj.getKey());
  }

  public KVDataEntry(Create create) throws ClassNotFoundException, JsonProcessingException {
    super(create);
  }

  public KVDataEntry() {
  }

  public KVDataEntry(String key) {
    super(key);
  }
}

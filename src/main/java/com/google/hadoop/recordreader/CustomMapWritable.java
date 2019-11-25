package com.google.hadoop.recordreader;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.json.simple.JSONObject;

import java.util.Set;

class CustomMapWritable extends MapWritable {
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    Set<Writable> keySet = this.keySet();
    JSONObject jsonObject = new JSONObject();

    for (Object key : keySet) {
      jsonObject.put(key.toString(), this.get(key));
    }
    return jsonObject.toString();
  }
}

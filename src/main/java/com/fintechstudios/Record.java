package com.fintechstudios;

import java.io.Serializable;

public class Record implements Serializable {
  private static final long serialVersionUID = 1L;

  String id;

  public Record() {}

  public Record(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}

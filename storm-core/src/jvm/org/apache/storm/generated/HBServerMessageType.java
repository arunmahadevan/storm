/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.10.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum HBServerMessageType implements org.apache.thrift.TEnum {
  CREATE_PATH(0),
  CREATE_PATH_RESPONSE(1),
  EXISTS(2),
  EXISTS_RESPONSE(3),
  SEND_PULSE(4),
  SEND_PULSE_RESPONSE(5),
  GET_ALL_PULSE_FOR_PATH(6),
  GET_ALL_PULSE_FOR_PATH_RESPONSE(7),
  GET_ALL_NODES_FOR_PATH(8),
  GET_ALL_NODES_FOR_PATH_RESPONSE(9),
  GET_PULSE(10),
  GET_PULSE_RESPONSE(11),
  DELETE_PATH(12),
  DELETE_PATH_RESPONSE(13),
  DELETE_PULSE_ID(14),
  DELETE_PULSE_ID_RESPONSE(15),
  CONTROL_MESSAGE(16),
  SASL_MESSAGE_TOKEN(17),
  NOT_AUTHORIZED(18);

  private final int value;

  private HBServerMessageType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static HBServerMessageType findByValue(int value) { 
    switch (value) {
      case 0:
        return CREATE_PATH;
      case 1:
        return CREATE_PATH_RESPONSE;
      case 2:
        return EXISTS;
      case 3:
        return EXISTS_RESPONSE;
      case 4:
        return SEND_PULSE;
      case 5:
        return SEND_PULSE_RESPONSE;
      case 6:
        return GET_ALL_PULSE_FOR_PATH;
      case 7:
        return GET_ALL_PULSE_FOR_PATH_RESPONSE;
      case 8:
        return GET_ALL_NODES_FOR_PATH;
      case 9:
        return GET_ALL_NODES_FOR_PATH_RESPONSE;
      case 10:
        return GET_PULSE;
      case 11:
        return GET_PULSE_RESPONSE;
      case 12:
        return DELETE_PATH;
      case 13:
        return DELETE_PATH_RESPONSE;
      case 14:
        return DELETE_PULSE_ID;
      case 15:
        return DELETE_PULSE_ID_RESPONSE;
      case 16:
        return CONTROL_MESSAGE;
      case 17:
        return SASL_MESSAGE_TOKEN;
      case 18:
        return NOT_AUTHORIZED;
      default:
        return null;
    }
  }
}

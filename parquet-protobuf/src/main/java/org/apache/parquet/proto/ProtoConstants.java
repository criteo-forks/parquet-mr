package org.apache.parquet.proto;

/**
 * Constants.
 */
public final class ProtoConstants {

  public static final String METADATA_ENUM_PREFIX = "parquet.proto.enum.";
  public static final String METADATA_ENUM_KEY_VALUE_SEPARATOR = ":";
  public static final String METADATA_ENUM_ITEM_SEPARATOR = ",";
  /**
   * Configuration flag to enable reader to accept enum label that's neither defined in its own proto schema nor conform
   * to the "UNKNOWN_ENUM_*" pattern with which we can get the enum number. The enum value will be treated as an unknown
   * enum with number -1. <br/>
   * Enabling it will avoid a job failure, but you should perhaps use an up-to-date schema instead.
   */
  public static final String CONFIG_ACCEPT_UNKNOWN_ENUM = "parquet.proto.accept.unknown.enum";

  private ProtoConstants() {
    // Do not instantiate.
  }
}

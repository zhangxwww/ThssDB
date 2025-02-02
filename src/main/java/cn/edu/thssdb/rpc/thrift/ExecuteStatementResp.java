/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package cn.edu.thssdb.rpc.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-06-11")
public class ExecuteStatementResp implements org.apache.thrift.TBase<ExecuteStatementResp, ExecuteStatementResp._Fields>, java.io.Serializable, Cloneable, Comparable<ExecuteStatementResp> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ExecuteStatementResp");

  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField("status", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField IS_ABORT_FIELD_DESC = new org.apache.thrift.protocol.TField("isAbort", org.apache.thrift.protocol.TType.BOOL, (short)2);
  private static final org.apache.thrift.protocol.TField HAS_RESULT_FIELD_DESC = new org.apache.thrift.protocol.TField("hasResult", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField COLUMNS_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("columnsList", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField ROW_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("rowList", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ExecuteStatementRespStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ExecuteStatementRespTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable Status status; // required
  public boolean isAbort; // required
  public boolean hasResult; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> columnsList; // optional
  public @org.apache.thrift.annotation.Nullable java.util.List<java.util.List<java.lang.String>> rowList; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STATUS((short)1, "status"),
    IS_ABORT((short)2, "isAbort"),
    HAS_RESULT((short)3, "hasResult"),
    COLUMNS_LIST((short)4, "columnsList"),
    ROW_LIST((short)5, "rowList");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // STATUS
          return STATUS;
        case 2: // IS_ABORT
          return IS_ABORT;
        case 3: // HAS_RESULT
          return HAS_RESULT;
        case 4: // COLUMNS_LIST
          return COLUMNS_LIST;
        case 5: // ROW_LIST
          return ROW_LIST;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ISABORT_ISSET_ID = 0;
  private static final int __HASRESULT_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.COLUMNS_LIST,_Fields.ROW_LIST};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData("status", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Status.class)));
    tmpMap.put(_Fields.IS_ABORT, new org.apache.thrift.meta_data.FieldMetaData("isAbort", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.HAS_RESULT, new org.apache.thrift.meta_data.FieldMetaData("hasResult", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.COLUMNS_LIST, new org.apache.thrift.meta_data.FieldMetaData("columnsList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.ROW_LIST, new org.apache.thrift.meta_data.FieldMetaData("rowList", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ExecuteStatementResp.class, metaDataMap);
  }

  public ExecuteStatementResp() {
  }

  public ExecuteStatementResp(
    Status status,
    boolean isAbort,
    boolean hasResult)
  {
    this();
    this.status = status;
    this.isAbort = isAbort;
    setIsAbortIsSet(true);
    this.hasResult = hasResult;
    setHasResultIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ExecuteStatementResp(ExecuteStatementResp other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetStatus()) {
      this.status = new Status(other.status);
    }
    this.isAbort = other.isAbort;
    this.hasResult = other.hasResult;
    if (other.isSetColumnsList()) {
      java.util.List<java.lang.String> __this__columnsList = new java.util.ArrayList<java.lang.String>(other.columnsList);
      this.columnsList = __this__columnsList;
    }
    if (other.isSetRowList()) {
      java.util.List<java.util.List<java.lang.String>> __this__rowList = new java.util.ArrayList<java.util.List<java.lang.String>>(other.rowList.size());
      for (java.util.List<java.lang.String> other_element : other.rowList) {
        java.util.List<java.lang.String> __this__rowList_copy = new java.util.ArrayList<java.lang.String>(other_element);
        __this__rowList.add(__this__rowList_copy);
      }
      this.rowList = __this__rowList;
    }
  }

  public ExecuteStatementResp deepCopy() {
    return new ExecuteStatementResp(this);
  }

  @Override
  public void clear() {
    this.status = null;
    setIsAbortIsSet(false);
    this.isAbort = false;
    setHasResultIsSet(false);
    this.hasResult = false;
    this.columnsList = null;
    this.rowList = null;
  }

  @org.apache.thrift.annotation.Nullable
  public Status getStatus() {
    return this.status;
  }

  public ExecuteStatementResp setStatus(@org.apache.thrift.annotation.Nullable Status status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public boolean isIsAbort() {
    return this.isAbort;
  }

  public ExecuteStatementResp setIsAbort(boolean isAbort) {
    this.isAbort = isAbort;
    setIsAbortIsSet(true);
    return this;
  }

  public void unsetIsAbort() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __ISABORT_ISSET_ID);
  }

  /** Returns true if field isAbort is set (has been assigned a value) and false otherwise */
  public boolean isSetIsAbort() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __ISABORT_ISSET_ID);
  }

  public void setIsAbortIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __ISABORT_ISSET_ID, value);
  }

  public boolean isHasResult() {
    return this.hasResult;
  }

  public ExecuteStatementResp setHasResult(boolean hasResult) {
    this.hasResult = hasResult;
    setHasResultIsSet(true);
    return this;
  }

  public void unsetHasResult() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __HASRESULT_ISSET_ID);
  }

  /** Returns true if field hasResult is set (has been assigned a value) and false otherwise */
  public boolean isSetHasResult() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __HASRESULT_ISSET_ID);
  }

  public void setHasResultIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __HASRESULT_ISSET_ID, value);
  }

  public int getColumnsListSize() {
    return (this.columnsList == null) ? 0 : this.columnsList.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> getColumnsListIterator() {
    return (this.columnsList == null) ? null : this.columnsList.iterator();
  }

  public void addToColumnsList(java.lang.String elem) {
    if (this.columnsList == null) {
      this.columnsList = new java.util.ArrayList<java.lang.String>();
    }
    this.columnsList.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.String> getColumnsList() {
    return this.columnsList;
  }

  public ExecuteStatementResp setColumnsList(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> columnsList) {
    this.columnsList = columnsList;
    return this;
  }

  public void unsetColumnsList() {
    this.columnsList = null;
  }

  /** Returns true if field columnsList is set (has been assigned a value) and false otherwise */
  public boolean isSetColumnsList() {
    return this.columnsList != null;
  }

  public void setColumnsListIsSet(boolean value) {
    if (!value) {
      this.columnsList = null;
    }
  }

  public int getRowListSize() {
    return (this.rowList == null) ? 0 : this.rowList.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.util.List<java.lang.String>> getRowListIterator() {
    return (this.rowList == null) ? null : this.rowList.iterator();
  }

  public void addToRowList(java.util.List<java.lang.String> elem) {
    if (this.rowList == null) {
      this.rowList = new java.util.ArrayList<java.util.List<java.lang.String>>();
    }
    this.rowList.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.util.List<java.lang.String>> getRowList() {
    return this.rowList;
  }

  public ExecuteStatementResp setRowList(@org.apache.thrift.annotation.Nullable java.util.List<java.util.List<java.lang.String>> rowList) {
    this.rowList = rowList;
    return this;
  }

  public void unsetRowList() {
    this.rowList = null;
  }

  /** Returns true if field rowList is set (has been assigned a value) and false otherwise */
  public boolean isSetRowList() {
    return this.rowList != null;
  }

  public void setRowListIsSet(boolean value) {
    if (!value) {
      this.rowList = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Status)value);
      }
      break;

    case IS_ABORT:
      if (value == null) {
        unsetIsAbort();
      } else {
        setIsAbort((java.lang.Boolean)value);
      }
      break;

    case HAS_RESULT:
      if (value == null) {
        unsetHasResult();
      } else {
        setHasResult((java.lang.Boolean)value);
      }
      break;

    case COLUMNS_LIST:
      if (value == null) {
        unsetColumnsList();
      } else {
        setColumnsList((java.util.List<java.lang.String>)value);
      }
      break;

    case ROW_LIST:
      if (value == null) {
        unsetRowList();
      } else {
        setRowList((java.util.List<java.util.List<java.lang.String>>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case STATUS:
      return getStatus();

    case IS_ABORT:
      return isIsAbort();

    case HAS_RESULT:
      return isHasResult();

    case COLUMNS_LIST:
      return getColumnsList();

    case ROW_LIST:
      return getRowList();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case STATUS:
      return isSetStatus();
    case IS_ABORT:
      return isSetIsAbort();
    case HAS_RESULT:
      return isSetHasResult();
    case COLUMNS_LIST:
      return isSetColumnsList();
    case ROW_LIST:
      return isSetRowList();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ExecuteStatementResp)
      return this.equals((ExecuteStatementResp)that);
    return false;
  }

  public boolean equals(ExecuteStatementResp that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_isAbort = true;
    boolean that_present_isAbort = true;
    if (this_present_isAbort || that_present_isAbort) {
      if (!(this_present_isAbort && that_present_isAbort))
        return false;
      if (this.isAbort != that.isAbort)
        return false;
    }

    boolean this_present_hasResult = true;
    boolean that_present_hasResult = true;
    if (this_present_hasResult || that_present_hasResult) {
      if (!(this_present_hasResult && that_present_hasResult))
        return false;
      if (this.hasResult != that.hasResult)
        return false;
    }

    boolean this_present_columnsList = true && this.isSetColumnsList();
    boolean that_present_columnsList = true && that.isSetColumnsList();
    if (this_present_columnsList || that_present_columnsList) {
      if (!(this_present_columnsList && that_present_columnsList))
        return false;
      if (!this.columnsList.equals(that.columnsList))
        return false;
    }

    boolean this_present_rowList = true && this.isSetRowList();
    boolean that_present_rowList = true && that.isSetRowList();
    if (this_present_rowList || that_present_rowList) {
      if (!(this_present_rowList && that_present_rowList))
        return false;
      if (!this.rowList.equals(that.rowList))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetStatus()) ? 131071 : 524287);
    if (isSetStatus())
      hashCode = hashCode * 8191 + status.hashCode();

    hashCode = hashCode * 8191 + ((isAbort) ? 131071 : 524287);

    hashCode = hashCode * 8191 + ((hasResult) ? 131071 : 524287);

    hashCode = hashCode * 8191 + ((isSetColumnsList()) ? 131071 : 524287);
    if (isSetColumnsList())
      hashCode = hashCode * 8191 + columnsList.hashCode();

    hashCode = hashCode * 8191 + ((isSetRowList()) ? 131071 : 524287);
    if (isSetRowList())
      hashCode = hashCode * 8191 + rowList.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ExecuteStatementResp other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status, other.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetIsAbort()).compareTo(other.isSetIsAbort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsAbort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.isAbort, other.isAbort);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetHasResult()).compareTo(other.isSetHasResult());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHasResult()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hasResult, other.hasResult);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetColumnsList()).compareTo(other.isSetColumnsList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumnsList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columnsList, other.columnsList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetRowList()).compareTo(other.isSetRowList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRowList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.rowList, other.rowList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ExecuteStatementResp(");
    boolean first = true;

    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("isAbort:");
    sb.append(this.isAbort);
    first = false;
    if (!first) sb.append(", ");
    sb.append("hasResult:");
    sb.append(this.hasResult);
    first = false;
    if (isSetColumnsList()) {
      if (!first) sb.append(", ");
      sb.append("columnsList:");
      if (this.columnsList == null) {
        sb.append("null");
      } else {
        sb.append(this.columnsList);
      }
      first = false;
    }
    if (isSetRowList()) {
      if (!first) sb.append(", ");
      sb.append("rowList:");
      if (this.rowList == null) {
        sb.append("null");
      } else {
        sb.append(this.rowList);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (status == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'status' was not present! Struct: " + toString());
    }
    // alas, we cannot check 'isAbort' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'hasResult' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
    if (status != null) {
      status.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ExecuteStatementRespStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExecuteStatementRespStandardScheme getScheme() {
      return new ExecuteStatementRespStandardScheme();
    }
  }

  private static class ExecuteStatementRespStandardScheme extends org.apache.thrift.scheme.StandardScheme<ExecuteStatementResp> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ExecuteStatementResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // STATUS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.status = new Status();
              struct.status.read(iprot);
              struct.setStatusIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // IS_ABORT
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.isAbort = iprot.readBool();
              struct.setIsAbortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // HAS_RESULT
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.hasResult = iprot.readBool();
              struct.setHasResultIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // COLUMNS_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.columnsList = new java.util.ArrayList<java.lang.String>(_list0.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _elem1;
                for (int _i2 = 0; _i2 < _list0.size; ++_i2)
                {
                  _elem1 = iprot.readString();
                  struct.columnsList.add(_elem1);
                }
                iprot.readListEnd();
              }
              struct.setColumnsListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // ROW_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list3 = iprot.readListBegin();
                struct.rowList = new java.util.ArrayList<java.util.List<java.lang.String>>(_list3.size);
                @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> _elem4;
                for (int _i5 = 0; _i5 < _list3.size; ++_i5)
                {
                  {
                    org.apache.thrift.protocol.TList _list6 = iprot.readListBegin();
                    _elem4 = new java.util.ArrayList<java.lang.String>(_list6.size);
                    @org.apache.thrift.annotation.Nullable java.lang.String _elem7;
                    for (int _i8 = 0; _i8 < _list6.size; ++_i8)
                    {
                      _elem7 = iprot.readString();
                      _elem4.add(_elem7);
                    }
                    iprot.readListEnd();
                  }
                  struct.rowList.add(_elem4);
                }
                iprot.readListEnd();
              }
              struct.setRowListIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetIsAbort()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'isAbort' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetHasResult()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'hasResult' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ExecuteStatementResp struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.status != null) {
        oprot.writeFieldBegin(STATUS_FIELD_DESC);
        struct.status.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(IS_ABORT_FIELD_DESC);
      oprot.writeBool(struct.isAbort);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(HAS_RESULT_FIELD_DESC);
      oprot.writeBool(struct.hasResult);
      oprot.writeFieldEnd();
      if (struct.columnsList != null) {
        if (struct.isSetColumnsList()) {
          oprot.writeFieldBegin(COLUMNS_LIST_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.columnsList.size()));
            for (java.lang.String _iter9 : struct.columnsList)
            {
              oprot.writeString(_iter9);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.rowList != null) {
        if (struct.isSetRowList()) {
          oprot.writeFieldBegin(ROW_LIST_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.LIST, struct.rowList.size()));
            for (java.util.List<java.lang.String> _iter10 : struct.rowList)
            {
              {
                oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, _iter10.size()));
                for (java.lang.String _iter11 : _iter10)
                {
                  oprot.writeString(_iter11);
                }
                oprot.writeListEnd();
              }
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ExecuteStatementRespTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ExecuteStatementRespTupleScheme getScheme() {
      return new ExecuteStatementRespTupleScheme();
    }
  }

  private static class ExecuteStatementRespTupleScheme extends org.apache.thrift.scheme.TupleScheme<ExecuteStatementResp> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ExecuteStatementResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.status.write(oprot);
      oprot.writeBool(struct.isAbort);
      oprot.writeBool(struct.hasResult);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetColumnsList()) {
        optionals.set(0);
      }
      if (struct.isSetRowList()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetColumnsList()) {
        {
          oprot.writeI32(struct.columnsList.size());
          for (java.lang.String _iter12 : struct.columnsList)
          {
            oprot.writeString(_iter12);
          }
        }
      }
      if (struct.isSetRowList()) {
        {
          oprot.writeI32(struct.rowList.size());
          for (java.util.List<java.lang.String> _iter13 : struct.rowList)
          {
            {
              oprot.writeI32(_iter13.size());
              for (java.lang.String _iter14 : _iter13)
              {
                oprot.writeString(_iter14);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ExecuteStatementResp struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.status = new Status();
      struct.status.read(iprot);
      struct.setStatusIsSet(true);
      struct.isAbort = iprot.readBool();
      struct.setIsAbortIsSet(true);
      struct.hasResult = iprot.readBool();
      struct.setHasResultIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list15 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.columnsList = new java.util.ArrayList<java.lang.String>(_list15.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _elem16;
          for (int _i17 = 0; _i17 < _list15.size; ++_i17)
          {
            _elem16 = iprot.readString();
            struct.columnsList.add(_elem16);
          }
        }
        struct.setColumnsListIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list18 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.rowList = new java.util.ArrayList<java.util.List<java.lang.String>>(_list18.size);
          @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> _elem19;
          for (int _i20 = 0; _i20 < _list18.size; ++_i20)
          {
            {
              org.apache.thrift.protocol.TList _list21 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
              _elem19 = new java.util.ArrayList<java.lang.String>(_list21.size);
              @org.apache.thrift.annotation.Nullable java.lang.String _elem22;
              for (int _i23 = 0; _i23 < _list21.size; ++_i23)
              {
                _elem22 = iprot.readString();
                _elem19.add(_elem22);
              }
            }
            struct.rowList.add(_elem19);
          }
        }
        struct.setRowListIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}


#include <stdexcept>
#include "client/DatumUtil.h"

Datum DatumUtil::boolDatum(const bool val){
  Datum datum;
  datum.__set_type(DataType::BOOL);
  Value value;
  value.__set_boolValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int8Datum(const int8_t val) {
  Datum datum;
  datum.__set_type(DataType::INT8);
  Value value;
  value.__set_int8Value(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int16Datum(const int16_t val) {
  Datum datum;
  datum.__set_type(DataType::INT16);
  Value value;
  value.__set_int16Value(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int32Datum(const int32_t val) {
  Datum datum;
  datum.__set_type(DataType::INT32);
  Value value;
  value.__set_int32Value(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int64Datum(const int64_t val) {
  Datum datum;
  datum.__set_type(DataType::INT64);
  Value value;
  value.__set_int64Value(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::doubleDatum(const double val) {
  Datum datum;
  datum.__set_type(DataType::DOUBLE);
  Value value;
  value.__set_doubleValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::stringDatum(const std::string& val) {
  Datum datum;
  datum.__set_type(DataType::STRING);
  Value value;
  value.__set_stringValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::binaryDatum(const std::string& val) {
  Datum datum;
  datum.__set_type(DataType::BINARY);
  Value value;
  value.__set_binaryValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::boolSetDatum(const std::vector<bool> & val) {
  Datum datum;
  datum.__set_type(DataType::BOOL_SET);
  Value value;
  value.__set_boolSetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int8SetDatum(const std::vector<int8_t> & val) {
  Datum datum;
  datum.__set_type(DataType::INT8_SET);
  Value value;
  value.__set_int8SetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int16SetDatum(const std::vector<int16_t> & val) {
  Datum datum;
  datum.__set_type(DataType::INT16_SET);
  Value value;
  value.__set_int16SetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int32SetDatum(const std::vector<int32_t> & val) {
  Datum datum;
  datum.__set_type(DataType::INT32_SET);
  Value value;
  value.__set_int32SetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::int64SetDatum(const std::vector<int64_t> & val) {
  Datum datum;
  datum.__set_type(DataType::INT64_SET);
  Value value;
  value.__set_int64SetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::doubleSetDatum(const std::vector<double> & val) {
  Datum datum;
  datum.__set_type(DataType::DOUBLE_SET);
  Value value;
  value.__set_doubleSetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::stringSetDatum(const std::vector<std::string> & val) {
  Datum datum;
  datum.__set_type(DataType::STRING_SET);
  Value value;
  value.__set_stringSetValue(val);
  datum.__set_value(value);
  return datum;
}

Datum DatumUtil::binarySetDatum(const std::vector<std::string> & val) {
  Datum datum;
  datum.__set_type(DataType::BINARY_SET);
  Value value;
  value.__set_binarySetValue(val);
  datum.__set_value(value);
  return datum;
}


bool DatumUtil::boolValue(const Datum& datum) {
  if (datum.type != DataType::BOOL) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.boolValue;
}

int8_t DatumUtil::int8Value(const Datum& datum) {
  if (datum.type != DataType::INT8) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int8Value;
}

int16_t DatumUtil::int16Value(const Datum& datum) {
  if (datum.type != DataType::INT16) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int16Value;
}

int32_t DatumUtil::int32Value(const Datum& datum) {
  if (datum.type != DataType::INT32) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int32Value;
}

int64_t DatumUtil::int64Value(const Datum& datum) {
  if (datum.type != DataType::INT64) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int64Value;
}

double DatumUtil::doubleValue(const Datum& datum) {
  if (datum.type != DataType::DOUBLE) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.doubleValue;
}

std::string DatumUtil::stringValue(const Datum& datum) {
  if (datum.type != DataType::STRING) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.stringValue;
}

std::string DatumUtil::binaryValue(const Datum& datum) {
  if (datum.type != DataType::BINARY) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.binaryValue;
}

std::vector<bool> DatumUtil::boolSetValue(const Datum& datum) {
  if (datum.type != DataType::BOOL_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.boolSetValue;
}

std::vector<int8_t> DatumUtil::int8SetValue(const Datum& datum) {
  if (datum.type != DataType::INT8_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int8SetValue;
}

std::vector<int16_t> DatumUtil::int16SetValue(const Datum& datum) {
  if (datum.type != DataType::INT16_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int16SetValue;
}

std::vector<int32_t> DatumUtil::int32SetValue(const Datum& datum) {
  if (datum.type != DataType::INT32_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int32SetValue;
}

std::vector<int64_t> DatumUtil::int64SetValue(const Datum& datum) {
  if (datum.type != DataType::INT64_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.int64SetValue;
}

std::vector<double> DatumUtil::doubleSetValue(const Datum& datum) {
  if (datum.type != DataType::DOUBLE_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.doubleSetValue;
}

std::vector<std::string> DatumUtil::stringSetValue(const Datum& datum) {
  if (datum.type != DataType::STRING_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.stringSetValue;
}

std::vector<std::string> DatumUtil::binarySetValue(const Datum& datum) {
  if (datum.type != DataType::BINARY_SET) {
    throw std::invalid_argument("Unexpected datum type");
  }
  return datum.value.binarySetValue;
}
#ifndef _SDS_DATUMUTIL_H_
#define _SDS_DATUMUTIL_H_


#include "sds/Table_types.h"

class DatumUtil {
public:
  static Datum boolDatum(const bool val);
  static Datum int8Datum(const int8_t val);
  static Datum int16Datum(const int16_t val);
  static Datum int32Datum(const int32_t val);
  static Datum int64Datum(const int64_t val);
  static Datum doubleDatum(double val);
  static Datum stringDatum(const std::string& val);
  static Datum binaryDatum(const std::string& val);
  static Datum boolSetDatum(const std::vector<bool> & val);
  static Datum int8SetDatum(const std::vector<int8_t> & val);
  static Datum int16SetDatum(const std::vector<int16_t> & val);
  static Datum int32SetDatum(const std::vector<int32_t> & val);
  static Datum int64SetDatum(const std::vector<int64_t> & val);
  static Datum doubleSetDatum(const std::vector<double> & val);
  static Datum binarySetDatum(const std::vector<std::string> & val);
  static Datum stringSetDatum(const std::vector<std::string> & val);

  static bool boolValue(const Datum& datum);
  static int8_t int8Value(const Datum& datum);
  static int16_t int16Value(const Datum& datum);
  static int32_t int32Value(const Datum& datum);
  static int64_t int64Value(const Datum& datum);
  static double doubleValue(const Datum& datum);
  static std::string stringValue(const Datum& datum);
  static std::string binaryValue(const Datum& datum);
  static std::vector<bool> boolSetValue(const Datum& datum);
  static std::vector<int8_t> int8SetValue(const Datum& datum);
  static std::vector<int16_t> int16SetValue(const Datum& datum);
  static std::vector<int32_t> int32SetValue(const Datum& datum);
  static std::vector<int64_t> int64SetValue(const Datum& datum);
  static std::vector<double> doubleSetValue(const Datum& datum);
  static std::vector<std::string> stringSetValue(const Datum& datum);
  static std::vector<std::string> binarySetValue(const Datum& datum);
};

#endif

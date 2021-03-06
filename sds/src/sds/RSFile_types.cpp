/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "RSFile_types.h"

#include <algorithm>



int _kCompressionValues[] = {
  Compression::NONE,
  Compression::SNAPPY
};
const char* _kCompressionNames[] = {
  "NONE",
  "SNAPPY"
};
const std::map<int, const char*> _Compression_VALUES_TO_NAMES(::apache::thrift::TEnumIterator(2, _kCompressionValues, _kCompressionNames), ::apache::thrift::TEnumIterator(-1, NULL, NULL));

int _kChecksumValues[] = {
  Checksum::NONE,
  Checksum::CRC32
};
const char* _kChecksumNames[] = {
  "NONE",
  "CRC32"
};
const std::map<int, const char*> _Checksum_VALUES_TO_NAMES(::apache::thrift::TEnumIterator(2, _kChecksumValues, _kChecksumNames), ::apache::thrift::TEnumIterator(-1, NULL, NULL));


RSFileHeader::~RSFileHeader() throw() {
}


void RSFileHeader::__set_magic(const std::string& val) {
  this->magic = val;
__isset.magic = true;
}

void RSFileHeader::__set_version(const int32_t val) {
  this->version = val;
__isset.version = true;
}

void RSFileHeader::__set_compression(const Compression::type val) {
  this->compression = val;
__isset.compression = true;
}

void RSFileHeader::__set_checksum(const Checksum::type val) {
  this->checksum = val;
__isset.checksum = true;
}

void RSFileHeader::__set_count(const int64_t val) {
  this->count = val;
__isset.count = true;
}

void RSFileHeader::__set_metadata(const std::string& val) {
  this->metadata = val;
__isset.metadata = true;
}

const char* RSFileHeader::ascii_fingerprint = "7864CDFCC618C95F19082A02B254B4E6";
const uint8_t RSFileHeader::binary_fingerprint[16] = {0x78,0x64,0xCD,0xFC,0xC6,0x18,0xC9,0x5F,0x19,0x08,0x2A,0x02,0xB2,0x54,0xB4,0xE6};

uint32_t RSFileHeader::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->magic);
          this->__isset.magic = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->version);
          this->__isset.version = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast0;
          xfer += iprot->readI32(ecast0);
          this->compression = (Compression::type)ecast0;
          this->__isset.compression = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          int32_t ecast1;
          xfer += iprot->readI32(ecast1);
          this->checksum = (Checksum::type)ecast1;
          this->__isset.checksum = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->count);
          this->__isset.count = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 6:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readBinary(this->metadata);
          this->__isset.metadata = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t RSFileHeader::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  oprot->incrementRecursionDepth();
  xfer += oprot->writeStructBegin("RSFileHeader");

  if (this->__isset.magic) {
    xfer += oprot->writeFieldBegin("magic", ::apache::thrift::protocol::T_STRING, 1);
    xfer += oprot->writeString(this->magic);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.version) {
    xfer += oprot->writeFieldBegin("version", ::apache::thrift::protocol::T_I32, 2);
    xfer += oprot->writeI32(this->version);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.compression) {
    xfer += oprot->writeFieldBegin("compression", ::apache::thrift::protocol::T_I32, 3);
    xfer += oprot->writeI32((int32_t)this->compression);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.checksum) {
    xfer += oprot->writeFieldBegin("checksum", ::apache::thrift::protocol::T_I32, 4);
    xfer += oprot->writeI32((int32_t)this->checksum);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.count) {
    xfer += oprot->writeFieldBegin("count", ::apache::thrift::protocol::T_I64, 5);
    xfer += oprot->writeI64(this->count);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.metadata) {
    xfer += oprot->writeFieldBegin("metadata", ::apache::thrift::protocol::T_STRING, 6);
    xfer += oprot->writeBinary(this->metadata);
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  oprot->decrementRecursionDepth();
  return xfer;
}

void swap(RSFileHeader &a, RSFileHeader &b) {
  using ::std::swap;
  swap(a.magic, b.magic);
  swap(a.version, b.version);
  swap(a.compression, b.compression);
  swap(a.checksum, b.checksum);
  swap(a.count, b.count);
  swap(a.metadata, b.metadata);
  swap(a.__isset, b.__isset);
}

RSFileHeader::RSFileHeader(const RSFileHeader& other2) {
  magic = other2.magic;
  version = other2.version;
  compression = other2.compression;
  checksum = other2.checksum;
  count = other2.count;
  metadata = other2.metadata;
  __isset = other2.__isset;
}
RSFileHeader& RSFileHeader::operator=(const RSFileHeader& other3) {
  magic = other3.magic;
  version = other3.version;
  compression = other3.compression;
  checksum = other3.checksum;
  count = other3.count;
  metadata = other3.metadata;
  __isset = other3.__isset;
  return *this;
}

Record::~Record() throw() {
}


void Record::__set_data(const std::string& val) {
  this->data = val;
__isset.data = true;
}

void Record::__set_checksum(const int32_t val) {
  this->checksum = val;
__isset.checksum = true;
}

void Record::__set_eof(const bool val) {
  this->eof = val;
__isset.eof = true;
}

const char* Record::ascii_fingerprint = "F9F753F2C64C625D0CA44DE5CC4AC024";
const uint8_t Record::binary_fingerprint[16] = {0xF9,0xF7,0x53,0xF2,0xC6,0x4C,0x62,0x5D,0x0C,0xA4,0x4D,0xE5,0xCC,0x4A,0xC0,0x24};

uint32_t Record::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readBinary(this->data);
          this->__isset.data = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->checksum);
          this->__isset.checksum = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->eof);
          this->__isset.eof = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t Record::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  oprot->incrementRecursionDepth();
  xfer += oprot->writeStructBegin("Record");

  if (this->__isset.data) {
    xfer += oprot->writeFieldBegin("data", ::apache::thrift::protocol::T_STRING, 1);
    xfer += oprot->writeBinary(this->data);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.checksum) {
    xfer += oprot->writeFieldBegin("checksum", ::apache::thrift::protocol::T_I32, 2);
    xfer += oprot->writeI32(this->checksum);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.eof) {
    xfer += oprot->writeFieldBegin("eof", ::apache::thrift::protocol::T_BOOL, 3);
    xfer += oprot->writeBool(this->eof);
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  oprot->decrementRecursionDepth();
  return xfer;
}

void swap(Record &a, Record &b) {
  using ::std::swap;
  swap(a.data, b.data);
  swap(a.checksum, b.checksum);
  swap(a.eof, b.eof);
  swap(a.__isset, b.__isset);
}

Record::Record(const Record& other4) {
  data = other4.data;
  checksum = other4.checksum;
  eof = other4.eof;
  __isset = other4.__isset;
}
Record& Record::operator=(const Record& other5) {
  data = other5.data;
  checksum = other5.checksum;
  eof = other5.eof;
  __isset = other5.__isset;
  return *this;
}


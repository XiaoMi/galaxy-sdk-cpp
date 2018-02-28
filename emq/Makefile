SRC = $(wildcard *.cpp ./examples/*.cpp ./client/*.cpp ./rpc/*.cpp ./emq/*cpp ./signer/*.cpp)
OBJ = $(SRC:.cpp=.o)

BOOST_LIB=#Set your boost lib path
BOOST_INC=#Set your boost include path
THRIFT_LIB=#Set your thrift lib path
THRIFT_INC=#Set your thrift include path
POCO_LIB=#Set your poco lib path

CPPFLAGS = -I$(BOOST_INC) -I$(THRIFT_INC) -I./
LDFLAGS = -L$(BOOST_LIB) -L$(THRIFT_LIB) -L$(POCO_LIB)
LIBS = -lthrift  -lcrypto -lssl -lPocoFoundation -lPocoNet  -lPocoNetSSL -lPocoUtil -lPocoXML


all : emqbasic

emqbasic : $(OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)
clean:
	-rm emqbasic $(OBJ) $(TMP)
.PHONY: clean

TMP = $(SRC:.cpp=.d) 
include $(TMP)

%.d: %.cpp
	set -e; rm -f $@; \
	$(CXX) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\(.*\)\.o, $(@:.d=.o),g'  < $@.$$$$ > $@; \
	rm -f $@.$$$$

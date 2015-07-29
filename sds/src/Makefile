SRC = $(wildcard *.cpp ./examples/*.cpp ./client/*.cpp ./sds/*.cpp)
OBJ = $(SRC:.cpp=.o)

BOOST_LIB =  #Set your boost lib path
BOOST_INC =  #Set your boost include path
THRIFT_LIB = #Set your thrift lib path
THRIFT_INC = #Set your thrift include path

CPPFLAGS = -I$(BOOST_INC) -I$(THRIFT_INC) -I./
LDFLAGS = -L$(BOOST_LIB) -L$(THRIFT_LIB)
LIBS = -lthrift  -lcrypto -lssl


all : basic

basic : $(OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)
clean:
	-rm basic $(OBJ) $(TMP)
.PHONY: clean

TMP = $(SRC:.cpp=.d) 
include $(TMP)

%.d: %.cpp
	set -e; rm -f $@; \
	$(CXX) -MM $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\(.*\)\.o, $(@:.d=.o),g'  < $@.$$$$ > $@; \
	rm -f $@.$$$$

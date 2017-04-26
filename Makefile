all: as-to-kafka consumer

CFLAGS += $(shell pkg-config rdkafka --cflags)
LIBS += $(shell pkg-config rdkafka --libs)

CFLAGS += $(shell pkg-config msgpack --cflags)
LIBS += $(shell pkg-config msgpack --libs)

AEROSPIKE_HOME=/usr/local
CFLAGS += -I$(AEROSPIKE_HOME)/include
LIBS += -L$(AEROSPIKE_HOME)/lib -laerospike

DEPS = util.h

as-to-kafka: as-to-kafka.o util.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

consumer: consumer.o util.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	$(RM) *.o as-to-kafka consumer

/*
 * Copyright 2017 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_config.h>
#include <aerospike/as_query.h>
#include <aerospike/as_predexp.h>
#include <aerospike/as_record.h>
#include <aerospike/as_boolean.h>

#include <msgpack.h>

#include "util.h"

static rd_kafka_t* g_rk;         // Producer instance handle
static rd_kafka_topic_t* g_rkt;  // Topic object
static int g_run = 1;            // Run main loop while this is true

/**
 * Signal termination of program
 */
static void
stop (int sig)
{
	g_run = 0;
}

static void pack_val(msgpack_packer* pk, const as_val* val);

static bool
pack_list_item(as_val* val, void* udata)
{
	msgpack_packer* pk = (msgpack_packer*) udata;
	pack_val(pk, val);
	return true;
}

static bool
pack_map_item(const as_val* key, const as_val* val, void* udata)
{
	msgpack_packer* pk = (msgpack_packer*) udata;
	pack_val(pk, key);
	pack_val(pk, val);
	return true;
}

static void
pack_val(msgpack_packer* pk, const as_val* val)
{
	bool packed = false;
	switch(as_val_type(val)) {
		case AS_STRING: {
			as_string* str_val = as_string_fromval(val);
			if (str_val) {
				size_t str_len = as_string_len(str_val);
				msgpack_pack_str(pk, str_len);
				msgpack_pack_str_body(pk, str_val->value, str_len);
				packed = true;
			}
			break;
		}
		case AS_BYTES: {
			as_bytes* bytes_val = as_bytes_fromval(val);
			if (bytes_val) {
				msgpack_pack_bin(pk, bytes_val->size);
				msgpack_pack_bin_body(pk, bytes_val->value, bytes_val->size);
				packed = true;
			}
			break;
		}
		case AS_DOUBLE: {
			as_double* double_val = as_double_fromval(val);
			if (double_val) {
				msgpack_pack_double(pk, double_val->value);
				packed = true;
			}
			break;
		}
		case AS_INTEGER: {
			as_integer* int_val = as_integer_fromval(val);
			if (int_val) {
				msgpack_pack_int64(pk, int_val->value);
				packed = true;
			}
			break;
		}
		case AS_BOOLEAN: {
			as_boolean* bool_val = as_boolean_fromval(val);
			if (bool_val) {
				if (bool_val->value) {
					msgpack_pack_true(pk);
				} else {
					msgpack_pack_false(pk);
				}
				packed = true;
			}
			break;
		}
		case AS_GEOJSON: {
			as_geojson* geo_val = as_geojson_fromval(val);
			if (geo_val) {
				// TBD: pack as ext type instead?
				msgpack_pack_str(pk, geo_val->len);
				msgpack_pack_str_body(pk, geo_val->value, geo_val->len);
				packed = true;
			}
			break;
		}
		case AS_LIST: {
			as_list* list_val = as_list_fromval((as_val*) val);
			if (list_val) {
				uint32_t size = as_list_size(list_val);
				msgpack_pack_array(pk, size);
				as_list_foreach(list_val, pack_list_item, pk);
				packed = true;
			}
			break;
		}
		case AS_MAP: {
			as_map* map_val = as_map_fromval(val);
			if (map_val) {
				uint32_t size = as_map_size(map_val);
				msgpack_pack_map(pk, size);
				as_map_foreach(map_val, pack_map_item, pk);
				packed = true;
			}
			break;
		}
		case AS_UNDEF:
		case AS_NIL:
		case AS_REC:
		case AS_PAIR:
			break;
	}
	if (!packed) {
		fprintf(stderr, "Skipping bin of type %u\n", as_val_type(val));
		msgpack_pack_nil(pk);
	}
}

/**
 * Packs a single bin into a Msgpack buffer
 * This is called by as_record_foreach.
 */
static bool
pack_bin(const char* name, const as_val* val, void* udata)
{
	msgpack_packer* pk = (msgpack_packer*) udata;
	size_t len = strlen(name);
	msgpack_pack_str(pk, len);
	msgpack_pack_str_body(pk, name, len);
	pack_val(pk, val);
	return true;
}

/**
 * Packs a record into a Msgpack buffer
 */
static void
pack_record(msgpack_sbuffer* sbuf, const as_record* rec)
{
	msgpack_sbuffer_init(sbuf);

	msgpack_packer pk;
	msgpack_packer_init(&pk, sbuf, msgpack_sbuffer_write);

	size_t numbins = as_record_numbins(rec);
	msgpack_pack_map(&pk, numbins);

	as_record_foreach(rec, pack_bin, (void*) &pk);
}

/**
 * Packs a key into a Msgpack buffer: [ ns, set, digest ]
 */
void
pack_key(msgpack_sbuffer* sbuf, const as_key* key)
{
	msgpack_sbuffer_init(sbuf);

	msgpack_packer pk;
	msgpack_packer_init(&pk, sbuf, msgpack_sbuffer_write);

	msgpack_pack_array(&pk, 3);

	size_t len = strlen(key->ns);
	msgpack_pack_str(&pk, len);
	msgpack_pack_str_body(&pk, key->ns, len);

	len = strlen(key->set);
	msgpack_pack_str(&pk, len);
	msgpack_pack_str_body(&pk, key->set, len);

	msgpack_pack_bin(&pk, AS_DIGEST_VALUE_SIZE);
	msgpack_pack_bin_body(&pk, key->digest.value, AS_DIGEST_VALUE_SIZE);
}

/**
 * Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	if (rkmessage->err) {
		fprintf(stderr, "%% Message delivery failed: %s\n",
				rd_kafka_err2str(rkmessage->err));
	} else {
		fprintf(stderr,
				"%% Message delivered (%zd bytes, "
				"partition %"PRId32")\n",
				rkmessage->len, rkmessage->partition);
	}

	// The rkmessage is destroyed automatically by librdkafka
}

static bool
query_cb (const as_val* val, void* udata)
{
	if (!val) {
		// query callback returned null - query is complete
		return true;
	}

	fprintf(stdout, "%% Processing updated record\n");
	as_record* rec = as_record_fromval(val);
	if (!rec) {
		fprintf(stderr, "query callback return value of unexpected type %d\n",
				as_val_type(val));
		return true;
	}

	msgpack_sbuffer key_sbuf;
	pack_key(&key_sbuf, &rec->key);
	hexdump(stdout, "key", key_sbuf.data, key_sbuf.size);
	unpack(stdout, "key", key_sbuf.data, key_sbuf.size);

	msgpack_sbuffer rec_sbuf;
	pack_record(&rec_sbuf, rec);
	hexdump(stdout, "record", rec_sbuf.data, rec_sbuf.size);
	unpack(stdout, "record", rec_sbuf.data, rec_sbuf.size);

retry:
	if (rd_kafka_produce(g_rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
				rec_sbuf.data, rec_sbuf.size, key_sbuf.data, key_sbuf.size,
				NULL) == -1) {
		fprintf(stderr,
				"%% Failed to produce to topic %s: %s\n",
				rd_kafka_topic_name(g_rkt),
				rd_kafka_err2str(rd_kafka_last_error()));

		if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
			// If the internal queue is full, wait for messages to be delivered
			// and then retry.
			rd_kafka_poll(g_rk, 1000);
			goto retry;
		}
	} else {
		fprintf(stderr, "Enqueued message (%zu bytes) for topic %s\n",
				rec_sbuf.size, rd_kafka_topic_name(g_rkt));
	}

	msgpack_sbuffer_destroy(&key_sbuf);
	msgpack_sbuffer_destroy(&rec_sbuf);

	return true;
}

void
run_query(aerospike* as, const char* namespace, const char* set, const time_t* time)
{
	as_error err;
	as_query query;
	int64_t time_ns = 1e9 * (int64_t) *time;

	as_query_init(&query, namespace, set);
	as_query_predexp_init(&query, 3);
	as_query_predexp_add(&query, as_predexp_rec_last_update());
	as_query_predexp_add(&query, as_predexp_integer_value(time_ns));
	as_query_predexp_add(&query, as_predexp_integer_greater());

	if (aerospike_query_foreach(as, &err, NULL, &query, query_cb, NULL) !=
			AEROSPIKE_OK) {
		if (err.code != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			fprintf(stderr, "%% Failed to query Aerospike cluster: %s [%d]\n",
					err.message, err.code);
		}
	}

	as_query_destroy(&query);
}

int
main(int argc, char **argv)
{
	const char* brokers;    // Argument: broker list
	const char* topic;      // Argument: topic to produce to
	const char* hosts;      // Argument: Aerospike cluster hosts
	const char* namespace;  // Argument: Aerospike namespace
	const char* set;        // Argument: Aerospike set
	rd_kafka_conf_t* conf;  // Temporary configuration object
	char errstr[512];       // librdkafka API error reporting buffer
	aerospike as;           // Aerospike client
	as_config as_conf;      // Temporary configuration object
	as_error as_err;        // Aerospike client error object
	time_t last_query_ts;   // Time of last query

	if (argc != 6) {
		fprintf(stderr, "%% Usage: %s <broker> <topic> <hosts> <namespace> <set>\n", argv[0]);
		return 1;
	}

	// Kafka bootstrap broker(s) as a comma-separated list of
	// host or host:port (default port 9092).
	// librdkafka will use the bootstrap brokers to acquire the full
	// set of brokers from the cluster.
	brokers = argv[1];
	// Kafka topic to publish data to
	topic = argv[2];
	// Aerospike cluster host(s) as a comma-separated list of
	// host or host:port (default port 3000).
	hosts = argv[3];
	// Aerospike namespace to fetch data from
	namespace = argv[4];
	// Aerospike set to fetch data from
	set = argv[5];

	conf = rd_kafka_conf_new();
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
				errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%s\n", errstr);
		return 1;
	}
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

	g_rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!g_rk) {
		fprintf(stderr, "Failed to create new producer: %s\n", errstr);
		return 1;
	}

	g_rkt = rd_kafka_topic_new(g_rk, topic, NULL);
	if (!g_rkt) {
		fprintf(stderr, "%% Failed to create topic object: %s\n",
				rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(g_rk);
		return 1;
	}

	as_config_init(&as_conf);
	if (!as_config_add_hosts(&as_conf, hosts, 3000)) {
		fprintf(stderr, "%% Invalid Aerospike cluster host address(es) %s\n",
				hosts);
		return 1;
	}

	aerospike_init(&as, &as_conf);
	if (aerospike_connect(&as, &as_err) != AEROSPIKE_OK) {
		fprintf(stderr, "%% Failed to connect to Aerospike cluster: %s [%d]\n",
				as_err.message, as_err.code);
		aerospike_destroy(&as);
		rd_kafka_destroy(g_rk);
		return 1;
	}

	// Signal handler for clean shutdown
	signal(SIGINT, stop);

	fprintf(stderr, "Press Ctrl-C to exit\n");

	time(&last_query_ts);

	while (g_run) {
		run_query(&as, namespace, set, &last_query_ts);
		time(&last_query_ts);
		rd_kafka_poll(g_rk, 0);
		sleep(5);
	}

	aerospike_close(&as, &as_err);
	aerospike_destroy(&as);

	fprintf(stderr, "Flushing final messages..\n");
	rd_kafka_flush(g_rk, 10 * 1000);
	rd_kafka_topic_destroy(g_rkt);
	rd_kafka_destroy(g_rk);

	return 0;
}

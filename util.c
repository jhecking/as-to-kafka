#include <ctype.h>
#include <msgpack.h>

void hexdump (FILE *fp, const char *name, const void *ptr, size_t len) {
	const char *p = (const char *)ptr;
	unsigned int of = 0;

	if (name)
		fprintf(fp, "%s hexdump (%zu bytes):\n", name, len);

	for (of = 0 ; of < len ; of += 16) {
		char hexen[16*3+1];
		char charen[16+1];
		int hof = 0;

		int cof = 0;
		int i;

		for (i = of ; i < (int)of + 16 && i < (int)len ; i++) {
			hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
			cof += sprintf(charen+cof, "%c",
				       isprint((int)p[i]) ? p[i] : '.');
		}
		fprintf(fp, "%08x: %-48s %-16s\n",
			of, hexen, charen);
	}
}

void unpack (FILE *fp, const char *name, const void *ptr, size_t len) {
	if (name)
		fprintf(fp, "%s msgpack (%zu bytes):\n", name, len);

	/* deserialize the buffer into msgpack_object instance. */
	/* deserialized object is valid during the msgpack_zone instance alive. */
	msgpack_zone mempool;
	msgpack_zone_init(&mempool, 2048);

	msgpack_object deserialized;
	msgpack_unpack(ptr, len, NULL, &mempool, &deserialized);

	/* print the deserialized object. */
	msgpack_object_print(fp, deserialized);
	fputs("\n", fp);

	msgpack_zone_destroy(&mempool);
}

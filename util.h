#include <stdio.h>

void hexdump (FILE *fp, const char *name, const void *ptr, size_t len);
void unpack (FILE *fp, const char *name, const void *ptr, size_t len);

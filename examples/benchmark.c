#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <rabinpoly.h>
#include <stdlib.h>
// #include <crypto.h>
// #include <openssl/md5.h>



#define POLYNOM 0x3f63dfbf84af3b
int main(int argc, char **argv){
	int i;

	printf("argc %d\n", argc);
	for (i=0; i < argc; i++) {
		printf("argv[%d] %s\n", i, argv[i]);
	}

	unsigned int window_size = atoi(argv[1]);
	size_t min_block_size = 1<<13;
	size_t avg_block_size = 1<<17;
	size_t max_block_size = 1<<23;
	size_t buf_size = max_block_size * 10;

	RabinPoly *rp;

	rp = rp_new(window_size,
            avg_block_size, min_block_size, max_block_size, buf_size, POLYNOM);
	rp_from_file(rp, argv[2]);

    for (;;) {

        int rc = rp_block_next(rp);
        if (rc) {
            assert (rc == EOF);
            break;
        } else {
		printf("Chunksize: %lu fingerprint: %#.16lx\n", rp->block_size,
		       rp->fingerprint);
	}

    }

    //assert(feof(stdin));

	rp_free(rp);

	return 0;
}

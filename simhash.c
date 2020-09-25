#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "src/rabinpoly.h"


#define SZ_16M (16*1024*1024)
#define NUM_HASHES ((2<<24) - 512)
#define M_OFFSET 8

char filebuf[SZ_16M];
int find_largest(uint64_t *numbers)
{
	uint64_t largest = 0;
	int found_index = 0;
	for (int i = 0; i < NUM_HASHES; i++) {
		if (numbers[i] > largest) {
			largest = numbers[i];
			found_index = i;
		}
	}

	// so that we can call it multiple times and always get a larger value
	numbers[found_index] = 0;

	return found_index;
}


int main(int arg, char **argv)
{
	size_t chunk_off = 0;
	uint64_t *hashlist = calloc(NUM_HASHES, sizeof(uint64_t));
	int fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("Error opening file");
		return 1;
	}

	while(1) {
		int i = 0;
		int rc = 0;
		int idx1 = 0, idx2 = 0, idx3 = 0, idx4 = 0;
		RabinPoly *rp = rp_new(512,SZ_16M,SZ_16M,SZ_16M, SZ_16M, 0x3f63dfbf84af3b);
		ssize_t	count = read(fd, filebuf, SZ_16M);
		if (count != SZ_16M) {
			printf("Short read: %ld\n", count);
			return 1;
		}

		rp_from_buffer(rp, filebuf, SZ_16M);

		// calculate first 512 bytes this fills our sliding window
		for (i = 0; i < 512; i++)
			calc_rabin(rp);
		i = 0;

		//save first hash
		hashlist[i++] = rp->fingerprint;

		// calculate every Ji'th hash sum and store it
		while ((rc = calc_rabin(rp)) != EOF)
			hashlist[i++] = rp->fingerprint;

		// This finds the indexes and shifts them by
		// M_OFFSET
		idx1 = find_largest(hashlist) + M_OFFSET;
		idx2 = find_largest(hashlist) + M_OFFSET;
		idx3 = find_largest(hashlist) + M_OFFSET;
		idx4 = find_largest(hashlist) + M_OFFSET;

		assert(idx1 < i && idx2< i && idx3 < i && idx4 < i);
		printf("Chunk: %lu-%lu repr hashes: %u %u %u %u total: %d\n",
		       chunk_off, chunk_off + count, idx1, idx2, idx3, idx4, i);

		//Prep for next iteration
		rp_free(rp);
		memset(hashlist, sizeof(uint64_t)*NUM_HASHES, 0);
		memset(filebuf, SZ_16M, 0);
		chunk_off =+ SZ_16M;
	}

	return 0;

}

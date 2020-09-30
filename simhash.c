#include <sys/stat.h>
#include <sys/types.h>
#include <linux/limits.h>
#include <dirent.h>
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


static char path[PATH_MAX] = {0,};
static char *pathp = path;

struct chunk_hash {
	uint64_t unit_hashes[4];
	unsigned int off;
	int fd;

};

static int stor_index = 0;
struct chunk_hash *hashes;

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

int hash_chunk(int fd, int chunk_off)
{
	// no need to alloc 16m on every iter
	static char filebuf[SZ_16M];
	static uint64_t chunk_hashlist[NUM_HASHES];
	int i = 0;
	int rc = 0;
	int idx;
	struct chunk_hash *chunk = &hashes[stor_index++];
	RabinPoly *rp = rp_new(512,SZ_16M,SZ_16M,SZ_16M, SZ_16M, 0x3f63dfbf84af3b);
	ssize_t	count = read(fd, filebuf, SZ_16M);
	if (count != SZ_16M) {
		printf("Short chunk - skipping: %ld\n", count);
		return 1;
	}

	rp_from_buffer(rp, filebuf, SZ_16M);

	// calculate first 512 bytes this fills our sliding window
	for (i = 0; i < 512; i++)
		calc_rabin(rp);
	i = 0;

	//save first hash
	chunk_hashlist[i++] = rp->fingerprint;

	// calculate every Ji'th hash sum and store it
	while ((rc = calc_rabin(rp)) != EOF)
		chunk_hashlist[i++] = rp->fingerprint;

	// This finds the indexes and shifts them by
	// M_OFFSET
	for (int c = 0; c < 4; c++) {
		int hash_index = find_largest(chunk_hashlist) + M_OFFSET;
		assert(hash_index < i);
		chunk->unit_hashes[c] = chunk_hashlist[hash_index];
	}
	chunk->off = chunk_off;

	assert(chunk->unit_hashes[0] > chunk->unit_hashes[1] &&
	       chunk->unit_hashes[1] > chunk->unit_hashes[2] &&
	       chunk->unit_hashes[2] > chunk->unit_hashes[3]);

	//Prep for next iteration
	rp_free(rp);
	memset(chunk_hashlist, sizeof(uint64_t)*NUM_HASHES, 0);
	memset(filebuf, SZ_16M, 0);

	return 0;
}

static int get_dirent_type(struct dirent *entry, int fd)
{
	int ret;
	struct stat st;

	if (entry->d_type != DT_UNKNOWN)
		return entry->d_type;

	/*
	 * FS doesn't support file type in dirent, do this the old
	 * fashioned way. We translate mode to DT_* for the
	 * convenience of the caller.
	 */
	ret = fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW);
	if (ret) {
		fprintf(stderr,
			"Error %d: %s while getting type of file %s/%s. "
			"Skipping.\n",
			errno, strerror(errno), path, entry->d_name);
		return DT_UNKNOWN;
	}

	if (S_ISREG(st.st_mode))
		return DT_REG;
	if (S_ISDIR(st.st_mode))
		return DT_DIR;
	if (S_ISBLK(st.st_mode))
		return DT_BLK;
	if (S_ISCHR(st.st_mode))
		return DT_CHR;
	if (S_ISFIFO(st.st_mode))
		return DT_FIFO;
	if (S_ISLNK(st.st_mode))
		return DT_LNK;
	if (S_ISSOCK(st.st_mode))
		return DT_SOCK;

	return DT_UNKNOWN;
}

int hash_file(char *filename)
{
	char abspath[PATH_MAX];
	int chunk_off = 0;
	int ret = 0;

	if (realpath(filename, abspath) == NULL) {
		printf("Error %d: %s while getting path to file %s\n",
		       errno, strerror(errno), filename);
	}

	printf("Hashing file %s\n", abspath);
	int fd = open(abspath, O_RDONLY);
	if (fd < 0) {
		perror("Error opening file");
		return 1;
	}

	while (!ret) {
		ret = hash_chunk(fd, chunk_off);
		chunk_off += SZ_16M;
	}

	close(fd);
}


static int walk_dir(const char *name)
{
	int ret = 0;
	int type;
	struct dirent *entry;
	DIR *dirp;
	char abspath[PATH_MAX];

	if (realpath(name, abspath) == NULL) {
		printf("Error resolving initial dir\n");
	}

	dirp = opendir(abspath);
	if (dirp == NULL) {
		fprintf(stderr, "Error %d: %s while opening directory %s\n",
			errno, strerror(errno), name);
		return 0;
	}

	// global path no contains the TLD we are scanning from
	ret = sprintf(pathp, "%s", name);
	pathp += ret;

	do {
		errno = 0;
		entry = readdir(dirp);
		if (entry) {
			if (strcmp(entry->d_name, ".") == 0
			    || strcmp(entry->d_name, "..") == 0)
				continue;

			type = get_dirent_type(entry, dirfd(dirp));
			if (type == DT_REG) {
				//pathtmp now points to the root dir
				char *pathtmp = pathp;

				// adds a terminating null char
				ret = sprintf(pathp, "/%s", entry->d_name);

				if (hash_file(path)) {
					ret = 1;
					goto out;
				}

				//pathp again points to rootdir, ready for
				//next iter
				pathp = pathtmp;
			}
		}
	} while (entry != NULL);

	if (errno) {
		fprintf(stderr, "Error %d: %s while reading directory %s\n",
			errno, strerror(errno), path);
	}

out:
	closedir(dirp);
	return ret;
}

int main(int arg, char **argv)
{
	size_t chunk_off = 0;
	int ret;

	/* 1k chunks can be hashed, enough for testing */
	hashes = calloc(1000, sizeof(struct chunk_hash));
	if (hashes == NULL) {
		printf("Error allocating memory");
		exit(1);
	}

	return walk_dir(argv[1]);
}

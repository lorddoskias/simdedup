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
#include <time.h>
#include "src/rabinpoly.h"


#define SZ_8M (8*1024*1024)
#define SZ_16M (16*1024*1024)
#define BUFSIZE SZ_8M
#define NUM_HASHES ((1<<23) - 512)
#define M_OFFSET 8
#define CHUNK_SIZE SZ_8M


clock_t overall_begin, overall_end;
clock_t read_elapsed, hash_elapsed;

static char path[PATH_MAX] = {0,};
static char *pathp = path;

struct chunk_hash {
	uint64_t unit_hashes[4];
	loff_t  off;
	int fd;
	char filename[PATH_MAX];

};

struct max_array {
	// top 4 hashes
	uint8_t size;
	struct max_hash {
		int index;
		uint64_t hash;
	} max_hashes[4];

	uint64_t chunk_hashlist[NUM_HASHES];
	int i;
};

static int stor_index = 0;
struct chunk_hash *hashes;

#define parent(i) (i-1)/2
#define left(i)	  (i*2+1)
#define right(i)  (i*2+2)

static void swap(struct max_hash *h1, struct max_hash *h2)
{
	struct max_hash temp = *h1;
	*h1 = *h2;
	*h2 = temp;
}

void min_heapify(struct max_array *ctx, int i)
{
	int left_idx = left(i);
	int right_idx = right(i);
	int smallest = i;
	if (left_idx < ctx->size && ctx->max_hashes[left_idx].hash < ctx->max_hashes[i].hash)
		smallest = left_idx;
	if (right_idx < ctx->size && ctx->max_hashes[right_idx].hash < ctx->max_hashes[smallest].hash)
		smallest = right_idx;
	if (smallest != i) {
		swap(&ctx->max_hashes[i], &ctx->max_hashes[smallest]);
		min_heapify(ctx, smallest);

	}
}

static void pop_heap(struct max_array *ctx)
{
	ctx->max_hashes[0] = ctx->max_hashes[--ctx->size];
	assert(ctx->size >= 0 && ctx->size <= 3);
	min_heapify(ctx, 0);
}

static void add_heap(struct max_array *ctx, int i, uint64_t hash)
{
	int k = ctx->size++;
	assert(k>= 0 && k<=3);
	// insert at the end
	ctx->max_hashes[k].hash = hash;
	ctx->max_hashes[k].index = i;

	while (k != 0 && ctx->max_hashes[parent(k)].hash > ctx->max_hashes[k].hash) {
		swap(&ctx->max_hashes[k], &ctx->max_hashes[parent(k)]);
		k = parent(k);
	}

}

static void insert_hash(struct max_array *ctx, uint64_t hash)
{
	int idx = ctx->i++;
	assert(idx < NUM_HASHES);

	ctx->chunk_hashlist[idx] = hash;
	if (ctx->size < 4) {
		add_heap(ctx, idx, hash);
	} else {
		if (ctx->max_hashes[0].hash < hash) {
			pop_heap(ctx);
			add_heap(ctx, idx, hash);
		}
	}
}


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

int hash_chunk(int fd, loff_t chunk_off, char *filename)
{
	static char filebuf[CHUNK_SIZE];
	static uint64_t chunk_hashlist[NUM_HASHES];
	int i = 0;
	int rc = 0;
	int idx;
	struct chunk_hash *chunk = &hashes[stor_index++];
	clock_t begin, end;
	ssize_t count;

	RabinPoly *rp = rp_new(512,BUFSIZE,BUFSIZE,BUFSIZE,BUFSIZE, 0x3f63dfbf84af3b);
	begin = clock();
	count = read(fd, filebuf, CHUNK_SIZE);
	end = clock();

	if (count != CHUNK_SIZE) {
		if (count)
			printf("Short chunk - skipping: %ld\n", count);
		stor_index--;
		return 1;
	}

	read_elapsed += end - begin;

	rp_from_buffer(rp, filebuf, BUFSIZE);
	strcpy(chunk->filename, filename);

	begin = clock();
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
	end = clock();

	hash_elapsed += end - begin;

#if 0
	// since we use M_OFFSET we can't be sure we have the largest
	// hashes
	assert(chunk->unit_hashes[0] > chunk->unit_hashes[1] &&
	       chunk->unit_hashes[1] > chunk->unit_hashes[2] &&
	       chunk->unit_hashes[2] > chunk->unit_hashes[3]);
#endif

	//Prep for next iteration
	rp_free(rp);
	memset(chunk_hashlist, sizeof(uint64_t)*NUM_HASHES, 0);
	memset(filebuf, BUFSIZE, 0);

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
	loff_t chunk_off = 0;
	int ret = 0;
	double elapsed, hash_time, read_time;

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

	//prep measurement
	hash_elapsed = read_elapsed = 0;

	overall_begin = clock();
	while (!ret) {
		ret = hash_chunk(fd, chunk_off, filename);
		if (!ret)
			chunk_off += CHUNK_SIZE;
	}
	overall_end = clock();
	elapsed = (double)(overall_end - overall_begin) / CLOCKS_PER_SEC;
	close(fd);

	printf("Hashed %lu mb in %f seconds(throughput: %f mb/s). Hash time: %f read time: %f\n",
	       chunk_off / 1024 / 1024,
	       elapsed, (chunk_off / elapsed)/1024/1024,
	       (double)hash_elapsed/CLOCKS_PER_SEC, (double)read_elapsed/CLOCKS_PER_SEC);

	return 0;
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
	hashes = calloc(5000, sizeof(struct chunk_hash));
	if (hashes == NULL) {
		printf("Error allocating memory");
		exit(1);
	}

	ret = walk_dir(argv[1]);
	if (ret < 0) {
		printf("Error hashing files in dir\n");
		exit(1);
	}
#if 0
	for (int i = 0; i < stor_index; i++) {
		struct chunk_hash *chunk = &hashes[i];
		printf("CHUNK[%d/%s]: id1: %lu id2: %lu id3: %lu  id4: %lu\n",
		       i, chunk->filename, chunk->unit_hashes[0], chunk->unit_hashes[1],
		       chunk->unit_hashes[2], chunk->unit_hashes[3]);
	}
#endif

}

/*
 * Copyright (C) 2014 Steve Traugott (stevegt@t7a.org)
 * Copyright (C) 2013 Pavan Kumar Alampalli (pavankumar@cmu.edu)
 * Copyright (C) 2004 Hyang-Ah Kim (hakim@cs.cmu.edu)
 * Copyright (C) 1999 David Mazieres (dm@uun.org)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

/*
 * To use this library:
 *
 * XXX
 *
 *      rp_init() to get started
 *      rp_in() in a loop to pass input chunks into rabin algorithm
 *      rp_out() in a loop to get output blocks from rabin algorithm
 *      rp_reset() to start a new input stream
 *      rp_free() to free memory when done
 *
 * We maintain state by passing a RabinPoly to each function.
 *
 */

#include "rabinpoly.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

static inline void rp_find_block_end(RabinPoly *rp);

/*
 * Routines for calculating the most significant bit of an integer.
 */

// /usr/src/linux-headers-3.8.0-32/include/asm-generic/bitops/fls.h
static inline u_int fls32(u_int32_t x)
{
	int r = 32;

	if (!x)
		return 0;
	if (!(x & 0xffff0000u)) {
		x <<= 16;
		r -= 16;
	}
	if (!(x & 0xff000000u)) {
		x <<= 8;
		r -= 8;
	}
	if (!(x & 0xf0000000u)) {
		x <<= 4;
		r -= 4;
	}
	if (!(x & 0xc0000000u)) {
		x <<= 2;
		r -= 2;
	}
	if (!(x & 0x80000000u)) {
		x <<= 1;
		r -= 1;
	}
	return r;
}

//static inline u_int fls64 (u_int64_t) __attribute__ ((const));
static inline char fls64 (u_int64_t v)
{
  u_int32_t h;
  if ((h = v >> 32))
    return 32 + fls32 (h);
  else
    return fls32 ((u_int32_t) v);
}

#define fls(v) (sizeof (v) > 4 ? fls64 (v) : fls32 (v))

#define INT64(n) n##LL
#define MSB64 INT64(0x8000000000000000)

#define DEFAULT_WINDOW_SIZE 32

/* Fingerprint value take from LBFS fingerprint.h. For detail on this,
 * refer to the original rabin fingerprint paper.
 */
#define FINGERPRINT_PT 0xbfe6b8a5bf378d83LL

static u_int64_t polymod (u_int64_t nh, u_int64_t nl, u_int64_t d);
static void polymult (u_int64_t *php, u_int64_t *plp, u_int64_t x, u_int64_t y);
static u_int64_t polymmult (u_int64_t x, u_int64_t y, u_int64_t d);

static void calcT(RabinPoly *rp);
static u_int64_t slide8(RabinPoly *rp, unsigned char m);
static u_int64_t append8(RabinPoly *rp, u_int64_t p, unsigned char m);

/*
     functions to calculate the rabin hash
*/

static u_int64_t polymod (u_int64_t nh, u_int64_t nl, u_int64_t d) {
	int i;
	int k = fls64 (d) - 1;
	d <<= 63 - k;

	if (nh) {
		if (nh & MSB64)
			nh ^= d;  // XXX unreachable? (on 32 bit platform?)
		for (i = 62; i >= 0; i--)
			if (nh & ((u_int64_t) 1) << i) {
				nh ^= d >> (63 - i);
				nl ^= d << (i + 1);
			}
	}
	for (i = 63; i >= k; i--)
	{
		if (nl & INT64 (1) << i)
			nl ^= d >> (63 - i);
	}
	return nl;
}

static void polymult (
        u_int64_t *php, u_int64_t *plp, u_int64_t x, u_int64_t y) {
	int i;
	u_int64_t ph = 0, pl = 0;
	if (x & 1)
		pl = y;
	for (i = 1; i < 64; i++)
		if (x & (INT64 (1) << i)) {
			ph ^= y >> (64 - i);
			pl ^= y << i;
		}
	if (php)
		*php = ph;
	if (plp)
		*plp = pl;
}

static u_int64_t polymmult (u_int64_t x, u_int64_t y, u_int64_t d) {
	u_int64_t h, l;
	polymult (&h, &l, x, y);
	return polymod (h, l, d);
}

/*
    Initialize the T[] and U[] array for faster computation of rabin
    fingerprint.  Called only once from rp_init() during
    initialization.
 */

static void calcT(RabinPoly *rp) {
    unsigned int i;
    int xshift = fls64 (rp->poly) - 1;
    rp->shift = xshift - 8;

	u_int64_t T1 = polymod (0, INT64 (1) << xshift, rp->poly);
	for (i = 0; i < 256; i++) {
		rp->T[i] = polymmult (i, T1, rp->poly) | ((u_int64_t) i << xshift);
	}

	u_int64_t sizeshift = 1;
	for (i = 1; i < rp->window_size; i++) {
		sizeshift = append8 (rp, sizeshift, 0);
	}

	for (i = 0; i < 256; i++) {
		rp->U[i] = polymmult (i, sizeshift, rp->poly);
	}
}

/*
   Feed a new byte into the rabin sliding window and update the rabin
   fingerprint.
 */

static u_int64_t slide8(RabinPoly *rp, unsigned char m) {
    rp->circbuf_pos++;
	if (rp->circbuf_pos >= rp->window_size) {
		rp->circbuf_pos = 0;
	}
	unsigned char om = rp->circbuf[rp->circbuf_pos];
	rp->circbuf[rp->circbuf_pos] = m;
	return rp->fingerprint = append8 (rp, rp->fingerprint ^ rp->U[om], m);
}

static u_int64_t append8(RabinPoly *rp, u_int64_t p, unsigned char m) {
	return ((p << 8) | m) ^ rp->T[p >> rp->shift];
}


/*

    rp_init() -- Initialize the RabinPoly structure

    Call this first to create a state container to be passed to the
    other library functions.  You'll later need to free all of the
    memory associated with this container by passing it to
    rp_free().

    Args:
    -----

    window_size

        Rabin fingerprint window size in bytes.  Suitable values range
        from 32 to 128.

    avg_block_size

        Average block size in bytes

    min_block_size

        Minimum block size in bytes

    max_block_size

        Maximum block size in bytes


    Return values:
    --------------

    rp
        Pointer to the RabinPoly structure we've allocated

    NULL
        Either malloc or arg error XXX need better error codes

*/

RabinPoly* rp_new(unsigned int window_size, size_t avg_block_size,
		  size_t min_block_size, size_t max_block_size,
		  size_t inbuf_size, u_int64_t poly) {
	RabinPoly *rp;

	if (!min_block_size || !avg_block_size || !max_block_size ||
		(min_block_size > avg_block_size) ||
		(max_block_size < avg_block_size) ||
		(inbuf_size < max_block_size*2) ||
		(window_size < DEFAULT_WINDOW_SIZE)) {
		return NULL;
	}

	rp = (RabinPoly *)malloc(sizeof(RabinPoly));
	if (!rp) {
		return NULL;
	}

	rp->poly = poly;
	rp->window_size = window_size;
	rp->inbuf_size = inbuf_size;
	rp->avg_block_size = avg_block_size;
	rp->min_block_size = min_block_size;
	rp->max_block_size = max_block_size;
	rp->fingerprint_mask = (1 << (fls32(rp->avg_block_size)-1))-1;

	rp->circbuf = (unsigned char *)malloc(rp->window_size*sizeof(unsigned char));
	if (!rp->circbuf){
        free(rp);
		return NULL;
	}

	rp->inbuf = (unsigned char *)malloc(rp->inbuf_size*sizeof(unsigned char));
	if (!rp->inbuf){
        free(rp->circbuf);
        free(rp);
		return NULL;
	}

    rp_from_stream(rp, NULL);
    rp->func_stream_read = rp_stream_read;

    calcT(rp);

    return rp;
}

void rp_free(RabinPoly *rp)
{
	if (!rp) {
		return;
	}
	free(rp->inbuf);
	free(rp->circbuf);
	free(rp);
	rp = NULL;
}

void rp_from_buffer(RabinPoly *rp, unsigned char *src, size_t size) {
	rp_from_stream(rp, NULL);
	assert(size <= rp->inbuf_size);
	memcpy(rp->inbuf, src, size);
	rp->inbuf_data_size = size;
    rp->func_stream_read = NULL;
    rp->buffer_only = 1;
}

void rp_from_file(RabinPoly *rp, const char *path) {
	FILE *stream = fopen(path, "rb");
	if (!stream) {
		rp->error = errno;
	}
	rp_from_stream(rp, stream);
}

void rp_from_stream(RabinPoly *rp, FILE *stream) {
    rp->stream = stream;
    rp->error = 0;
    rp->buffer_only = 0;
    rp->inbuf_data_size = 0;
	rp->block_size = 0;
	rp->block_streampos = 0;
    rp->block_addr = rp->inbuf;
	rp->fingerprint = 0;
	rp->circbuf_pos = -1;
	bzero ((char*) rp->circbuf, rp->window_size*sizeof (unsigned char));
}

size_t rp_stream_read(RabinPoly *rp, unsigned char *dst, size_t size) {
    size_t count = fread(dst, 1, size, rp->stream);
	rp->error = 0;
    if (count == 0) {
        if (ferror(rp->stream)) {
            rp->error = errno;
        } else if (feof(rp->stream)) {
            rp->error = EOF;
        }
    }
    return count;
}

#define CUR_ADDR rp->block_addr+rp->block_size
#define INBUF_END rp->inbuf+rp->inbuf_size

int rp_block_next(RabinPoly *rp) {

    rp->block_streampos += rp->block_size;
    rp->block_addr += rp->block_size;
	rp->block_size = 0;

    /*
     * Skip early part of each block -- there appears to be no reason
     * to checksum the first min_block_size-N bytes, because the
     * effect of those early bytes gets flushed out pretty quickly.
     * Setting N to 256 seems to work; not sure if that's the "right"
     * number, but we'll use that for now.  This one optimization
     * alone provides a 30% speedup in benchmark.py though, with no
     * detected change in block boundary locations or fingerprints in
     * any of the existing tests.  - stevegt
     *
     * @moinakg found similar results, and also seems to think 256 is
     * right: https://moinakg.wordpress.com/tag/rolling-hash/
     *
    */
    size_t skip = rp->min_block_size - 256;
    size_t data_remaining = rp->inbuf_data_size - (rp->block_addr - rp->inbuf);
    if ((data_remaining > rp->min_block_size+1) &&
            (rp->min_block_size > 512)) {
        rp->block_size += skip;
    }

    for(;;) {

        if (CUR_ADDR == INBUF_END) {
            /* end of input buffer: there's a partial block at the end
             * of the buffer; move it to the beginning of the buffer
             * so we can append more from input stream
             */
            memmove(rp->inbuf, rp->block_addr, rp->block_size);
            rp->block_addr = rp->inbuf;
            rp->inbuf_data_size = rp->block_size;
        }

        if (CUR_ADDR == rp->inbuf + rp->inbuf_data_size) {
            /* no more valid data in input buffer */
			int count = 0;
			if (!rp->error) {
				if (rp->buffer_only) {
					/* don't refill buffer */
					rp->error = EOF;
				} else {
					/* use func_stream_read to refill buffer */
					int size = rp->inbuf_size - rp->inbuf_data_size;
					assert(size > 0);
					count = rp->func_stream_read(rp,
							rp->inbuf + rp->inbuf_data_size, size);
					if (!count) {
						assert(rp->error);
					}
					rp->inbuf_data_size += count;
				}
			}
			if (rp->error && (count == 0)) {
				/* we're either carrying an error from earlier, or the
				 * func_stream_read above just threw one
				 */
				if (rp->block_size == 0) {
					/* we're done. caller shouldn't call us again */
					return rp->error;
				} else {
					/* give final block to caller; caller should call
					 * us again to get e.g. eof error
					 */
					return 0;
				}
			}
        }

        /* feed the next byte into rabinpoly algo */
        slide8(rp, rp->block_addr[rp->block_size]);
        rp->block_size++;

        /*
         *
         * We compare the low-order fingerprint bits (LOFB) to
         * something other than zero in order to avoid generating
         * short blocks when scanning long strings of zeroes.
         * Mechiel Lukkien (mjl), while working on the Plan9 gsoc,
         * seemed to think that avg_block_size - 1 was a good value.
         *
         * http://gsoc.cat-v.org/people/mjl/blog/2007/08/06/1_Rabin_fingerprints/
         *
         * ...and since we're already using avg_block_size - 1 to set
         * the fingerprint mask itself, then simply comparing LOFB to
         * the mask itself will do the right thing.
         *
         */
        if ((rp->block_size == rp->max_block_size) ||
            ((rp->block_size >= rp->min_block_size) &&
            ((rp->fingerprint & rp->fingerprint_mask) == rp->fingerprint_mask))) {
            /* full block or fingerprint boundary */
            return 0;
        }
    }
}

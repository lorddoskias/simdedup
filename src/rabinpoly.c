// $Id$

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
 *      rabin_init() to get started
 *      rabin_segment_next() in a loop
 *      rabin_reset() to reset rabinpoly_t and start a new stream
 *      rabin_free() to free memory when done
 *
 * We maintain state by passing a rabinpoly_t to each function.
 *
 */

#include <stdlib.h>

#include "rabinpoly.h"
#include "msb.h"
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

static void calcT(rabinpoly_t *rp);
static u_int64_t slide8(rabinpoly_t *rp, u_char m);
static u_int64_t append8(rabinpoly_t *rp, u_int64_t p, u_char m);

/**
 * functions to calculate the rabin hash
 */
static u_int64_t
polymod (u_int64_t nh, u_int64_t nl, u_int64_t d)
{
	int i;
	int k = fls64 (d) - 1;
	d <<= 63 - k;

	if (nh) {
		if (nh & MSB64)
			nh ^= d;
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

static void
polymult (u_int64_t *php, u_int64_t *plp, u_int64_t x, u_int64_t y)
{
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

static u_int64_t
polymmult (u_int64_t x, u_int64_t y, u_int64_t d)
{
	u_int64_t h, l;
	polymult (&h, &l, x, y);
	return polymod (h, l, d);
}

/**
 * Initialize the T[] and U[] array for faster computation of rabin fingerprint
 * Called only once from rabin_init() during initialization
 */
static void calcT(rabinpoly_t *rp)
{
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

/**
 * Feed a new byte into the rabin sliding window and update 
 * the rabin fingerprint
 */
static u_int64_t slide8(rabinpoly_t *rp, u_char m) 
{
	rp->bufpos++;

	if (rp->bufpos >= rp->window_size) {
		rp->bufpos = 0;
	}
	u_char om = rp->buf[rp->bufpos];
	rp->buf[rp->bufpos] = m;
	return rp->fingerprint = append8 (rp, rp->fingerprint ^ rp->U[om], m);
}

static u_int64_t append8(rabinpoly_t *rp, u_int64_t p, u_char m) 
{ 	
	return ((p << 8) | m) ^ rp->T[p >> rp->shift]; 
}


/**
 * Interface functions exposed by the library
 */

/**
 * rabin_init()
 *
 * Initialize the rabinpoly_t structure
 *
 * Call this first to create a state container to be passed to the
 * other library functions.  You'll later need to free all of the
 * memory associated with this container by passing it to
 * rabin_free(). 
 * 
 * Args:
 *
 * window_size
 *              [input] Rabin fingerprint window size in bytes.  
 *                      Suitable values range from 32 to 128.
 * avg_segment_size 
 *              [input] Average segment size in bytes
 *
 * min_segment_size 
 *              [input] Minimum segment size in bytes
 *
 * max_segment_size 
 *              [input] Maximum segment size in bytes
 *
 * Return values:
 *
 * rp 
 *              Pointer to the rabin_poly_t structure we've allocated
 *
 * NULL 
 *              Either malloc or arg error XXX need better error codes
 */
rabinpoly_t *rabin_init(unsigned int window_size,
						unsigned long avg_segment_size, 
						unsigned long min_segment_size,
						unsigned long max_segment_size)
{
	rabinpoly_t *rp;

	if (!min_segment_size || !avg_segment_size || !max_segment_size ||
		(min_segment_size > avg_segment_size) ||
		(max_segment_size < avg_segment_size) ||
		(window_size < DEFAULT_WINDOW_SIZE)) {
		return NULL;
	}

	rp = (rabinpoly_t *)malloc(sizeof(rabinpoly_t));
	if (!rp) {
		return NULL;
	}

	rp->poly = FINGERPRINT_PT;
	rp->window_size = window_size;;
	rp->avg_segment_size = avg_segment_size;
	rp->min_segment_size = min_segment_size;
	rp->max_segment_size = max_segment_size;
	rp->fingerprint_mask = (1 << (fls32(rp->avg_segment_size)-1))-1;

	rp->fingerprint = 0;
	rp->bufpos = -1;
	rp->cur_seg_size = 0;

	calcT(rp);

	rp->buf = (u_char *)malloc(rp->window_size*sizeof(u_char));
	if (!rp->buf){
		return NULL;
	}
	bzero ((char*) rp->buf, rp->window_size*sizeof (u_char));
	return rp;
}

/**
 * rabin_segment_next()
 *
 * Search buffer for next segment boundary
 *
 * Call this multiple times while looping through an input stream.  On
 * each call, we return a boolean indicating whether the input buffer
 * contains a segment boundary.  If we found a segment boundary, then
 * we return a length value (len) showing where it is.  If we didn't find
 * a boundary, then the length value shows where we stopped
 * processing -- this might be before the end of the input buffer if
 * we reached max_segment_size.
 *
 * Because we might stop at a segment boundary in the middle of the
 * buffer, you will want to call this function in a loop, providing
 * the same buffer content, but with a new starting pointer value each
 * time (buf), until buf + len == end of buffer.
 *
 * We will never return a segment length longer than max_segment_size,
 * or shorter than min_segment_size.  We will attempt to return
 * segements with an average length of avg_segment_size.
 * 
 * Args:
 *
 * XXX
 */
unsigned long rabin_segment_next(rabinpoly_t *rp, 
						const char *buf, 
						unsigned long bytes,
						int *is_new_segment)
{
	unsigned long i;

	if (!rp || !buf || !is_new_segment) {
		return -1;
	}

    /* 
     * Skip early part of each block -- there appears to be no reason
     * to checksum the first min_segment_size-N bytes, because the
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

	unsigned long skip = 0;
	if (*is_new_segment && (bytes > rp->min_segment_size) && (rp->min_segment_size > 512)) {
		skip = rp->min_segment_size - 256;
		rp->cur_seg_size += skip;
	}

	*is_new_segment = 0;

    /* We compare the low-order fingerprint bits (LOFB) to something
     * other than zero in order to avoid generating short segments
     * when scanning long strings of zeroes.  Mechiel Lukkien (mjl),
     * while working on the Plan9 gsoc, seemed to think that
     * avg_segment_size - 1 was a good value:
     *
     * http://gsoc.cat-v.org/people/mjl/blog/2007/08/06/1_Rabin_fingerprints/ 
     *
     * ...and since we're already using avg_segment_size - 1 to set
     * the fingerprint mask itself, then simply comparing LOFB to the
     * mask itself will do the right thing.  And because we're only
     * interested in those cases where LOFB is all ones, we can simply
     * xor LOFB with the mask.
     *
     * */

	for (i = skip; i < bytes; i++) {
		slide8(rp, buf[i]);
		rp->cur_seg_size++;

		if (rp->cur_seg_size < rp->min_segment_size) {
			continue;
		}

		if( !((rp->fingerprint & rp->fingerprint_mask) ^ rp->fingerprint_mask) 
				|| (rp->cur_seg_size == rp->max_segment_size)) {
			*is_new_segment = 1;
			rp->cur_seg_size = 0;
            // printf("fingerprint %x\n", rp->fingerprint);
			return i+1;
		}
	}

	return i;
}

void rabin_reset(rabinpoly_t *rp) { 
	rp->fingerprint = 0; 
	rp->bufpos = -1;
	rp->cur_seg_size = 0;
	bzero ((char*) rp->buf, rp->window_size*sizeof (u_char));
}

void rabin_free(rabinpoly_t **p_rp)
{
	if (!p_rp || !*p_rp) {
		return;
	}

	free((*p_rp)->buf);
	free(*p_rp);
	*p_rp = NULL;
}


int rabin_callback(
        rabinpoly_t *rp, const char *buf, unsigned long size, int is_eof) {
    printf("hello from the callback! %d\n", size);
    return 0;
}

unsigned long rabin_write(
        rabinpoly_t *rp, const char *buf, unsigned long size, int is_eof, 
        int (*callback)(rabinpoly_t *, const char *, unsigned long, int)) {

	unsigned long written = 0;
    unsigned long hashed = 0;
    const char *hash_buf;
    unsigned long hash_size = 0;
    int is_new_segment = 0;

	if (!rp || !buf) {
		return -1;
	}

    hash_buf = buf;

	for (;;) {
        // printf("%lx %ld %d: ", hash_buf, hash_size, is_new_segment);
        hashed = rabin_segment_next(rp, hash_buf, hash_size, &is_new_segment);
        written += hashed;
        hash_size = size - written;
        // printf("%ld %ld %d %d\n", hashed, written, is_new_segment, is_eof);

        if (is_new_segment || ((written >= size) && is_eof)) {
            int rc;
            rc = callback(rp, hash_buf, hashed, (written >= size) && is_eof);
            // printf("got %d back\n", rc);
            if (rc) {
                written -= hashed;
                return written;
            }
        }
        hash_buf += hashed;

        if (written >= size) {
            break;
        }
    }

    return written;
}


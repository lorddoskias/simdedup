/*
 * Copyright (C) 2014 Steve Traugott (stevegt@t7a.org)
 * Copyright (C) 2013 Pavan Kumar Alampalli (pavankumar@cmu.edu)
 * Copyright (C) 2004 Hyang-Ah Kim (hakim@cs.cmu.edu)
 * Copyright (C) 2000 David Mazieres (dm@uun.org)
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

#ifndef _RABINPOLY_H_
#define _RABINPOLY_H_

#include <stdio.h>
#include <sys/types.h>

typedef struct RabinPoly {
	//Private config values
	u_int64_t poly;		    // Actual polynomial
	unsigned int window_size;   // in bytes
	size_t avg_block_size;	    // in bytes
	size_t min_block_size;	    // in bytes
	size_t max_block_size;	    // in bytes

	//PRIVATE
	unsigned char *inbuf;	    // input buffer
	size_t inbuf_size;	    // size of input buffer
	size_t inbuf_data_size;     // size of valid data in input buffer

	u_int64_t fingerprint_mask; // to check if we are at block boundary

	unsigned char *circbuf;	    // circular buffer of size 'window_size'
	unsigned int circbuf_pos;   // current position in circular buffer
	FILE *stream;		    // input stream
	int error;		    // input stream errno
	int buffer_only;	    // if set, read loaded buffer only; ignore stream
	int shift;
	u_int64_t T[256];	    // Lookup table for mod
	u_int64_t U[256];	    // Lookup table for subtraction
	size_t (*func_stream_read)(struct RabinPoly*, unsigned char *dst, size_t size);

	//PUB
	u_int64_t fingerprint;	    // current rabin fingerprint
	size_t block_streampos;	    // block start position in input stream
	unsigned char * block_addr; // starting address of current block
	size_t block_size;	    // size of the current block
} RabinPoly;

extern RabinPoly *rp_new(unsigned int window_size, size_t avg_block_size,
			 size_t min_block_size, size_t max_block_size,
			 size_t inbuf_size, u_int64_t poly);
extern void rp_from_buffer(RabinPoly *rp, unsigned char *src, size_t size);
extern void rp_from_file(RabinPoly *rp, const char *path);
extern void rp_from_stream(RabinPoly *rp, FILE *);
extern int rp_block_next(RabinPoly *rp);
extern void rp_free(RabinPoly *rp);

#endif /* !_RABINPOLY_H_ */


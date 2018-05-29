#ifndef _LCOMPRESSION_H
#define _LCOMPRESSION_H
#endif

/*
	Compression page pool
*/
#include "../utils/compression/cmp_pool.h" /* TODO more generic path */

/*
	LZ4
*/
#if defined(HAVE_KERNEL_LZ4_COMPRESS) || defined(HAVE_KERNEL_LZ4_COMPRESS_FAST)
#include <linux/lz4.h>
#else
#include "../utils/compression/lz4/lz4.h" /* TODO more generic path */
#endif

/* Ask whether a de-/compressor can handle page arrays or contiguous buffers */
#define CBUF_MASK 0x00F0
#define PGA_MASK 0xF000
#define CAN_CBUF(x) (((x) & CBUF_MASK) != 0)
#define CAN_PGA(x) (((x) & PGA_MASK) != 0)

enum l_compress {
	/* category of general compression settings - bit mask & 0x000F */
	L_COMPRESS_OFF 			= 0x0000,
	L_COMPRESS_ON			= 0x0001,
	L_COMPRESS_INHERIT		= 0x0002,		/* maybe useful for external compression? */

	/* category of algorithms, that can handle contiguous buffers - bit mask & 0x00F0
		keep enough bits for more algos 0x0FF0*/
	L_COMPRESS_LZ4 			= 0x0010,
	L_COMPRESS_LZ4_FAST 	= 0x0020,
	L_COMPRESS_LZ4_HC 		= 0x0030,
	L_COMPRESS_ZSTD 		= 0x0040,
	L_COMPRESS_GZIP 		= 0x0050,

	/* category of algorithms, that can handle page arrays - bit mask & 0xF000*/
	L_COMPRESS_BEWALGO 		= 0x1000
};

/*
	Commpression header written first within every chunk,
	whenever compression is enabled and chunk is compressible
*/
struct chdr {
	__u32 algo; 		/* compression algorithm */
	__u32 psize;		/* size of compressed data + header size */
	__u32 lsize;		/* size of uncompressed data (need it?) */
	__u32 cksum;		/* cksum (need it?) */
};

/*
 * GPL HEADER START
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this program; If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html
 *
 * GPL HEADER END
 */
/*
 * Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 * Use is subject to license terms.
 *
 * Copyright (c) 2011, 2015, Intel Corporation.
 */
/*
 * This file is part of Lustre, http://www.lustre.org/
 * Lustre is a trademark of Sun Microsystems, Inc.
 */

#include <linux/list.h> /* LIST_HEAD; .. */
#include <obd.h>

#ifndef MAX
#define MAX(a,b) (((a)<(b))?(b):(a))
#endif

struct page_bundle
{
        struct page *page;
        struct list_head list;
};

/**
 * Initializes the pool
 *
 * This function is used to initialize the page pool and thus obviously needs
 * to be called before every other cmp_pool function. It will try to allocate
 * n_bundles many bundles of consecutive pages close to the supplied
 * buffer_size. As the allocation is done page wise and contigious pages are
 * only available in sizes of 2^n we take the buffer_size argument and round
 * it up to the next possible size. If there was not enough space to allocate
 * all the pages... TODO implement correct behaviour
 *
 * \param[in]   n_bundles       The number of bundles to be allocated
 * \param[in]   buffer_size     The size of the buffers in the pool
 * \retval      0               success
 * \retval      -ENOMEM         wasn't able to allocate everything
 */
int cmp_pool_init(unsigned int n_bundles, unsigned int buffer_size);

/**
 * Tries to free all the memory previously used by the pool.
 *
 * Please return all the used buffers and arrays before calling this.
 *
 * \retval      0       someone(tm) was too lazy for error handling
 */
int cmp_pool_free(void);

/**
 * Returns the buf_size.
 *
 * Returns the buf_size because it may be slightly different from the
 * buffer_size you specified in cmp_pool_init().
 *
 * \retval      buf_size        unsigned integer between 4096 and 4194304
 */
unsigned int cmp_pool_get_buf_size(void);

/**
 * Used to request a buffer from the pool.
 *
 * This function returns a void buffer between 4-4096KiB in size. The buffer
 * size is fixed and needs to be set during initialization. The size argument
 * is used for verification only. If a buffer smaller than buf_size is
 * requested a warning will be issued and a buffer of size buf_size will be
 * returned instead. If the requested buffer is larger than buf_size a warning
 * will be issued and NULL will be returned.
 *
 * \param[in]   size    size of the buffer in bytes
 * \retval      void*   address of the requested buffer
 * \retval      NULL    invalid arg or out of memory
 */
void* cmp_pool_get_page_buffer(unsigned int size);

/**
 * Used to request a number of buffers at the same time.
 *
 * This function takes a void* array and fills it with count many buffers of
 * size size, similar to the ones being returned by cmp_pool_get_page_buffer().
 * You are responsible for the memory required by the array.
 *
 * \param[in]	count		the number of buffers
 * \param[in]	size		size of the individual buffers in bytes
 * \param[in]	destination	void** with enough space for count many void*
 * \retval	0		success
 * \retval	-E*		error
 */
int cmp_pool_get_many_buffers(unsigned int count, unsigned int size, void** destination);

/**
 * Request a brw_page* array from the pool.
 *
 * The size is fixed and space wise the same as the buffer size, e.g.
 * buf_size / 4 pages.The page array will be NULL terminated too keep track of
 * the length. The count argument is used for verification only. If count is
 * smaller than 2^page_order a warning will be issued and an array with
 * 2^page_order many pages will be returned instead. If count is bigger than
 * 2^page_order NULL will be returned.
 *
 * \retval NULL         count to big
 * \retval brw_page**   the array
 */
struct brw_page** cmp_pool_get_page_array(unsigned int count);

/**
 * Returns a buffer to the pool.
 *
 * This is what you should do with them after you have used them, otherwise
 * there wont be any space left pretty soon.
 *
 * \param[in]   buffer  The address of the buffer
 * \retval      0       success
 * \retval      -E*     error
 */
int cmp_pool_return_page_buffer(void* buffer);

/**
 * Returns a brw_page* array to the pool.
 *
 * This is what you should do with them after you have used them, otherwise
 * there wont be any space left pretty soon. Pls only try to return brw_page*
 * arrays that you got from cmp_pool_get_page_array(), otherwise this will
 * go horribly wrong.
 *
 * \param[in]   pages   The address of the page_array
 * \retval      0       success
 * \retval      -E*     error
 */
int cmp_pool_return_page_array(struct brw_page**);

MODULE_LICENSE("GPL");
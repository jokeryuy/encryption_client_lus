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

//#include <obd_support.h> /* OBD_ALLOC; OBD_FREE */
#include <linux/spinlock.h> /* spinlock_t; .. */
#include <linux/delay.h> /* msleep */
#include <linux/highmem.h> /* kmap */
#include <linux/rhashtable.h>
#include <linux/jhash.h>
#include <linux/log2.h>
#include "cmp_pool.h"

#define MAX_MEM_WAIT_TIME 2048

DEFINE_SPINLOCK(cmp_lock);
LIST_HEAD(cmp_list);

DEFINE_SPINLOCK(empty_containers_lock);
LIST_HEAD(empty_containers_list);

unsigned int cmp_list_total_length = 0;
unsigned int cmp_list_current_length = 0;

unsigned int buf_size = 128 * 1024;
unsigned int page_order = 5;

struct many_buf_cont {
	struct page* key;
	struct rhash_head node;
	struct page** page_array;
};

struct rhashtable track_map;

struct rhashtable_params track_map_params = {
	.head_offset = offsetof(struct many_buf_cont, node),
	.key_offset = offsetof(struct many_buf_cont, key),
	.key_len = sizeof(struct page*),
};

unsigned int cmp_pool_get_buf_size(void)
{
	return buf_size;
}
EXPORT_SYMBOL(cmp_pool_get_buf_size);

int cmp_pool_alloc_page_bundles(unsigned int count)
{
	int res;
	unsigned int i;
	struct page_bundle* tmp = NULL;

	CERROR("begin alloc\n");
	msleep(500);

	for (i = 0; i < count; i++)
	{
		OBD_ALLOC_PTR(tmp);

		if (tmp == NULL)
		{
			res = -ENOMEM; /* init phase does not have enough memory - fatal error */
			goto _finish_alloc;
		}

		tmp->page = alloc_pages(GFP_NOFS | __GFP_ZERO, page_order);

		if (tmp->page == NULL)
		{
			OBD_FREE_PTR(tmp);
			res= -ENOMEM; /* init phase does not have enough memory - fatal error */
			goto _finish_alloc;
		}

	INIT_LIST_HEAD(&tmp->list);
	spin_lock(&cmp_lock);
	list_add_tail(&tmp->list, &cmp_list);
	spin_unlock(&cmp_lock);

	}
	/* end of loop -> (i == count) */
	cmp_list_total_length += i;
	cmp_list_current_length += i;
	res = 0;

	CERROR("successfully finished alloc\n");
	msleep(500);

_finish_alloc:
	return res;
}

struct page** _downsize_page_bundle(struct page* old_page, unsigned int count, unsigned int new_page_count)
{
	unsigned int i;
	struct page** res;

	OBD_ALLOC(res, sizeof(*res) * count);
	if (res == NULL)
	{
		CERROR("_downsize failed\n");
		msleep(1000);
		goto _finish__downsize;
	}

	for (i = 0; i < count; i++)
		res[i] = old_page + i * new_page_count;

_finish__downsize:
	return res;
}

static inline int wait_for_mem(unsigned int count)
{
	unsigned int i,j;

	j = 0;
	i = 1;

	while (READ_ONCE(cmp_list_current_length) < count && j < MAX_MEM_WAIT_TIME)
	{
		spin_unlock(&cmp_lock);
		msleep(i);
		j += i;
		i = (i == 128) ? 128 : i << 1;
		spin_lock(&cmp_lock);
	}

	return MAX_MEM_WAIT_TIME - j;
}

int cmp_pool_get_many_buffers(unsigned int count, unsigned int size, void** destination)
{
	struct page_bundle* tmp;
	struct list_head** empty_containers_array;
	struct many_buf_cont* container;
	unsigned int i;
	int res;

	if (unlikely(size != buf_size))
	{
		if (size < buf_size)
		{
			CERROR("Requested bufsize %d too small, returned bufsize %d\n", size, buf_size);
		}
		else
		{
			CERROR("Requested bufsize %d too large, max size %d!\n", size, buf_size);
			return -EDOM;
		}
	}

	OBD_ALLOC(empty_containers_array, sizeof(*empty_containers_array) * count);
	if (empty_containers_array == NULL)
	{
		res = -ENOMEM;
		goto _many_buf_exit;
	}

	OBD_ALLOC_PTR(container);
	if (container == NULL)
	{
		res = -ENOMEM;
		goto _many_buf_exit;
	}

	OBD_ALLOC(container->page_array, sizeof(*container->page_array) * (count + 1));
	if (container->page_array == NULL)
	{
		res = -ENOMEM;
		OBD_FREE_PTR(container);
		goto _many_buf_exit;
	}

	spin_lock(&cmp_lock);

	if (wait_for_mem(count) < 1)
	{
		spin_unlock(&cmp_lock);
		CERROR("time out while waiting for memory \n");
		res = -ETIME;
		OBD_FREE(container->page_array, sizeof(*container->page_array) * (count + 1));
		OBD_FREE_PTR(container);
		goto _many_buf_exit;
	}

	for (i = 0; i < count; i++)
	{
		tmp = list_first_entry(&cmp_list, struct page_bundle, list);

		list_del_init(&tmp->list);
		cmp_list_current_length--;

		container->page_array[i] = tmp->page;
		empty_containers_array[i] = &tmp->list;

		tmp->page = NULL;
	}

	spin_unlock(&cmp_lock);

	spin_lock(&empty_containers_lock);
	for (i = 0; i < count; i++)
	{
		list_add_tail((empty_containers_array[i]), &empty_containers_list);
	}
	spin_unlock(&empty_containers_lock);

	for (i = 0; i < count; i++)
	{
		destination[i] = kmap(container->page_array[i]);
	}

	container->page_array[count] = NULL;
	container->key = container->page_array[0];

	if (rhashtable_insert_fast(&track_map, &container->node, track_map_params) != 0)
	{
		/* TODO */
		OBD_FREE(container->page_array, sizeof(*container->page_array) * (count + 1));
		OBD_FREE_PTR(container);
		CERROR("failed insert into hashmap\n");
		res = -EAGAIN;
		goto _many_buf_exit;
	}

	res = 0;

_many_buf_exit:
	OBD_FREE(empty_containers_array, sizeof(*empty_containers_array) * count);
	return res;
}
EXPORT_SYMBOL(cmp_pool_get_many_buffers);

void* cmp_pool_get_page_buffer(unsigned int size)
{
	void* destination = NULL;
	/*
	OBD_ALLOC_PTR(destination);
	TODO: free this somewhere */

	cmp_pool_get_many_buffers(1, size, &destination);
	/* TODO: error handling */

	return destination;
}
EXPORT_SYMBOL(cmp_pool_get_page_buffer);

/**
 * Helper function that returns a page and its bundle to the pool
 */
int _return_page(struct page* page)
{

	struct page_bundle* tmp;

	spin_lock(&empty_containers_lock);
	tmp = list_first_entry(&empty_containers_list, struct page_bundle, list);
	list_del_init(&tmp->list);
	spin_unlock(&empty_containers_lock);

	tmp->page = page;

	spin_lock(&cmp_lock);
	list_add_tail(&tmp->list, &cmp_list);
	cmp_list_current_length++;
	spin_unlock(&cmp_lock);

	return 0;
}

int cmp_pool_return_page_buffer(void* buffer)
{
	unsigned int i;
	struct page* page;
	struct many_buf_cont* container;
	int res = 0;

	page = virt_to_page(buffer);

	container = rhashtable_lookup_fast(&track_map, &page, track_map_params);
	if (container == NULL)
	{
		CERROR("couldn't find buffer in map, are you sure its from the pool?\n");
		return -EFAULT;
	}

	if (rhashtable_remove_fast(&track_map, &container->node, track_map_params) != 0)
	{
		CERROR("failed to remove from hashtable\n");
		/* TODO: handle this */
	}

	for (i = 0; container->page_array[i] != NULL; i++)
	{
		page = container->page_array[i];
		kunmap(page);
		res += ! _return_page(page);
	}

	res -= i;

	if (res != 0)
	{
		CERROR("could not return all pages\n");
	}

	if (i == 1)
		CERROR("returned single buffer\n"); /* OBD_FREE_PTR(&buffer); TODO: brace for impact.. */

	OBD_FREE(container->page_array, sizeof(*container->page_array) * (i + 1));
	OBD_FREE_PTR(container);

	return -res;
}
EXPORT_SYMBOL(cmp_pool_return_page_buffer);

/**
 * Converts a struct page* array to a struct brw_page* array
 */
struct brw_page** pg_array_to_brw_array(struct page** pages, unsigned int length)
{
	unsigned int i;
	struct brw_page** res = NULL;
	struct brw_page* tmp = NULL;

	OBD_ALLOC(res, sizeof(*res) * (length + 1));
	if (res == NULL)
		return res; /* out of memory or worse */

	for (i = 0; i < length; i++)
	{
		OBD_ALLOC_PTR(tmp);
		if (tmp == NULL)
		{
			return NULL;
		}
		tmp->pg = pages[i];
		res[i] = tmp;
	}
	res[i] = NULL;

	return res;
}

struct brw_page** cmp_pool_get_page_array(unsigned int count)
{
	struct page** pages;
	struct page_bundle* tmp;
	struct brw_page** res = NULL;
	unsigned int pages_per_bundle = buf_size >> 12;

	unsigned int page_count = 1 << page_order;

	if (unlikely(count != page_count))
	{
		if (count < page_count)
			CERROR("Requested pga of %d pages, returned %d pages\n", count, page_count);
		else
		{
			CERROR("Requested pga of %d pages, max page count is %d\n", count, page_count);
			return NULL;
		}
	}


	spin_lock(&cmp_lock);

	if (wait_for_mem(1) < 1)
	{
		spin_unlock(&cmp_lock);
		return NULL;
	}

	tmp = list_first_entry(&cmp_list, struct page_bundle, list);
	list_del_init(&tmp->list);
	cmp_list_current_length--;
	spin_unlock(&cmp_lock);

	pages = _downsize_page_bundle(tmp->page, pages_per_bundle, 1);
	res = pg_array_to_brw_array(pages, pages_per_bundle);

	tmp->page = NULL;

	spin_lock(&empty_containers_lock);
	list_add_tail(&tmp->list, &empty_containers_list);
	spin_unlock(&empty_containers_lock);

	OBD_FREE(pages, sizeof(*pages) * pages_per_bundle);

	return res;
}
EXPORT_SYMBOL(cmp_pool_get_page_array);

int cmp_pool_return_page_array(struct brw_page** pages)
{
	unsigned int i, length;
	struct page* first_page;

	first_page = (*pages)->pg;

	for (i = 0; pages[i] != NULL; i++)
	{
		OBD_FREE_PTR(pages[i]);
	}

	/* i is now the number of pages in the array */
	length = i;

	if (length != (buf_size >> 12))
	{
		return -EFAULT; /* rekt */
	}

	return _return_page(first_page);
}
EXPORT_SYMBOL(cmp_pool_return_page_array);

int cmp_pool_init(unsigned int n_bundles, unsigned int buffer_size)
{
	int a, b;

	page_order = order_base_2(buffer_size) - 12; /* 12 because log_2(4096) = 12, TODO: use PAGE_SIZE */
	buf_size = 1 << (page_order + 12);

	if (unlikely(buf_size > buffer_size))
		CERROR("requested buffer_size %d was rounded up to %d\n", buffer_size, buf_size);

	a = cmp_pool_alloc_page_bundles(n_bundles);
	if (a == -ENOMEM)
	{
		CERROR("not enough memory for init\n");
		msleep(500);
	}

	track_map_params.max_size = n_bundles;
	track_map_params.nelem_hint = n_bundles / 4 * 3;

	b = rhashtable_init(&track_map, &track_map_params);
	if (b == -ENOMEM)
	{
		CERROR("not enough memory to init hashtable\n");
		msleep(500);
	}

	return a; /* TODO: error handling */
}
EXPORT_SYMBOL(cmp_pool_init);

int cmp_pool_free(void)
{
	struct page_bundle* current_bundle;
	struct page_bundle* next_bundle;

	spin_lock(&cmp_lock);

	/*
	while (cmp_list_total_length != cmp_list_current_length)
	{
		spin_unlock(&cmp_lock);
		msleep(100);
		spin_lock(&cmp_lock);
		CERROR("fun\n");
	}
	*/

	/*
	CERROR("list_total_length %d\n", cmp_list_total_length);
	CERROR("list_current_length %d\n", cmp_list_current_length);
	*/
	CERROR("%d buffers were not returned to the pool\n", cmp_list_total_length - cmp_list_current_length);

	list_for_each_entry_safe(current_bundle, next_bundle, &cmp_list, list)
	{
		list_del(&current_bundle->list);
		/*kmap(current_bundle->page); */
		__free_pages(current_bundle->page, page_order);
		OBD_FREE_PTR(current_bundle);
		cmp_list_total_length--;
		cmp_list_current_length--;
	}

	/*
	CERROR("list_total_length %d\n", cmp_list_total_length);
	CERROR("list_current_length %d\n", cmp_list_current_length);
	*/

	CERROR("bytes LEAKED: %d\n", (cmp_list_total_length - cmp_list_current_length) * buf_size);

	spin_unlock(&cmp_lock);

	return 0;
}
EXPORT_SYMBOL(cmp_pool_free);

MODULE_LICENSE("GPL");
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
 * Copyright (c) 2012, 2017 Intel Corporation.
 */
/*
 * lustre/mdt/mdt_io.c
 *
 * Author: Mikhail Pershin <mike.pershin@intel.com>
 */

#define DEBUG_SUBSYSTEM S_FILTER

#include <dt_object.h>
#include "mdt_internal.h"

/* functions below are stubs for now, they will be implemented with
 * grant support on MDT */
static inline void mdt_io_counter_incr(struct obd_export *exp, int opcode,
				       char *jobid, long amount)
{
	return;
}

static inline void mdt_dom_read_lock(struct mdt_object *mo)
{
	down_read(&mo->mot_dom_sem);
}

static inline void mdt_dom_read_unlock(struct mdt_object *mo)
{
	up_read(&mo->mot_dom_sem);
}

static inline void mdt_dom_write_lock(struct mdt_object *mo)
{
	down_write(&mo->mot_dom_sem);
}

static inline void mdt_dom_write_unlock(struct mdt_object *mo)
{
	up_write(&mo->mot_dom_sem);
}

static int mdt_preprw_read(const struct lu_env *env, struct obd_export *exp,
			   struct mdt_device *mdt, struct mdt_object *mo,
			   struct lu_attr *la, int niocount,
			   struct niobuf_remote *rnb, int *nr_local,
			   struct niobuf_local *lnb, char *jobid)
{
	struct dt_object *dob;
	int i, j, rc, tot_bytes = 0;

	ENTRY;

	mdt_dom_read_lock(mo);
	if (!mdt_object_exists(mo))
		GOTO(unlock, rc = -ENOENT);

	dob = mdt_obj2dt(mo);
	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < niocount; i++) {
		rc = dt_bufs_get(env, dob, rnb + i, lnb + j, 0);
		if (unlikely(rc < 0))
			GOTO(buf_put, rc);
		/* correct index for local buffers to continue with */
		j += rc;
		*nr_local += rc;
		tot_bytes += rnb[i].rnb_len;
	}

	rc = dt_attr_get(env, dob, la);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	rc = dt_read_prep(env, dob, lnb, *nr_local);
	if (unlikely(rc))
		GOTO(buf_put, rc);

	mdt_io_counter_incr(exp, LPROC_MDT_IO_READ, jobid, tot_bytes);
	RETURN(0);
buf_put:
	dt_bufs_put(env, dob, lnb, *nr_local);
unlock:
	mdt_dom_read_unlock(mo);
	return rc;
}

static int mdt_preprw_write(const struct lu_env *env, struct obd_export *exp,
			    struct mdt_device *mdt, struct mdt_object *mo,
			    struct lu_attr *la, struct obdo *oa,
			    int objcount, struct obd_ioobj *obj,
			    struct niobuf_remote *rnb, int *nr_local,
			    struct niobuf_local *lnb, char *jobid)
{
	struct dt_object *dob;
	int i, j, k, rc = 0, tot_bytes = 0;

	ENTRY;

	/* Process incoming grant info, set OBD_BRW_GRANTED flag and grant some
	 * space back if possible */
	tgt_grant_prepare_write(env, exp, oa, rnb, obj->ioo_bufcnt);

	mdt_dom_read_lock(mo);
	if (!mdt_object_exists(mo)) {
		CDEBUG(D_ERROR, "%s: BRW to missing obj "DFID"\n",
		       exp->exp_obd->obd_name, PFID(mdt_object_fid(mo)));
		GOTO(unlock, rc = -ENOENT);
	}

	dob = mdt_obj2dt(mo);
	/* parse remote buffers to local buffers and prepare the latter */
	*nr_local = 0;
	for (i = 0, j = 0; i < obj->ioo_bufcnt; i++) {
		rc = dt_bufs_get(env, dob, rnb + i, lnb + j, 1);
		if (unlikely(rc < 0))
			GOTO(err, rc);
		/* correct index for local buffers to continue with */
		for (k = 0; k < rc; k++) {
			lnb[j + k].lnb_flags = rnb[i].rnb_flags;
			if (!(rnb[i].rnb_flags & OBD_BRW_GRANTED))
				lnb[j + k].lnb_rc = -ENOSPC;
		}
		j += rc;
		*nr_local += rc;
		tot_bytes += rnb[i].rnb_len;
	}

	rc = dt_write_prep(env, dob, lnb, *nr_local);
	if (likely(rc))
		GOTO(err, rc);

	mdt_io_counter_incr(exp, LPROC_MDT_IO_WRITE, jobid, tot_bytes);
	RETURN(0);
err:
	dt_bufs_put(env, dob, lnb, *nr_local);
unlock:
	mdt_dom_read_unlock(mo);
	/* tgt_grant_prepare_write() was called, so we must commit */
	tgt_grant_commit(exp, oa->o_grant_used, rc);
	/* let's still process incoming grant information packed in the oa,
	 * but without enforcing grant since we won't proceed with the write.
	 * Just like a read request actually. */
	tgt_grant_prepare_read(env, exp, oa);
	return rc;
}

int mdt_obd_preprw(const struct lu_env *env, int cmd, struct obd_export *exp,
		   struct obdo *oa, int objcount, struct obd_ioobj *obj,
		   struct niobuf_remote *rnb, int *nr_local,
		   struct niobuf_local *lnb, struct chunk_desc *cdesc)
{
	struct tgt_session_info *tsi = tgt_ses_info(env);
	struct mdt_thread_info *info = tsi2mdt_info(tsi);
	struct lu_attr *la = &info->mti_attr.ma_attr;
	struct mdt_device *mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	struct mdt_object *mo;
	char *jobid;
	int rc = 0;

	/* The default value PTLRPC_MAX_BRW_PAGES is set in tgt_brw_write()
	 * but for MDT it is different, correct it here. */
	if (*nr_local > MD_MAX_BRW_PAGES)
		*nr_local = MD_MAX_BRW_PAGES;

	jobid = tsi->tsi_jobid;

	if (!oa || objcount != 1 || obj->ioo_bufcnt == 0) {
		CERROR("%s: bad parameters %p/%i/%i\n",
		       exp->exp_obd->obd_name, oa, objcount, obj->ioo_bufcnt);
		rc = -EPROTO;
	}

	mo = mdt_object_find(env, mdt, &tsi->tsi_fid);
	if (IS_ERR(mo))
		GOTO(out, rc = PTR_ERR(mo));

	LASSERT(info->mti_object == NULL);
	info->mti_object = mo;

	if (cmd == OBD_BRW_WRITE) {
		la_from_obdo(la, oa, OBD_MD_FLGETATTR);
		rc = mdt_preprw_write(env, exp, mdt, mo, la, oa,
				      objcount, obj, rnb, nr_local, lnb,
				      jobid);
	} else if (cmd == OBD_BRW_READ) {
		tgt_grant_prepare_read(env, exp, oa);
		rc = mdt_preprw_read(env, exp, mdt, mo, la,
				     obj->ioo_bufcnt, rnb, nr_local, lnb,
				     jobid);
		obdo_from_la(oa, la, LA_ATIME);
	} else {
		CERROR("%s: wrong cmd %d received!\n",
		       exp->exp_obd->obd_name, cmd);
		rc = -EPROTO;
	}
	if (rc) {
		lu_object_put(env, &mo->mot_obj);
		info->mti_object = NULL;
	}
out:
	RETURN(rc);
}

static int mdt_commitrw_read(const struct lu_env *env, struct mdt_device *mdt,
			     struct mdt_object *mo, int objcount, int niocount,
			     struct niobuf_local *lnb)
{
	struct dt_object *dob;
	int rc = 0;

	ENTRY;

	LASSERT(niocount > 0);

	dob = mdt_obj2dt(mo);

	dt_bufs_put(env, dob, lnb, niocount);

	mdt_dom_read_unlock(mo);
	RETURN(rc);
}

static int mdt_commitrw_write(const struct lu_env *env, struct obd_export *exp,
			      struct mdt_device *mdt, struct mdt_object *mo,
			      struct lu_attr *la, int objcount, int niocount,
			      struct niobuf_local *lnb, unsigned long granted,
			      int old_rc)
{
	struct dt_device *dt = mdt->mdt_bottom;
	struct dt_object *dob;
	struct thandle *th;
	int rc = 0;
	int retries = 0;
	int i;

	ENTRY;

	dob = mdt_obj2dt(mo);

	if (old_rc)
		GOTO(out, rc = old_rc);

	la->la_valid &= LA_ATIME | LA_MTIME | LA_CTIME;
retry:
	if (!dt_object_exists(dob))
		GOTO(out, rc = -ENOENT);

	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		GOTO(out, rc = PTR_ERR(th));

	for (i = 0; i < niocount; i++) {
		if (!(lnb[i].lnb_flags & OBD_BRW_ASYNC)) {
			th->th_sync = 1;
			break;
		}
	}

	if (OBD_FAIL_CHECK(OBD_FAIL_OST_DQACQ_NET))
		GOTO(out_stop, rc = -EINPROGRESS);

	rc = dt_declare_write_commit(env, dob, lnb, niocount, th);
	if (rc)
		GOTO(out_stop, rc);

	if (la->la_valid) {
		/* update [mac]time if needed */
		rc = dt_declare_attr_set(env, dob, la, th);
		if (rc)
			GOTO(out_stop, rc);
	}

	rc = dt_trans_start(env, dt, th);
	if (rc)
		GOTO(out_stop, rc);

	dt_write_lock(env, dob, 0);
	rc = dt_write_commit(env, dob, lnb, niocount, th);
	if (rc)
		GOTO(unlock, rc);

	if (la->la_valid) {
		rc = dt_attr_set(env, dob, la, th);
		if (rc)
			GOTO(unlock, rc);
	}
	/* get attr to return */
	rc = dt_attr_get(env, dob, la);
unlock:
	dt_write_unlock(env, dob);

out_stop:
	/* Force commit to make the just-deleted blocks
	 * reusable. LU-456 */
	if (rc == -ENOSPC)
		th->th_sync = 1;


	if (rc == 0 && granted > 0) {
		if (tgt_grant_commit_cb_add(th, exp, granted) == 0)
			granted = 0;
	}

	th->th_result = rc;
	dt_trans_stop(env, dt, th);
	if (rc == -ENOSPC && retries++ < 3) {
		CDEBUG(D_INODE, "retry after force commit, retries:%d\n",
		       retries);
		goto retry;
	}

out:
	dt_bufs_put(env, dob, lnb, niocount);
	mdt_dom_read_unlock(mo);
	if (granted > 0)
		tgt_grant_commit(exp, granted, old_rc);
	RETURN(rc);
}

void mdt_dom_obj_lvb_update(const struct lu_env *env, struct mdt_object *mo,
			    bool increase_only)
{
	struct mdt_device *mdt = mdt_dev(mo->mot_obj.lo_dev);
	struct ldlm_res_id resid;
	struct ldlm_resource *res;

	fid_build_reg_res_name(mdt_object_fid(mo), &resid);
	res = ldlm_resource_get(mdt->mdt_namespace, NULL, &resid,
				LDLM_IBITS, 1);
	if (IS_ERR(res))
		return;

	/* Update lvbo data if exists. */
	if (mdt_dom_lvb_is_valid(res))
		mdt_dom_disk_lvbo_update(env, mo, res, increase_only);
	ldlm_resource_putref(res);
}

int mdt_obd_commitrw(const struct lu_env *env, int cmd, struct obd_export *exp,
		     struct obdo *oa, int objcount, struct obd_ioobj *obj,
		     struct niobuf_remote *rnb, int npages,
		     struct niobuf_local *lnb, int old_rc)
{
	struct mdt_thread_info *info = mdt_th_info(env);
	struct mdt_device *mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	struct mdt_object *mo = info->mti_object;
	struct lu_attr *la = &info->mti_attr.ma_attr;
	__u64 valid;
	int rc = 0;

	if (npages == 0) {
		CERROR("%s: no pages to commit\n",
		       exp->exp_obd->obd_name);
		rc = -EPROTO;
	}

	LASSERT(mo);

	if (cmd == OBD_BRW_WRITE) {
		/* Don't update timestamps if this write is older than a
		 * setattr which modifies the timestamps. b=10150 */

		/* XXX when we start having persistent reservations this needs
		 * to be changed to ofd_fmd_get() to create the fmd if it
		 * doesn't already exist so we can store the reservation handle
		 * there. */
		valid = OBD_MD_FLUID | OBD_MD_FLGID;
		valid |= OBD_MD_FLATIME | OBD_MD_FLMTIME | OBD_MD_FLCTIME;

		la_from_obdo(la, oa, valid);

		rc = mdt_commitrw_write(env, exp, mdt, mo, la, objcount,
					npages, lnb, oa->o_grant_used, old_rc);
		if (rc == 0)
			obdo_from_la(oa, la, VALID_FLAGS | LA_GID | LA_UID);
		else
			obdo_from_la(oa, la, LA_GID | LA_UID);

		mdt_dom_obj_lvb_update(env, mo, false);
		/* don't report overquota flag if we failed before reaching
		 * commit */
		if (old_rc == 0 && (rc == 0 || rc == -EDQUOT)) {
			/* return the overquota flags to client */
			if (lnb[0].lnb_flags & OBD_BRW_OVER_USRQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_USRQUOTA;
				else
					oa->o_flags = OBD_FL_NO_USRQUOTA;
			}

			if (lnb[0].lnb_flags & OBD_BRW_OVER_GRPQUOTA) {
				if (oa->o_valid & OBD_MD_FLFLAGS)
					oa->o_flags |= OBD_FL_NO_GRPQUOTA;
				else
					oa->o_flags = OBD_FL_NO_GRPQUOTA;
			}

			oa->o_valid |= OBD_MD_FLFLAGS | OBD_MD_FLUSRQUOTA |
				       OBD_MD_FLGRPQUOTA;
		}
	} else if (cmd == OBD_BRW_READ) {
		/* If oa != NULL then mdt_preprw_read updated the inode
		 * atime and we should update the lvb so that other glimpses
		 * will also get the updated value. bug 5972 */
		if (oa)
			mdt_dom_obj_lvb_update(env, mo, true);
		rc = mdt_commitrw_read(env, mdt, mo, objcount, npages, lnb);
		if (old_rc)
			rc = old_rc;
	} else {
		rc = -EPROTO;
	}
	/* this put is pair to object_get in ofd_preprw_write */
	mdt_thread_info_fini(info);
	RETURN(rc);
}

int mdt_object_punch(const struct lu_env *env, struct dt_device *dt,
		     struct dt_object *dob, __u64 start, __u64 end,
		     struct lu_attr *la)
{
	struct thandle *th;
	int rc;

	ENTRY;

	/* we support truncate, not punch yet */
	LASSERT(end == OBD_OBJECT_EOF);

	if (!dt_object_exists(dob))
		RETURN(-ENOENT);

	th = dt_trans_create(env, dt);
	if (IS_ERR(th))
		RETURN(PTR_ERR(th));

	rc = dt_declare_attr_set(env, dob, la, th);
	if (rc)
		GOTO(stop, rc);

	rc = dt_declare_punch(env, dob, start, OBD_OBJECT_EOF, th);
	if (rc)
		GOTO(stop, rc);

	tgt_vbr_obj_set(env, dob);
	rc = dt_trans_start(env, dt, th);
	if (rc)
		GOTO(stop, rc);

	dt_write_lock(env, dob, 0);
	rc = dt_punch(env, dob, start, OBD_OBJECT_EOF, th);
	if (rc)
		GOTO(unlock, rc);
	rc = dt_attr_set(env, dob, la, th);
	if (rc)
		GOTO(unlock, rc);
unlock:
	dt_write_unlock(env, dob);
stop:
	th->th_result = rc;
	dt_trans_stop(env, dt, th);
	RETURN(rc);
}

int mdt_punch_hdl(struct tgt_session_info *tsi)
{
	const struct obdo *oa = &tsi->tsi_ost_body->oa;
	struct ost_body *repbody;
	struct mdt_thread_info *info;
	struct lu_attr *la;
	struct ldlm_namespace *ns = tsi->tsi_tgt->lut_obd->obd_namespace;
	struct obd_export *exp = tsi->tsi_exp;
	struct mdt_device *mdt = mdt_dev(exp->exp_obd->obd_lu_dev);
	struct mdt_object *mo;
	struct dt_object *dob;
	__u64 flags = 0;
	struct lustre_handle lh = { 0, };
	__u64 start, end;
	int rc;
	bool srvlock;

	ENTRY;

	/* check that we do support OBD_CONNECT_TRUNCLOCK. */
	CLASSERT(OST_CONNECT_SUPPORTED & OBD_CONNECT_TRUNCLOCK);

	if ((oa->o_valid & (OBD_MD_FLSIZE | OBD_MD_FLBLOCKS)) !=
	    (OBD_MD_FLSIZE | OBD_MD_FLBLOCKS))
		RETURN(err_serious(-EPROTO));

	repbody = req_capsule_server_get(tsi->tsi_pill, &RMF_OST_BODY);
	if (repbody == NULL)
		RETURN(err_serious(-ENOMEM));

	/* punch start,end are passed in o_size,o_blocks throught wire */
	start = oa->o_size;
	end = oa->o_blocks;

	if (end != OBD_OBJECT_EOF) /* Only truncate is supported */
		RETURN(-EPROTO);

	info = tsi2mdt_info(tsi);
	la = &info->mti_attr.ma_attr;
	/* standard truncate optimization: if file body is completely
	 * destroyed, don't send data back to the server. */
	if (start == 0)
		flags |= LDLM_FL_AST_DISCARD_DATA;

	repbody->oa.o_oi = oa->o_oi;
	repbody->oa.o_valid = OBD_MD_FLID;

	srvlock = (exp_connect_flags(exp) & OBD_CONNECT_SRVLOCK) &&
		  oa->o_valid & OBD_MD_FLFLAGS &&
		  oa->o_flags & OBD_FL_SRVLOCK;

	if (srvlock) {
		rc = tgt_mdt_data_lock(ns, &tsi->tsi_resid, &lh, LCK_PW,
				       &flags);
		if (rc != 0)
			GOTO(out, rc);
	}

	CDEBUG(D_INODE, "calling punch for object "DFID", valid = %#llx"
	       ", start = %lld, end = %lld\n", PFID(&tsi->tsi_fid),
	       oa->o_valid, start, end);

	mo = mdt_object_find(tsi->tsi_env, mdt, &tsi->tsi_fid);
	if (IS_ERR(mo))
		GOTO(out_unlock, rc = PTR_ERR(mo));

	if (!mdt_object_exists(mo))
		GOTO(out_put, rc = -ENOENT);

	/* Shouldn't happen on dirs */
	if (S_ISDIR(lu_object_attr(&mo->mot_obj))) {
		rc = -EPERM;
		CERROR("%s: Truncate on dir "DFID": rc = %d\n",
		       exp->exp_obd->obd_name, PFID(&tsi->tsi_fid), rc);
		GOTO(out_put, rc);
	}

	mdt_dom_write_lock(mo);
	dob = mdt_obj2dt(mo);

	la_from_obdo(la, oa, OBD_MD_FLMTIME | OBD_MD_FLATIME | OBD_MD_FLCTIME);
	la->la_size = start;
	la->la_valid |= LA_SIZE;

	rc = mdt_object_punch(tsi->tsi_env, mdt->mdt_bottom, dob,
			      start, end, la);
	mdt_dom_write_unlock(mo);
	if (rc)
		GOTO(out_put, rc);

	mdt_dom_obj_lvb_update(tsi->tsi_env, mo, false);
	mdt_io_counter_incr(tsi->tsi_exp, LPROC_MDT_IO_PUNCH,
			    tsi->tsi_jobid, 1);
	EXIT;
out_put:
	lu_object_put(tsi->tsi_env, &mo->mot_obj);
out_unlock:
	if (srvlock)
		mdt_save_lock(info, &lh, LCK_PW, rc);
out:
	mdt_thread_info_fini(info);
	return rc;
}

/**
 * MDT glimpse for Data-on-MDT
 *
 * If there is write lock on client then function issues glimpse_ast to get
 * an actual size from that client.
 *
 */
int mdt_do_glimpse(const struct lu_env *env, struct ldlm_namespace *ns,
		   struct ldlm_resource *res)
{
	union ldlm_policy_data policy;
	struct lustre_handle lockh;
	enum ldlm_mode mode;
	struct ldlm_lock *lock;
	struct ldlm_glimpse_work *gl_work;
	struct list_head gl_list;
	int rc;

	ENTRY;

	/* There can be only one write lock covering data, try to match it. */
	policy.l_inodebits.bits = MDS_INODELOCK_DOM;
	mode = ldlm_lock_match(ns, LDLM_FL_BLOCK_GRANTED | LDLM_FL_TEST_LOCK,
			       &res->lr_name, LDLM_IBITS, &policy,
			       LCK_PW, &lockh, 0);

	/* There is no PW lock on this object; finished. */
	if (mode == 0)
		RETURN(0);

	lock = ldlm_handle2lock(&lockh);
	if (lock == NULL)
		RETURN(0);

	/*
	 * This check is for lock taken in mdt_reint_unlink() that does
	 * not have l_glimpse_ast set. So the logic is: if there is a lock
	 * with no l_glimpse_ast set, this object is being destroyed already.
	 * Hence, if you are grabbing DLM locks on the server, always set
	 * non-NULL glimpse_ast (e.g., ldlm_request.c::ldlm_glimpse_ast()).
	 */
	if (lock->l_glimpse_ast == NULL) {
		LDLM_DEBUG(lock, "no l_glimpse_ast");
		GOTO(out, rc = -ENOENT);
	}

	OBD_SLAB_ALLOC_PTR_GFP(gl_work, ldlm_glimpse_work_kmem, GFP_ATOMIC);
	if (!gl_work)
		GOTO(out, rc = -ENOMEM);

	/* Populate the gl_work structure.
	 * Grab additional reference on the lock which will be released in
	 * ldlm_work_gl_ast_lock() */
	gl_work->gl_lock = LDLM_LOCK_GET(lock);
	/* The glimpse callback is sent to one single IO lock. As a result,
	 * the gl_work list is just composed of one element */
	INIT_LIST_HEAD(&gl_list);
	list_add_tail(&gl_work->gl_list, &gl_list);
	/* There is actually no need for a glimpse descriptor when glimpsing
	 * IO locks */
	gl_work->gl_desc = NULL;
	/* the ldlm_glimpse_work structure is allocated on the stack */
	gl_work->gl_flags = LDLM_GL_WORK_SLAB_ALLOCATED;

	ldlm_glimpse_locks(res, &gl_list); /* this will update the LVB */

	/* If the list is not empty, we failed to glimpse a lock and
	 * must clean it up. Usually due to a race with unlink.*/
	if (!list_empty(&gl_list)) {
		LDLM_LOCK_RELEASE(lock);
		OBD_SLAB_FREE_PTR(gl_work, ldlm_glimpse_work_kmem);
	}
	rc = 0;
	EXIT;
out:
	LDLM_LOCK_PUT(lock);
	return rc;
}

static void mdt_lvb2body(struct ldlm_resource *res, struct mdt_body *mb)
{
	struct ost_lvb *res_lvb;

	lock_res(res);
	res_lvb = res->lr_lvb_data;
	mb->mbo_dom_size = res_lvb->lvb_size;
	mb->mbo_dom_blocks = res_lvb->lvb_blocks;
	mb->mbo_mtime = res_lvb->lvb_mtime;
	mb->mbo_ctime = res_lvb->lvb_ctime;
	mb->mbo_atime = res_lvb->lvb_atime;

	CDEBUG(D_DLMTRACE, "size %llu\n", res_lvb->lvb_size);

	mb->mbo_valid |= OBD_MD_FLATIME | OBD_MD_FLCTIME | OBD_MD_FLMTIME |
			 OBD_MD_DOM_SIZE;
	unlock_res(res);
}

/**
 * MDT glimpse for Data-on-MDT
 *
 * This function is called when MDT get attributes for the DoM object.
 * If there is write lock on client then function issues glimpse_ast to get
 * an actual size from that client.
 */
int mdt_dom_object_size(const struct lu_env *env, struct mdt_device *mdt,
			const struct lu_fid *fid, struct mdt_body *mb,
			bool dom_lock)
{
	struct ldlm_res_id resid;
	struct ldlm_resource *res;
	int rc = 0;

	ENTRY;

	fid_build_reg_res_name(fid, &resid);
	res = ldlm_resource_get(mdt->mdt_namespace, NULL, &resid,
				LDLM_IBITS, 1);
	if (IS_ERR(res))
		RETURN(-ENOENT);

	/* Update lvbo data if DoM lock returned or if LVB is not yet valid. */
	if (dom_lock || !mdt_dom_lvb_is_valid(res))
		mdt_dom_lvbo_update(res, NULL, NULL, false);

	mdt_lvb2body(res, mb);
	ldlm_resource_putref(res);
	RETURN(rc);
}

/**
 * MDT DoM lock intent policy (glimpse)
 *
 * Intent policy is called when lock has an intent, for DoM file that
 * means glimpse lock and policy fills Lock Value Block (LVB).
 *
 * If already granted lock is found it will be placed in \a lockp and
 * returned back to caller function.
 *
 * \param[in] tsi	 session info
 * \param[in,out] lockp	 pointer to the lock
 * \param[in] flags	 LDLM flags
 *
 * \retval		ELDLM_LOCK_REPLACED if already granted lock was found
 *			and placed in \a lockp
 * \retval		ELDLM_LOCK_ABORTED in other cases except error
 * \retval		negative value on error
 */
int mdt_glimpse_enqueue(struct mdt_thread_info *mti, struct ldlm_namespace *ns,
			struct ldlm_lock **lockp, __u64 flags)
{
	struct ldlm_lock *lock = *lockp;
	struct ldlm_resource *res = lock->l_resource;
	ldlm_processing_policy policy;
	struct ldlm_reply *rep;
	struct mdt_body *mbo;
	int rc;

	ENTRY;

	policy = ldlm_get_processing_policy(res);
	LASSERT(policy != NULL);

	req_capsule_set_size(mti->mti_pill, &RMF_MDT_MD, RCL_SERVER, 0);
	req_capsule_set_size(mti->mti_pill, &RMF_ACL, RCL_SERVER, 0);
	rc = req_capsule_server_pack(mti->mti_pill);
	if (rc)
		RETURN(err_serious(rc));

	rep = req_capsule_server_get(mti->mti_pill, &RMF_DLM_REP);
	if (rep == NULL)
		RETURN(-EPROTO);

	mbo = req_capsule_server_get(mti->mti_pill, &RMF_MDT_BODY);
	if (mbo == NULL)
		RETURN(-EPROTO);

	lock_res(res);
	/* Check if this is a resend case (MSG_RESENT is set on RPC) and a
	 * lock was found by ldlm_handle_enqueue(); if so no need to grant
	 * it again. */
	if (flags & LDLM_FL_RESENT) {
		rc = LDLM_ITER_CONTINUE;
	} else {
		__u64 tmpflags = 0;
		enum ldlm_error err;

		rc = policy(lock, &tmpflags, LDLM_PROCESS_RESCAN, &err, NULL);
		check_res_locked(res);
	}
	unlock_res(res);

	/* The lock met with no resistance; we're finished. */
	if (rc == LDLM_ITER_CONTINUE) {
		GOTO(fill_mbo, rc = ELDLM_LOCK_REPLACED);
	} else if (flags & LDLM_FL_BLOCK_NOWAIT) {
		/* LDLM_FL_BLOCK_NOWAIT means it is for AGL. Do not send glimpse
		 * callback for glimpse size. The real size user will trigger
		 * the glimpse callback when necessary. */
		GOTO(fill_mbo, rc = ELDLM_LOCK_ABORTED);
	}

	rc = mdt_do_glimpse(mti->mti_env, ns, res);
	if (rc == -ENOENT) {
		/* We are racing with unlink(); just return -ENOENT */
		rep->lock_policy_res2 = ptlrpc_status_hton(-ENOENT);
		rc = 0;
	} else if (rc == -EINVAL) {
		/* this is possible is client lock has been cancelled but
		 * still exists on server. If that lock was found on server
		 * as only conflicting lock then the client has already
		 * size authority and glimpse is not needed. */
		CDEBUG(D_DLMTRACE, "Glimpse from the client owning lock\n");
		rc = 0;
	} else if (rc < 0) {
		RETURN(rc);
	}
	rc = ELDLM_LOCK_ABORTED;
fill_mbo:
	/* LVB can be without valid data in case of DOM */
	if (!mdt_dom_lvb_is_valid(res))
		mdt_dom_lvbo_update(res, lock, NULL, false);
	mdt_lvb2body(res, mbo);
	RETURN(rc);
}

int mdt_brw_enqueue(struct mdt_thread_info *mti, struct ldlm_namespace *ns,
		    struct ldlm_lock **lockp, __u64 flags)
{
	struct tgt_session_info *tsi = tgt_ses_info(mti->mti_env);
	struct lu_fid *fid = &tsi->tsi_fid;
	struct ldlm_lock *lock = *lockp;
	struct ldlm_resource *res = lock->l_resource;
	struct ldlm_reply *rep;
	struct mdt_body *mbo;
	struct mdt_lock_handle *lhc = &mti->mti_lh[MDT_LH_RMT];
	struct mdt_object *mo;
	int rc = 0;

	ENTRY;

	/* Get lock from request for possible resent case. */
	mdt_intent_fixup_resent(mti, *lockp, lhc, flags);
	req_capsule_set_size(mti->mti_pill, &RMF_MDT_MD, RCL_SERVER, 0);
	req_capsule_set_size(mti->mti_pill, &RMF_ACL, RCL_SERVER, 0);
	rc = req_capsule_server_pack(mti->mti_pill);
	if (rc)
		RETURN(err_serious(rc));

	rep = req_capsule_server_get(mti->mti_pill, &RMF_DLM_REP);
	if (rep == NULL)
		RETURN(-EPROTO);

	mbo = req_capsule_server_get(mti->mti_pill, &RMF_MDT_BODY);
	if (mbo == NULL)
		RETURN(-EPROTO);

	fid_extract_from_res_name(fid, &res->lr_name);
	mo = mdt_object_find(mti->mti_env, mti->mti_mdt, fid);
	if (unlikely(IS_ERR(mo)))
		RETURN(PTR_ERR(mo));

	if (!mdt_object_exists(mo))
		GOTO(out, rc = -ENOENT);

	if (mdt_object_remote(mo))
		GOTO(out, rc = -EPROTO);

	/* resent case */
	if (!lustre_handle_is_used(&lhc->mlh_reg_lh)) {
		mdt_lock_handle_init(lhc);
		mdt_lock_reg_init(lhc, (*lockp)->l_req_mode);
		/* This will block MDT thread but it should be fine until
		 * client caches small amount of data for DoM, which should be
		 * smaller than one BRW RPC and should be able to be
		 * piggybacked by lock cancel RPC.
		 * If the client could hold the lock too long, this code can be
		 * revised to call mdt_object_lock_try(). And if fails, it will
		 * return ELDLM_OK here and fall back into normal lock enqueue
		 * process.
		 */
		rc = mdt_object_lock(mti, mo, lhc, MDS_INODELOCK_DOM);
		if (rc)
			GOTO(out, rc);
	}

	if (!mdt_dom_lvb_is_valid(res)) {
		rc = mdt_dom_lvb_alloc(res);
		if (rc)
			GOTO(out_fail, rc);
		mdt_dom_disk_lvbo_update(mti->mti_env, mo, res, false);
	}
	mdt_lvb2body(res, mbo);
out_fail:
	rep->lock_policy_res2 = clear_serious(rc);
	if (rep->lock_policy_res2) {
		lhc->mlh_reg_lh.cookie = 0ull;
		GOTO(out, rc = ELDLM_LOCK_ABORTED);
	}

	rc = mdt_intent_lock_replace(mti, lockp, lhc, flags, rc);
out:
	mdt_object_put(mti->mti_env, mo);
	RETURN(rc);
}

void mdt_dom_discard_data(struct mdt_thread_info *info,
			  const struct lu_fid *fid)
{
	struct mdt_device *mdt = info->mti_mdt;
	union ldlm_policy_data *policy = &info->mti_policy;
	struct ldlm_res_id *res_id = &info->mti_res_id;
	struct lustre_handle dom_lh;
	__u64 flags = LDLM_FL_AST_DISCARD_DATA;
	int rc = 0;

	policy->l_inodebits.bits = MDS_INODELOCK_DOM;
	policy->l_inodebits.try_bits = 0;
	fid_build_reg_res_name(fid, res_id);

	/* Tell the clients that the object is gone now and that they should
	 * throw away any cached pages. */
	rc = ldlm_cli_enqueue_local(mdt->mdt_namespace, res_id, LDLM_IBITS,
				    policy, LCK_PW, &flags, ldlm_blocking_ast,
				    ldlm_completion_ast, NULL, NULL, 0,
				    LVB_T_NONE, NULL, &dom_lh);

	/* We only care about the side-effects, just drop the lock. */
	if (rc == ELDLM_OK)
		ldlm_lock_decref(&dom_lh, LCK_PW);
}

/* check if client has already DoM lock for given resource */
bool mdt_dom_client_has_lock(struct mdt_thread_info *info,
			     const struct lu_fid *fid)
{
	struct mdt_device *mdt = info->mti_mdt;
	union ldlm_policy_data *policy = &info->mti_policy;
	struct ldlm_res_id *res_id = &info->mti_res_id;
	struct lustre_handle lockh;
	enum ldlm_mode mode;
	struct ldlm_lock *lock;
	bool rc;

	policy->l_inodebits.bits = MDS_INODELOCK_DOM;
	fid_build_reg_res_name(fid, res_id);

	mode = ldlm_lock_match(mdt->mdt_namespace, LDLM_FL_BLOCK_GRANTED |
			       LDLM_FL_TEST_LOCK, res_id, LDLM_IBITS, policy,
			       LCK_PW, &lockh, 0);

	/* There is no other PW lock on this object; finished. */
	if (mode == 0)
		return false;

	lock = ldlm_handle2lock(&lockh);
	if (lock == 0)
		return false;

	/* check if lock from the same client */
	rc = (lock->l_export->exp_handle.h_cookie ==
	      info->mti_exp->exp_handle.h_cookie);
	LDLM_LOCK_PUT(lock);
	return rc;
}


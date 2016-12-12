/*-------------------------------------------------------------------------
 *
 * gtm_ipc.h
 *	  POSTGRES_X2 inter-process communication definitions.
 * 
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.	The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 * Portions Copyright (c) 2015, PostgreSQL_X2 Global Development Group
 *
 * src/include/gtm/gtm_ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_IPC_H
#define GTM_IPC_H

typedef void (*gtm_pg_on_exit_callback) (int code, uintptr_t arg);

/* gtm_ipc.c */
extern bool gtm_proc_exit_inprogress;
extern void gtm_proc_exit(int code) __attribute__((noreturn));
extern void gtm_on_proc_exit(gtm_pg_on_exit_callback function, uintptr_t arg);

#endif   /* GTM_IPC_H */


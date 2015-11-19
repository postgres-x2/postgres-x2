/*-------------------------------------------------------------------------
 *
 * gtm_ipc.c
 *		GTM inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.  The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 2015, PostgreSQL_X2 Global Development Group
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_ipc.c
 *
 *-------------------------------------------------------------------------
 */

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "gtm/elog.h"
#include "gtm/gtm_ipc.h"

/*
 * This flag is set during gtm_proc_exit() to change ereport()'s behavior,
 * so that an ereport() from an on_proc_exit routine cannot get us out
 * of the exit procedure.  We do NOT want to go back to the idle loop...
 */
bool		gtm_proc_exit_inprogress = false;

/*
 * This flag tracks whether we've called gtm_atexit() in the current process
 */
static bool gtm_atexit_callback_setup = false;

/* local functions */
static void gtm_proc_exit_prepare(int code);


/* ----------------------------------------------------------------
 *                      exit() handling stuff
 *
 * These functions are in generally the same spirit as atexit(),
 * but provide some additional features we need --- in particular,
 * we want to register callbacks to invoke when gtm/proxy exit
 *
 * Callback functions can take zero, one, or two args: the first passed
 * arg is the integer exitcode, the second is the uintptr_t supplied when
 * the callback was registered.
 * ----------------------------------------------------------------
 */


#define MAX_ON_EXITS 20

static struct ONEXIT
{
	gtm_pg_on_exit_callback function;
	uintptr_t				arg;
}	gtm_on_proc_exit_list[MAX_ON_EXITS];

static int  gtm_on_proc_exit_index;

/* ----------------------------------------------------------------
 *      borrow from proc_exit
 *
 *      this function calls all the callbacks registered
 *      for it (to free resources) and then calls exit.
 *
 *      This should be the only function to call exit().
 *      -cim 2/6/90
 *
 *      Unfortunately, we can't really guarantee that add-on code
 *      obeys the rule of not calling exit() directly.  So, while
 *      this is the preferred way out of the system, we also register
 *      an atexit callback that will make sure cleanup happens.
 * ----------------------------------------------------------------
 */
void
gtm_proc_exit(int code)
{
 /* Clean up everything that must be cleaned up */
	gtm_proc_exit_prepare(code);

#ifdef PROFILE_PID_DIR
	{
		char        gprofDirName[32];

		snprintf(gprofDirName, 32, "gtm_gprof/%d", (int) getpid());

		mkdir("gtm_gprof", S_IRWXU | S_IRWXG | S_IRWXO);
		mkdir(gprofDirName, S_IRWXU | S_IRWXG | S_IRWXO);
		chdir(gprofDirName);
	}
#endif

	elog(DEBUG3, "exit(%d)", code);

	exit(code);
}

/*
 * Code shared between proc_exit and the atexit handler.  Note that in
 * normal exit through proc_exit, this will actually be called twice ...
 * but the second call will have nothing to do.
 */
static void
gtm_proc_exit_prepare(int code)
{
	/*
	 * Once we set this flag, we are committed to exit.  Any ereport() will
	 * NOT send control back to the main loop, but right back here.
	 */
	gtm_proc_exit_inprogress = true;

    elog(DEBUG3, "proc_exit(%d): %d callbacks to make",
         code, gtm_on_proc_exit_index);

    /*
 	 * call all the registered callbacks.
 	 *
 	 * Note that since we decrement on_proc_exit_index each time, if a
 	 * callback calls ereport(ERROR) or ereport(FATAL) then it won't be
 	 * invoked again when control comes back here (nor will the
 	 * previously-completed callbacks).  So, an infinite loop should not be
 	 * possible.
 	 */
	while (--gtm_on_proc_exit_index >= 0)
		(*gtm_on_proc_exit_list[gtm_on_proc_exit_index].function) (code,
								gtm_on_proc_exit_list[gtm_on_proc_exit_index].arg);

	gtm_on_proc_exit_index = 0;
}

/* ----------------------------------------------------------------
 *		borrow from atexit_callback
 *
 *		Backstop to ensure that direct calls of exit() don't mess us up.
 * ----------------------------------------------------------------
 */
static void
gtm_atexit_callback(void)
{
	/* Clean up everything that must be cleaned up */
	/* ... too bad we don't know the real exit code ... */
	gtm_proc_exit_prepare(-1);
}

/* ----------------------------------------------------------------
 *		borrow from on_proc_exit
 *
 *		this function adds a callback function to the list of
 *		functions invoked by gtm_proc_exit().
 * ----------------------------------------------------------------
 */
void
gtm_on_proc_exit(gtm_pg_on_exit_callback function, uintptr_t arg)
{
	if (gtm_on_proc_exit_index >= MAX_ON_EXITS)
		ereport(FATAL,
				(EINVAL,
				errmsg_internal("out of on_proc_exit slots")));

	gtm_on_proc_exit_list[gtm_on_proc_exit_index].function = function;
	gtm_on_proc_exit_list[gtm_on_proc_exit_index].arg = arg;

	++gtm_on_proc_exit_index;

	if (!gtm_atexit_callback_setup)
	{
		atexit(gtm_atexit_callback);
		gtm_atexit_callback_setup = true;
	}
}


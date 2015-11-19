/*-------------------------------------------------------------------------
 *
 * gtm_miscinit.c
 *    miscellaneous initialization support stuff
 *
 * Portions Copyright (c) 2015, PostgreSQL_X2 Global Development Group
 *
 *
 * IDENTIFICATION
 *    src/gtm/common/gtm_miscinit.c
 *
 *-------------------------------------------------------------------------
 */

#include <sys/param.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <grp.h>
#include <pwd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif

//#include "gtm/gtm_c.h"
//#include "gtm/path.h"
#include "gtm/gtm.h"
#include "gtm/elog.h"
#include "gtm/gtm_miscinit.h"

/*
 * Create a lockfile.
 *
 * filename is the name of the lockfile to create.
 * dirName is used to determine what error message to produce.
 */
static void
CreateLockFile(const char *filename, const char *dirName)
{
	int			fd;
	char		buffer[MAXPGPATH + 100];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		other_pid;
	pid_t		my_pid = getpid();

	/*
	 * We need a loop here because of race conditions.	But don't loop forever
	 * (for example, a non-writable $PGDATA directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600.	See
		 * comments below.
		 */
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not create lock file \"%s\": %m",
							filename)));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not open lock file \"%s\": %m",
							filename)));
		}
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			ereport(FATAL,
					(EINVAL,
					 errmsg("could not read lock file \"%s\": %m",
							filename)));
		close(fd);

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);
		other_pid = (pid_t) encoded_pid;

		if (other_pid <= 0)
			elog(FATAL, "bogus data in lock file \"%s\": \"%s\"",
				 filename, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * If the PID in the lockfile is our own PID or our parent's PID, then
		 * the file must be stale (probably left over from a previous system
		 * boot cycle).  We need this test because of the likelihood that a
		 * reboot will assign exactly the same PID as we had in the previous
		 * reboot.	Also, if there is just one more process launch in this
		 * reboot than in the previous one, the lockfile might mention our
		 * parent's PID.  We can reject that since we'd never be launched
		 * directly by a competing postmaster.	We can't detect grandparent
		 * processes unfortunately, but if the init script is written
		 * carefully then all but the immediate parent shell will be
		 * root-owned processes and so the kill test will fail with EPERM.
		 *
		 * We can treat the EPERM-error case as okay because that error
		 * implies that the existing process has a different userid than we
		 * do, which means it cannot be a competing postmaster.  A postmaster
		 * cannot successfully attach to a data directory owned by a userid
		 * other than its own.	(This is now checked directly in
		 * checkDataDir(), but has been true for a long time because of the
		 * restriction that the data directory isn't group- or
		 * world-accessible.)  Also, since we create the lockfiles mode 600,
		 * we'd have failed above if the lockfile belonged to another userid
		 * --- which means that whatever process kill() is reporting about
		 * isn't the one that made the lockfile.  (NOTE: this last
		 * consideration is the only one that keeps us from blowing away a
		 * Unix socket file belonging to an instance of Postgres being run by
		 * someone else, at least on machines where /tmp hasn't got a
		 * stickybit.)
		 *
		 * Windows hasn't got getppid(), but doesn't need it since it's not
		 * using real kill() either...
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist.
		 */
		if (other_pid != my_pid
#ifndef WIN32
			&& other_pid != getppid()
#endif
			)
		{
			if (kill(other_pid, 0) == 0 ||
				(errno != ESRCH && errno != EPERM))
			{
				/* lockfile belongs to a live process */
				ereport(FATAL,
						(EINVAL,
						 errmsg("lock file \"%s\" already exists",
								filename),
						  errhint("Is another GTM proxy (PID %d) running in data directory \"%s\"?",
								  (int) other_pid, dirName)));
			}
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.	Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(filename) < 0)
			ereport(FATAL,
					(EACCES,
					 errmsg("could not remove old lock file \"%s\": %m",
							filename),
					 errhint("The file seems accidentally left over, but "
						   "it could not be removed. Please remove the file "
							 "by hand and try again.")));
	}

	/*
	 * Successfully created the file, now fill it.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n%s\n",
			 (int) my_pid, dirName);
	errno = 0;
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(FATAL,
				(EACCES,
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}
	if (close(fd))
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		ereport(FATAL,
				(EACCES,
				 errmsg("could not write lock file \"%s\": %m", filename)));
	}

}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 *
 * Note that the socket directory path line is initially written as empty.
 * gtm or proxy will rewrite it upon creating the first Unix socket.
 */
void
CreateDataDirLockFile(const char *pidfile, const char *dataDir)
{
	CreateLockFile(pidfile, dataDir);
}

/*
 * Create a lockfile for the specified Unix socket file.
 */
void
CreateSocketLockFile(const char *socketfile, const char *socketDir)
{
	char		lockfile[MAXPGPATH];

	snprintf(lockfile, sizeof(lockfile), "%s.lock", socketfile);
	CreateLockFile(lockfile, socketDir);
}

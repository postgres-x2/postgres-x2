/*-------------------------------------------------------------------------
 *
 * assert.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2015 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
/*
 * In this file, we define Trap and TrapMacro macros, which are duplicate
 * to corresponding PostgreSQL's ones defined in "c.h".
 * We need separate definition due to the difference in process/thread
 * model of postgres and gtm/gtm_proxy.
 * In previous PG version, these macros were defined in separate assert.h
 * file.  Because they have been moved to c.h, we need to redifine them
 * here.
 *
 * Programmers of XC core must be careful about this duplicate.
 */
#ifndef GTM_ASSERT_H
#define GTM_ASSERT_H

#include "c.h"

extern bool assert_enabled;

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Trap
 *		Generates an exception if the given condition is true.
 */
/*
 * Macro Trap is originally defined in "c.h".   We need to redefine
 * this to use in GTM/GTM_Proxy.
 */
#ifdef Trap
#undef Trap
#endif
#define Trap(condition, errorType) \
	do { \
		if ((assert_enabled) && (condition)) \
			ExceptionalCondition(CppAsString(condition), (errorType), \
								 __FILE__, __LINE__); \
	} while (0)

/*
 *	TrapMacro is the same as Trap but it's intended for use in macros:
 *
 *		#define foo(x) (AssertMacro(x != 0) && bar(x))
 *
 *	Isn't CPP fun?
 */
/*
 * Macro TrapMacro is originally defined in "c.h".   We need to redefine
 * this to use in GTM/GTM_Proxy.
 */
#ifdef TrapMacro
#undef TrapMacro
#endif
#define TrapMacro(condition, errorType) \
	((bool) ((! assert_enabled) || ! (condition) || \
			 (ExceptionalCondition(CppAsString(condition), (errorType), \
								   __FILE__, __LINE__))))

#ifndef USE_ASSERT_CHECKING
#define Assert(condition)
#define AssertMacro(condition)	((void)true)
#define AssertArg(condition)
#define AssertState(condition)
#else
#define Assert(condition) \
		Trap(!(condition), "FailedAssertion")

#define AssertMacro(condition) \
		((void) TrapMacro(!(condition), "FailedAssertion"))

#define AssertArg(condition) \
		Trap(!(condition), "BadArgument")

#define AssertState(condition) \
		Trap(!(condition), "BadState")
#endif   /* USE_ASSERT_CHECKING */

extern int ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName, int lineNumber);

#endif

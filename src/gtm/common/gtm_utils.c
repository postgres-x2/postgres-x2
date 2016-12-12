/*-------------------------------------------------------------------------
 *
 * gtm_utils.c
 *  Utililies of GTM
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/gtm/common/gtm_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "gtm/gtm_utils.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_msg.h"

#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}

#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);  c += b; \
  b -= a;  b ^= rot(a, 6);  a += c; \
  c -= b;  c ^= rot(b, 8);  b += a; \
  a -= c;  a ^= rot(c,16);  c += b; \
  b -= a;  b ^= rot(a,19);  a += c; \
  c -= b;  c ^= rot(b, 4);  b += a; \
}

struct enum_name
{
	int type;
	char *name;
};

/*
 * Advise:
 * Following table can be formatted using gtm_msg.h definitions.
 */
static struct enum_name message_name_tab[] =
{
	{MSG_TYPE_INVALID, "MSG_TYPE_INVALID"},
	{MSG_SYNC_STANDBY, "MSG_SYNC_STANDBY"},
	{MSG_NODE_REGISTER, "MSG_NODE_REGISTER"},
	{MSG_BKUP_NODE_REGISTER, "MSG_BKUP_NODE_REGISTER"},
	{MSG_NODE_UNREGISTER, "MSG_NODE_UNREGISTER"},
	{MSG_BKUP_NODE_UNREGISTER, "MSG_BKUP_NODE_UNREGISTER"},
	{MSG_NODE_LIST, "MSG_NODE_LIST"},
	{MSG_NODE_BEGIN_REPLICATION_INIT, "MSG_NODE_BEGIN_REPLICATION_INIT"},
	{MSG_NODE_END_REPLICATION_INIT, "MSG_NODE_END_REPLICATION_INIT"},
	{MSG_BEGIN_BACKUP, "MSG_BEGIN_BACKUP"},
	{MSG_END_BACKUP, "MSG_END_BACKUP"},
	{MSG_TXN_BEGIN, "MSG_TXN_BEGIN"},
	{MSG_BKUP_TXN_BEGIN, "MSG_BKUP_TXN_BEGIN"},
	{MSG_TXN_BEGIN_GETGXID, "MSG_TXN_BEGIN_GETGXID"},
	{MSG_BKUP_TXN_BEGIN_GETGXID, "MSG_BKUP_TXN_BEGIN_GETGXID"},
	{MSG_TXN_BEGIN_GETGXID_MULTI, "MSG_TXN_BEGIN_GETGXID_MULTI"},
	{MSG_BKUP_TXN_BEGIN_GETGXID_MULTI, "MSG_BKUP_TXN_BEGIN_GETGXID_MULTI"},
	{MSG_TXN_START_PREPARED, "MSG_TXN_START_PREPARED"},
	{MSG_BKUP_TXN_START_PREPARED, "MSG_BKUP_TXN_START_PREPARED"},
	{MSG_TXN_COMMIT, "MSG_TXN_COMMIT"},
	{MSG_BKUP_TXN_COMMIT, "MSG_BKUP_TXN_COMMIT"},
	{MSG_TXN_COMMIT_MULTI, "MSG_TXN_COMMIT_MULTI"},
	{MSG_BKUP_TXN_COMMIT_MULTI, "MSG_BKUP_TXN_COMMIT_MULTI"},
	{MSG_TXN_COMMIT_PREPARED, "MSG_TXN_COMMIT_PREPARED"},
	{MSG_BKUP_TXN_COMMIT_PREPARED, "MSG_BKUP_TXN_COMMIT_PREPARED"},
	{MSG_TXN_PREPARE, "MSG_TXN_PREPARE"},
	{MSG_BKUP_TXN_PREPARE, "MSG_BKUP_TXN_PREPARE"},
	{MSG_TXN_ROLLBACK, "MSG_TXN_ROLLBACK"},
	{MSG_BKUP_TXN_ROLLBACK, "MSG_BKUP_TXN_ROLLBACK"},
	{MSG_TXN_ROLLBACK_MULTI, "MSG_TXN_ROLLBACK_MULTI"},
	{MSG_BKUP_TXN_ROLLBACK_MULTI, "MSG_BKUP_TXN_ROLLBACK_MULTI"},
	{MSG_TXN_GET_GID_DATA, "MSG_TXN_GET_GID_DATA"},
	{MSG_TXN_GET_GXID, "MSG_TXN_GET_GXID"},
	{MSG_BKUP_TXN_GET_GXID, "MSG_BKUP_TXN_GET_GXID"},
	{MSG_TXN_GET_NEXT_GXID, "MSG_TXN_GET_NEXT_GXID"},
	{MSG_TXN_GXID_LIST, "MSG_TXN_GXID_LIST"},
	{MSG_SNAPSHOT_GET, "MSG_SNAPSHOT_GET"},
	{MSG_SNAPSHOT_GET_MULTI, "MSG_SNAPSHOT_GET_MULTI"},
	{MSG_SNAPSHOT_GXID_GET, "MSG_SNAPSHOT_GXID_GET"},
	{MSG_SEQUENCE_INIT, "MSG_SEQUENCE_INIT"},
	{MSG_BKUP_SEQUENCE_INIT, "MSG_BKUP_SEQUENCE_INIT"},
	{MSG_SEQUENCE_GET_NEXT, "MSG_SEQUENCE_GET_NEXT"},
	{MSG_BKUP_SEQUENCE_GET_NEXT, "MSG_BKUP_SEQUENCE_GET_NEXT"},
	{MSG_SEQUENCE_GET_LAST, "MSG_SEQUENCE_GET_LAST"},
	{MSG_SEQUENCE_SET_VAL, "MSG_SEQUENCE_SET_VAL"},
	{MSG_BKUP_SEQUENCE_SET_VAL, "MSG_BKUP_SEQUENCE_SET_VAL"},
	{MSG_SEQUENCE_RESET, "MSG_SEQUENCE_RESET"},
	{MSG_BKUP_SEQUENCE_RESET, "MSG_BKUP_SEQUENCE_RESET"},
	{MSG_SEQUENCE_CLOSE, "MSG_SEQUENCE_CLOSE"},
	{MSG_BKUP_SEQUENCE_CLOSE, "MSG_BKUP_SEQUENCE_CLOSE"},
	{MSG_SEQUENCE_RENAME, "MSG_SEQUENCE_RENAME"},
	{MSG_BKUP_SEQUENCE_RENAME, "MSG_BKUP_SEQUENCE_RENAME"},
	{MSG_BKUP_SEQUENCE_RENAME, "MSG_BKUP_SEQUENCE_RENAME"},
	{MSG_SEQUENCE_ALTER, "MSG_SEQUENCE_ALTER"},
	{MSG_BKUP_SEQUENCE_ALTER, "MSG_BKUP_SEQUENCE_ALTER"},
	{MSG_SEQUENCE_LIST, "MSG_SEQUENCE_LIST"},
	{MSG_TXN_GET_STATUS, "MSG_TXN_GET_STATUS"},
	{MSG_TXN_GET_ALL_PREPARED, "MSG_TXN_GET_ALL_PREPARED"},
	{MSG_TXN_BEGIN_GETGXID_AUTOVACUUM, "MSG_TXN_BEGIN_GETGXID_AUTOVACUUM"},
	{MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM, "MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM"},
	{MSG_DATA_FLUSH, "MSG_DATA_FLUSH"},
	{MSG_BACKEND_DISCONNECT, "MSG_BACKEND_DISCONNECT"},
	{MSG_TYPE_COUNT, "MSG_TYPE_COUNT"},
	{-1, NULL}
};

static struct enum_name result_name_tab[] =
{
	{SYNC_STANDBY_RESULT, "SYNC_STANDBY_RESULT"},
	{NODE_REGISTER_RESULT, "NODE_REGISTER_RESULT"},
	{NODE_UNREGISTER_RESULT, "NODE_UNREGISTER_RESULT"},
	{NODE_LIST_RESULT, "NODE_LIST_RESULT"},
	{NODE_BEGIN_REPLICATION_INIT_RESULT, "NODE_BEGIN_REPLICATION_INIT_RESULT"},
	{NODE_END_REPLICATION_INIT_RESULT, "NODE_END_REPLICATION_INIT_RESULT"},
	{BEGIN_BACKUP_RESULT, "BEGIN_BACKUP_RESULT"},
	{END_BACKUP_RESULT, "END_BACKUP_RESULT"},
	{TXN_BEGIN_RESULT, "TXN_BEGIN_RESULT"},
	{TXN_BEGIN_GETGXID_RESULT, "TXN_BEGIN_GETGXID_RESULT"},
	{TXN_BEGIN_GETGXID_MULTI_RESULT, "TXN_BEGIN_GETGXID_MULTI_RESULT"},
	{TXN_PREPARE_RESULT, "TXN_PREPARE_RESULT"},
	{TXN_START_PREPARED_RESULT, "TXN_START_PREPARED_RESULT"},
	{TXN_COMMIT_PREPARED_RESULT, "TXN_COMMIT_PREPARED_RESULT"},
	{TXN_COMMIT_RESULT, "TXN_COMMIT_RESULT"},
	{TXN_COMMIT_MULTI_RESULT, "TXN_COMMIT_MULTI_RESULT"},
	{TXN_ROLLBACK_RESULT, "TXN_ROLLBACK_RESULT"},
	{TXN_ROLLBACK_MULTI_RESULT, "TXN_ROLLBACK_MULTI_RESULT"},
	{TXN_GET_GID_DATA_RESULT, "TXN_GET_GID_DATA_RESULT"},
	{TXN_GET_GXID_RESULT, "TXN_GET_GXID_RESULT"},
	{TXN_GET_NEXT_GXID_RESULT, "TXN_GET_NEXT_GXID_RESULT"},
	{TXN_GXID_LIST_RESULT, "TXN_GXID_LIST_RESULT"},
	{SNAPSHOT_GET_RESULT, "SNAPSHOT_GET_RESULT"},
	{SNAPSHOT_GET_MULTI_RESULT, "SNAPSHOT_GET_MULTI_RESULT"},
	{SNAPSHOT_GXID_GET_RESULT, "SNAPSHOT_GXID_GET_RESULT"},
	{SEQUENCE_INIT_RESULT, "SEQUENCE_INIT_RESULT"},
	{SEQUENCE_GET_NEXT_RESULT, "SEQUENCE_GET_NEXT_RESULT"},
	{SEQUENCE_GET_LAST_RESULT, "SEQUENCE_GET_LAST_RESULT"},
	{SEQUENCE_SET_VAL_RESULT, "SEQUENCE_SET_VAL_RESULT"},
	{SEQUENCE_RESET_RESULT, "SEQUENCE_RESET_RESULT"},
	{SEQUENCE_CLOSE_RESULT, "SEQUENCE_CLOSE_RESULT"},
	{SEQUENCE_RENAME_RESULT, "SEQUENCE_RENAME_RESULT"},
	{SEQUENCE_ALTER_RESULT, "SEQUENCE_ALTER_RESULT"},
	{SEQUENCE_LIST_RESULT, "SEQUENCE_LIST_RESULT"},
	{TXN_GET_STATUS_RESULT, "TXN_GET_STATUS_RESULT"},
	{TXN_GET_ALL_PREPARED_RESULT, "TXN_GET_ALL_PREPARED_RESULT"},
	{TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT, "TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT"},
	{RESULT_TYPE_COUNT, "RESULT_TYPE_COUNT"},
	{-1, NULL}
};

static char **message_name = NULL;
static int message_max;
static char **result_name = NULL;
static int result_max;

void gtm_util_init_nametabs(void)
{
	int ii;

	if (message_name)
		free(message_name);
	if (result_name)
		free(result_name);
	for (ii = 0, message_max = 0; message_name_tab[ii].type >= 0; ii++)
	{
		if (message_max < message_name_tab[ii].type)
			message_max = message_name_tab[ii].type;
	}
	message_name = (char **)malloc(sizeof(char *) * (message_max + 1));
	memset(message_name, 0, sizeof(char *) * (message_max + 1));
	for (ii = 0; message_name_tab[ii].type >= 0; ii++)
	{
		message_name[message_name_tab[ii].type] = message_name_tab[ii].name;
	}

	for (ii = 0, result_max = 0; result_name_tab[ii].type >= 0; ii++)
	{
		if (result_max < result_name_tab[ii].type)
			result_max = result_name_tab[ii].type;
	}
	result_name = (char **)malloc(sizeof(char *) * (result_max + 1));
	memset(result_name, 0, sizeof(char *) * (result_max + 1));
	for (ii = 0; result_name_tab[ii].type >= 0; ii++)
	{
		result_name[result_name_tab[ii].type] = result_name_tab[ii].name;
	}
}

char *gtm_util_message_name(GTM_MessageType type)
{
	if (message_name == NULL)
		gtm_util_init_nametabs();
	if (type > message_max)
		return "UNKNOWN_MESSAGE";
	return message_name[type];
}

char *gtm_util_result_name(GTM_ResultType type)
{
	if (result_name == NULL)
		gtm_util_init_nametabs();
	if (type > result_max)
		return "UNKNOWN_RESULT";
	return result_name[type];
}

uint32
gtm_util_hash_any(const unsigned char *k, register int keylen)
{
    register uint32 a,
                b,
                c,
                len;

    /* Set up the internal state */
    len = keylen;
    a = b = c = 0x9e3779b9 + len + 3923095;

    /* If the source pointer is word-aligned, we use word-wide fetches */
    if (((uintptr_t) k & UINT32_ALIGN_MASK) == 0)
    {
        /* Code path for aligned source data */
        register const uint32 *ka = (const uint32 *) k;

        /* handle most of the key */
        while (len >= 12)
        {
            a += ka[0];
            b += ka[1];
            c += ka[2];
            mix(a, b, c);
            ka += 3;
            len -= 12;
        }

        /* handle the last 11 bytes */
        k = (const unsigned char *) ka;
#ifdef WORDS_BIGENDIAN
        switch (len)
        {
            case 11:
                c += ((uint32) k[10] << 8);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 24);
                /* the lowest byte of c is reserved for the length */
                /* fall through */
            case 8:
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 8);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 16);
                /* fall through */
            case 5:
                b += ((uint32) k[4] << 24);
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 8);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 16);
                /* fall through */
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                           /* !WORDS_BIGENDIAN */
        switch (len)
        {
            case 11:
                c += ((uint32) k[10] << 24);
                /* fall through */
            case 10:
                c += ((uint32) k[9] << 16);
                /* fall through */
            case 9:
                c += ((uint32) k[8] << 8);
                /* the lowest byte of c is reserved for the length */
                /* fall through */
            case 8:
                b += ka[1];
                a += ka[0];
                break;
            case 7:
                b += ((uint32) k[6] << 16);
                /* fall through */
            case 6:
                b += ((uint32) k[5] << 8);
                /* fall through */
            case 5:
                b += k[4];
                /* fall through */
            case 4:
                a += ka[0];
                break;
            case 3:
                a += ((uint32) k[2] << 16);
                /* fall through */
            case 2:
                a += ((uint32) k[1] << 8);
                /* fall through */
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif   /* WORDS_BIGENDIAN */
    }
    else
    {
        /* Code path for non-aligned source data */

        /* handle most of the key */
        while (len >= 12)
        {
#ifdef WORDS_BIGENDIAN
            a += (k[3] + ((uint32) k[2] << 8) + ((uint32) k[1] << 16) + ((uint32) k[0] << 24));
            b += (k[7] + ((uint32) k[6] << 8) + ((uint32) k[5] << 16) + ((uint32) k[4] << 24));
            c += (k[11] + ((uint32) k[10] << 8) + ((uint32) k[9] << 16) + ((uint32) k[8] << 24));
#else                           /* !WORDS_BIGENDIAN */
            a += (k[0] + ((uint32) k[1] << 8) + ((uint32) k[2] << 16) + ((uint32) k[3] << 24));
            b += (k[4] + ((uint32) k[5] << 8) + ((uint32) k[6] << 16) + ((uint32) k[7] << 24));
            c += (k[8] + ((uint32) k[9] << 8) + ((uint32) k[10] << 16) + ((uint32) k[11] << 24));
#endif   /* WORDS_BIGENDIAN */
            mix(a, b, c);
            k += 12;
            len -= 12;
        }

        /* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
        switch (len)            /* all the case statements fall through */
        {
            case 11:
                c += ((uint32) k[10] << 8);
            case 10:
                c += ((uint32) k[9] << 16);
            case 9:
                c += ((uint32) k[8] << 24);
                /* the lowest byte of c is reserved for the length */
            case 8:
                b += k[7];
            case 7:
                b += ((uint32) k[6] << 8);
            case 6:
                b += ((uint32) k[5] << 16);
            case 5:
                b += ((uint32) k[4] << 24);
            case 4:
                a += k[3];
            case 3:
                a += ((uint32) k[2] << 8);
            case 2:
                a += ((uint32) k[1] << 16);
            case 1:
                a += ((uint32) k[0] << 24);
                /* case 0: nothing left to add */
        }
#else                           /* !WORDS_BIGENDIAN */
        switch (len)            /* all the case statements fall through */
        {
            case 11:
                c += ((uint32) k[10] << 24);
            case 10:
                c += ((uint32) k[9] << 16);
            case 9:
                c += ((uint32) k[8] << 8);
                /* the lowest byte of c is reserved for the length */
            case 8:
                b += ((uint32) k[7] << 24);
            case 7:
                b += ((uint32) k[6] << 16);
            case 6:
                b += ((uint32) k[5] << 8);
            case 5:
                b += k[4];
            case 4:
                a += ((uint32) k[3] << 24);
            case 3:
                a += ((uint32) k[2] << 16);
            case 2:
                a += ((uint32) k[1] << 8);
            case 1:
                a += k[0];
                /* case 0: nothing left to add */
        }
#endif   /* WORDS_BIGENDIAN */
    }

    final(a, b, c);

    /* report the result */
    return (uint32)c;
}


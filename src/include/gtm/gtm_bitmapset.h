#ifndef GTM_BITMAPSET_H
#define GTM_BITMAPSET_H

/*
 *  * Data representation
 *   */

/* The unit size can be adjusted by changing these three declarations: */
#define GTM_BITS_PER_BITMAPWORD 32
typedef uint32 gtm_bitmapword;      /* must be an unsigned type */
typedef int32 gtm_signedbitmapword; /* must be the matching signed type */

typedef struct gtm_Bitmapset
{
    int         nwords;         /* number of words in array */
    gtm_bitmapword  words[1];       /* really [nwords] */
} gtm_Bitmapset;                    /* VARIABLE LENGTH STRUCT */

extern gtm_Bitmapset *gtm_bms_copy(const gtm_Bitmapset *a);
extern gtm_Bitmapset *gtm_bms_make_singleton(int x);
extern void gtm_bms_free(gtm_Bitmapset *a);
extern int  gtm_bms_num_members(const gtm_Bitmapset *a);
extern gtm_Bitmapset *gtm_bms_add_member(gtm_Bitmapset *a, int x);
extern gtm_Bitmapset *gtm_bms_del_member(gtm_Bitmapset *a, int x);
extern gtm_Bitmapset *gtm_bms_del_members(gtm_Bitmapset *a, const gtm_Bitmapset *b);
extern bool gtm_bms_is_empty(const gtm_Bitmapset *a);
extern int  gtm_bms_first_member(gtm_Bitmapset *a);
extern void  gtm_bms_reset(gtm_Bitmapset *a);

#endif   /* GTM_BITMAPSET_H */

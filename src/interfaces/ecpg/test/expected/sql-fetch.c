/* Processed by ecpg (regression mode) */
/* These include files are added by the preprocessor */
#include <ecpglib.h>
#include <ecpgerrno.h>
#include <sqlca.h>
/* End of automatic include section */
#define ECPGdebug(X,Y) ECPGdebug((X)+100,(Y))

#line 1 "fetch.pgc"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#line 1 "regression.h"






#line 5 "fetch.pgc"


int main() {
  /* exec sql begin declare section */
     
      
  
#line 9 "fetch.pgc"
 char str [ 25 ] ;
 
#line 10 "fetch.pgc"
 int i , count = 1 ;
/* exec sql end declare section */
#line 11 "fetch.pgc"


  ECPGdebug(1, stderr);
  { ECPGconnect(__LINE__, 0, "regress1" , NULL, NULL , NULL, 0); }
#line 14 "fetch.pgc"


  /* exec sql whenever sql_warning  sqlprint ; */
#line 16 "fetch.pgc"

  /* exec sql whenever sqlerror  sqlprint ; */
#line 17 "fetch.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "create table My_Table ( Item1 int , Item2 text )", ECPGt_EOIT, ECPGt_EORT);
#line 19 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 19 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 19 "fetch.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "insert into My_Table values ( 1 , 'text1' )", ECPGt_EOIT, ECPGt_EORT);
#line 21 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 21 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 21 "fetch.pgc"

  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "insert into My_Table values ( 2 , 'text2' )", ECPGt_EOIT, ECPGt_EORT);
#line 22 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 22 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 22 "fetch.pgc"

  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "insert into My_Table values ( 3 , 'text3' )", ECPGt_EOIT, ECPGt_EORT);
#line 23 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 23 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 23 "fetch.pgc"

  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "insert into My_Table values ( 4 , 'text4' )", ECPGt_EOIT, ECPGt_EORT);
#line 24 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 24 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 24 "fetch.pgc"


  /* declare C cursor for select * from My_Table order by 1 , 2 */
#line 26 "fetch.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "declare C cursor for select * from My_Table order by 1 , 2", ECPGt_EOIT, ECPGt_EORT);
#line 28 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 28 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 28 "fetch.pgc"


  /* exec sql whenever not found  break ; */
#line 30 "fetch.pgc"

  while (1) {
  	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch 1 in C", ECPGt_EOIT, 
	ECPGt_int,&(i),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_char,(str),(long)25,(long)1,(25)*sizeof(char), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EORT);
#line 32 "fetch.pgc"

if (sqlca.sqlcode == ECPG_NOT_FOUND) break;
#line 32 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 32 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 32 "fetch.pgc"

	printf("%d: %s\n", i, str);
  }

  /* exec sql whenever not found  continue ; */
#line 36 "fetch.pgc"

/* Move backward is not supported by PGXC */
/*
  EXEC SQL MOVE BACKWARD 2 IN C;
*/

  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch $0 in C", 
	ECPGt_int,&(count),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, 
	ECPGt_int,&(i),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_char,(str),(long)25,(long)1,(25)*sizeof(char), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EORT);
#line 42 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 42 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 42 "fetch.pgc"

  printf("%d: %s\n", i, str);

  /* declare D cursor for select * from My_Table where Item1 = $1 order by 1 , 2 */
#line 45 "fetch.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "declare D cursor for select * from My_Table where Item1 = $1 order by 1 , 2", 
	ECPGt_const,"1",(long)1,(long)1,strlen("1"), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EOIT, ECPGt_EORT);
#line 47 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 47 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 47 "fetch.pgc"


  /* exec sql whenever not found  break ; */
#line 49 "fetch.pgc"

  while (1) {
  	{ ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "fetch 1 in D", ECPGt_EOIT, 
	ECPGt_int,&(i),(long)1,(long)1,sizeof(int), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, 
	ECPGt_char,(str),(long)25,(long)1,(25)*sizeof(char), 
	ECPGt_NO_INDICATOR, NULL , 0L, 0L, 0L, ECPGt_EORT);
#line 51 "fetch.pgc"

if (sqlca.sqlcode == ECPG_NOT_FOUND) break;
#line 51 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 51 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 51 "fetch.pgc"

	printf("%d: %s\n", i, str);
  }
  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "close D", ECPGt_EOIT, ECPGt_EORT);
#line 54 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 54 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 54 "fetch.pgc"


  { ECPGdo(__LINE__, 0, 1, NULL, 0, ECPGst_normal, "drop table My_Table", ECPGt_EOIT, ECPGt_EORT);
#line 56 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 56 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 56 "fetch.pgc"


  { ECPGdisconnect(__LINE__, "ALL");
#line 58 "fetch.pgc"

if (sqlca.sqlwarn[0] == 'W') sqlprint();
#line 58 "fetch.pgc"

if (sqlca.sqlcode < 0) sqlprint();}
#line 58 "fetch.pgc"


  return 0;
}

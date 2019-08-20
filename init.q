/for linux
if[not .z.o in`w32`w64;system "l /home/quser/db_script/dblib.q";system "l /home/quser/db_script/dbmaint.q"; system "l ."];

/  for windows
if[.z.o in`w32`w64;system "l d:/db_script/dblib.q";system "l d:/db_script/dbmaint.q";system "l ."];

/  delete from `.
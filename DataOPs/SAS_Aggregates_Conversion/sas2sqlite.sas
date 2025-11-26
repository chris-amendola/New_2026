%macro sas2sqlite( sastable = 
				  ,path = 
				  ,database = );
				  
   /*
    *  PARAMETERS: sastable  = dataset in SAS for SQLite
    *              path      = destinate file path for SQLite database
    *              database  = name of SQLite database
    */
	
   /*Create a date_time stamp value for the temp files being created for uniqueness*/
   %let DateJul   = %sysfunc(date(),julian7.)    ;
   %let Time      = %sysfunc(time(),time8.0)     ;
   %let Time_HH   = %scan(&Time.,1,:)            ;
   %let Time_MM   = %scan(&Time.,2,:)            ;
   %let Time_SS   = %scan(&Time.,3,:)            ;
   
   %let _stamp=%trim(&DateJul)%trim(&Time_HH)%trim(&Time_MM)%trim(&Time_SS);
   
   
   proc export data = &sastable 
            outfile = "&path\tmp&_stamp..txt" 
               dbms = tab 
               replace;
   run;
   
   data _null_;
      infile "&path\tmp&_stamp..txt";
      file "&path\sas_2_sqlite&_stamp..txt";
      input;
      if _n_ gt 1 then put _infile_;
   run;
   
   options noxwait noxsync;
   x "del &path\tmp&_stamp..txt";

   ods listing close;
   ods output variables = _varlist;
   proc contents data = &sastable; 
   run;
   proc sort data = _varlist;
      by num;
   run;
   ods listing;

   data _tmp01; 
      set _varlist;
      if lowcase(type) = 'num' then vartype = 'real';
      else if lowcase(type) = 'char' then vartype = 'text';
   run;
   proc sql noprint;
      select trim(variable) ||' '|| trim(vartype) 
            into: table_value separated by ', '
      from _tmp01
   ;quit;

   proc sql;
      create table _tmp02 (string char(800));
      insert into _tmp02  
        set string = '.stats on'
        set string = 'create table sas_table(sas_table_value);'
        set string = '.separator "\t"'
        set string = ".import 'sas_path\sas_2_sqlite&_stamp..txt' sas_table"
		set string = ".exit"
		set string = ""
   ;quit;

   data _tmp03;
      set _tmp02;
        string = tranwrd(string, "sas_table_value", "&table_value");
        string = tranwrd(string, "sas_table", "&sastable");
        string = tranwrd(string, "sas_path", "&path");
   run;
   data _null_;
     set _tmp03;
     file "&path\sas_2_sqlite&_stamp..sql";
     put string;
   run;
   x "sqlite3 -init &path\sas_2_sqlite&_stamp..sql &path\&database .exit";
 
   proc datasets nolist;
      delete _:;
   quit;
%mend;
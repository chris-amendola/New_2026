/*Create a date_time stamp value for the temp files being created for uniqueness*/
   %let _tstamp=%sysfunc(datepart(datetime))%sysfunc(timepart(datetime));
   %put &_tstamp;

/* BUILD A MACRO FROM SAS TO SQLITE */
%macro sas2sqlite( src_lib=
                  ,sastable = 
				  ,path = 
				  ,database = );
				  
   /*
    *  PARAMETERS: src_lib   = SAS library for source dataset
	*              sastable  = dataset in SAS for SQLite
    *              path      = destinate file path for SQLite database
    *              database  = name of SQLite database
    */
   /*Create a date_time stamp value for the temp files being created for uniqueness*/
   %let _tstamp=%sysfunc(datepart(datetime()))%sysfunc(timepart(datetime()));
     
   proc export data = &sastable outfile = "&path\tmp.txt" dbms = tab 
               repalce;
   run;
   data _null_;
      infile "&path\tmp.txt";
      file "&path\sas_2_sqlite.txt";
      input;
      if _n_ gt 1 then put _infile_;
   run;
   options noxwait noxsync;
   x "del &path\tmp.txt";

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
      set string = ".import 'sas_path\sas_2_sqlite.txt' sas_table"
   ;quit;

   data _tmp03;
      set _tmp02;
      string = tranwrd(string, "sas_table_value", "&table_value");
      string = tranwrd(string, "sas_table", "&sastable");
      string = tranwrd(string, "sas_path", "&path");
   run;
   data _null_;
     set _tmp03;
     file "&path\sas_2_sqlite.sql";
     put string;
   run;
   x "sqlite3 -init &path\sas_2_sqlite.sql &path\&database .exit";
 
   proc datasets nolist;
      delete _:;
   quit;
%mend;

libname out "\\apswp2286\w\camendol\SPC\data\";
data temp;
  set out.phm_summ_prh01_pd20200331;
run;

%sas2sqlite(sastable = temp, path = \\apswp2286\W\camendol\SPC\data, database = testing.sqlite3);
%mend hold;
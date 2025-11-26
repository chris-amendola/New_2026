options sasautos=(sasautos
                 '\\nasv9112.uhc.com\Prod\trunk\SASApps\Macros\Prod'
				 '\\nasv9112.uhc.com\Imp\Users\camendol\SAS_ETL_dev\support_lib'
				 '\\nasv9112.uhc.com\Imp\Users\camendol\DataOPs\DCM\SAS_Aggregates_Conversion'
                 '!sasfolder\sasmacro'
                 );

%let client=xnc;
%let data_type=;
%let cycle=20210331;
%let raw_base=\\apswp2287\Q\Data_Warehouse\BCBSNC\PD20210331\base;

%let src_contract=plan_source;

%let med_keep_list =  plan_source bill_prv bill_type cap_ind clm_head covclass denied_flg diag1 diag2 diag3 diag4 diag5
                      diag6 diag7 diag8 diag9 diag10 dis_stat drg drg_level drg_level_payer drg_n_payer drg_outlier drg_payer drg_version drg_version_payer icd_proc1 
                      icd_proc2 icd_proc3 icd_proc4 icd_proc5 icd_proc6 icd_proc7 icd_proc8 icd_proc9 icd_proc10 icd_type invasive map_source
                      network_status order_prv poa_n pos proc_code proc_mod proctype pseudo_flg refer_prv revenue revtype secondary_flg serv_affil 
                      serv_prv serv_prv_spec1 serv_prv_type source_file tos; 

%let phm_keep_list =  clm_source plan_source covclass dea_num denied_flg formulary_ind gbo_ind map_source ndc
                      network_status pharmacy_name phm_type_n pres_prv
                      pseudo_flg secondary_flg serv_prv source_file;	  
 
/*Internal Functions*/
%macro freq_summ( table=
                 ,column_list=
				 ,by_var=
				 );
				 
    %local _enum;			
			
    %let sql_statement=;
	%let _enum=0;
	
	%macro sql_freq_st(column);
	
	    %if &_enum>0 %then %let sql_statement=&sql_statement.   UNION ALL ;
		%let _enum=%eval(&_enum+1);
		
		%let sql_statement=&sql_statement. SELECT &by_var,"&column." as variable,"Missing" as value,count(*) as freq FROM &table. WHERE &column. IS NULL GROUP BY &by_var.
                            UNION ALL
                            SELECT &by_var.,"&column." as variable,&column. as value,count(&column.) as freq FROM &table. WHERE &column. IS NOT NULL GROUP BY &column.,&by_var;
				
	%mend  sql_freq_st;

     	
	%mac_map(sql_freq_st,to_list=column_list)
 
    &sql_statement.

%mend freq_summ;

libname &client._base "&raw_base.";	

/* RX SERVICES FROM BASE */
%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_phm.{3}_20|&client._cur_phm_20);

/* Appending all monthly client source file into one dataset */
data _null_(keep=dset);
  set work.file_list;
  attrib append_code        length=$15000
         merge_adjust_code  length=$15000;
  
  put _all_;
  
  dset=scan(memname,1,'.');
  
  dsid=open("&client._base."!!dset);
  _obs=attrn(dsid,'nobs');
  
  /* Construct adjustment dataset name from current file name*/
  /*Break up the current filename by underscores*/
  dsid_c1=scan(dset,1,'_');
  dsid_c2=scan(dset,2,'_');
  dsid_c3=scan(dset,3,'_');
  dsid_c4=scan(dset,4,'_');
  dsid_c5=scan(dset,5,'_');
  
  const_filename=catx('_',dsid_c1,dsid_c2,dsid_c3,dsid_c4,dsid_c5);
  /*put const_filename;*/
  
  /* 'Insert' 'adj' in the current filename to create adjustment file name*/
  adj_filename=catx('_',dsid_c1,dsid_c2,dsid_c3,'adj',dsid_c4,dsid_c5);
  /*put adj_filename;*/
  
  append_code='';
  merge_adjust_code='';
  
  if _obs>0 then do;
      if exist("&client._base."!!adj_filename) then do;
	    merge_adjust_code='proc sql; create table _adj_merge_ as  select cur.*, adj.* from &client._base.'!!adj_filename!!' as adj
						   join &client._base.'!!strip(dset)!!' as cur on adj.row_id  = cur.row_id; quit;';
      end;
      else do;
	    put 'EXCEPTION - FAIL';
		put 'Populated Service Lines files missing corresponding adjustment file!';
		abort cancel;
      end;	  
      append_code="proc append base=work._temp_ data= _adj_merge_(keep=row_id &phm_keep_list.) force ; run;";
	  call execute(merge_adjust_code);
	  call execute(append_code);
  end;
  
  rc=close(dsid);
run; 

/* Field Level aggregation */ 
proc sql;
   create table phm_fields as
   %freq_summ( table=work._temp_ 
              ,column_list=&phm_keep_list.
		      ,by_var=&src_contract.);
quit;

proc print data=phm_fields (obs=25);
proc contents data=phm_fields;
run;

%sas2sqlite( sastable = phm_fields
            ,path = \\apswp2286\W\camendol\SPC\data
		 ,database = &client._base_&cycle._summ.sqlite);

/* MED SERVICES FROM BASE */
%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_med.{3}_20);

/* Appending all monthly client source file into one dataset */
data _null_(keep=dset);
  set work.file_list;
  attrib append_code        length=$15000
         merge_adjust_code  length=$15000;
  
  dset=scan(memname,1,'.');
  
  dsid=open("&client._base."!!dset);
  _obs=attrn(dsid,'nobs');
  
  /* Construct adjustment dataset name from current file name*/
  /*Break up the current filename by underscores*/
  dsid_c1=scan(dset,1,'_');
  dsid_c2=scan(dset,2,'_');
  dsid_c3=scan(dset,3,'_');
  dsid_c4=scan(dset,4,'_');
  dsid_c5=scan(dset,5,'_');
  
  const_filename=catx('_',dsid_c1,dsid_c2,dsid_c3,dsid_c4,dsid_c5);
  /*put const_filename;*/
  
  /* 'Insert' 'adj' in the current filename to create adjustment file name*/
  adj_filename=catx('_',dsid_c1,dsid_c2,dsid_c3,'adj',dsid_c4,dsid_c5);
  /*put adj_filename;*/
  
  append_code='';
  merge_adjust_code='';
  
  if _obs>0 then do;
      if exist("&client._base."!!adj_filename) then do;
	    merge_adjust_code='proc sql; create table _adj_merge_ as  select cur.*, adj.* from &client._base.'!!adj_filename!!' as adj
						   join &client._base.'!!strip(dset)!!' as cur on adj.row_id  = cur.row_id; quit;';
      end;
      else do;
	    put 'EXCEPTION - FAIL';
		put 'Populated Service Lines files missing corresponding adjustment file!';
		abort cancel;
      end;	  
      append_code="proc append base=work._temp_ data= _adj_merge_(keep=&med_keep_list.) force ; run;";
	  call execute(merge_adjust_code);
	  call execute(append_code);
  end;
  
  rc=close(dsid);
run; 
 
/* Field Level aggregation */
proc sql;
   create table med_fields as
   select * from (
   %freq_summ( table=work._temp_ 
              ,column_list=&med_keep_list.
		      ,by_var=&src_contract.));
quit;

proc print data=med_fields (obs=25);
run;

%sas2sqlite( sastable = med_fields
            ,path = \\apswp2286\W\camendol\SPC\data
		    ,database = &client._base_&cycle._summ.sqlite);
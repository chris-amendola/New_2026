options sasautos=(sasautos
                 '\\nasv9112.uhc.com\Prod\trunk\SASApps\Macros\Prod'
				 '\\nasv9112.uhc.com\Imp\Users\camendol\SAS_ETL_dev\support_lib'
				 '\\nasv9112.uhc.com\Imp\Users\camendol\DataOPs\DCM\SAS_Aggregates_Conversion'
                 '!sasfolder\sasmacro'
                 );

%let client=ful;
%let cont=.{3};
%let cycle=20210228;
%let raw_base=\\apswp2287\M\Data_Warehouse\Fullwell\PD20210228\base;
	
%let src_contract=plan_source;
%let mem_keep_list=&src_contract. member eff_dt end_dt;
%let med_keep_list=member pay_dt serv_dt from_dt to_dt amt_req amt_all amt_pay qty adj_stat1 adj_stat2 adj_stat3 &src_contract.;
%let phm_keep_list=member pay_dt serv_dt met_qty amt_req amt_all amt_pay adj_stat1 adj_stat2 adj_stat3 &src_contract.;

libname &client._base "&raw_base.";	

/* RX SERVICES FROM BASE */
%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_phm&cont._20);

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
      append_code="proc append base=work._temp_ data= _adj_merge_(keep=&phm_keep_list.) force ; run;";
	  call execute(merge_adjust_code);
	  call execute(append_code);
  end;
  
  rc=close(dsid);
run;  

/* Volumetrics aggregation */
proc sql;
    create table servicerx as
      select put(MONTH(serv_dt),2.) as Mon
            ,put(YEAR(serv_dt),4.) as Yr
			,adj_stat1
			,adj_stat2
			,adj_stat3
		    ,count(*) as numrows_sum
		    ,count(distinct member) as members_sum
		    ,sum(amt_pay) as amt_pay_sum
		    ,sum(amt_req) as amt_req_sum
		    ,sum(amt_all) as amt_eqv_sum
		    ,sum(met_qty) as met_qty_sum
		    ,&src_contract. as contract  
	  from work._temp_ 
	  group by Yr
	       ,Mon
		   ,adj_stat1
		   ,adj_stat2
		   ,adj_stat3
		   ,&src_contract.
           		   
	   order by Yr
	       ,Mon
		   ,adj_stat1
		   ,adj_stat2
		   ,adj_stat3
		   ,&src_contract. ;			 
  quit;

  data servicerx;
    set servicerx;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;

  %sas2sqlite( sastable = servicerx
              ,path = \\apswp2286\W\camendol\SPC\data
			  ,database = &client._base_&cycle._summ.sqlite);
  
  proc print data=servicerx (obs=25);
  proc contents data=servicerx;
  run;
  proc sql;
    drop table _adj_merge_;
	drop table work._temp_;
	drop table work.file_list;
  quit;	

/* MED SERVICES FROM BASE */
%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_med&cont._20);

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
/* Volumetrics aggregation */
proc sql;
    create table servicemed as
      select put(MONTH(serv_dt),2.) as Mon
            ,put(YEAR(serv_dt),4.) as Yr
			,adj_stat1
			,adj_stat2
			,adj_stat3
		    ,count(*) as numrows_sum
		    ,count(distinct member) as members_sum
		    ,sum(amt_pay) as amt_pay_sum
		    ,sum(amt_req) as amt_req_sum
		    ,sum(amt_all) as amt_eqv_sum
		    ,sum(qty) as qty_sum
		    ,&src_contract. as contract  
	  from work._temp_ 
	  group by Yr
	       ,Mon
		   ,adj_stat1
			,adj_stat2
			,adj_stat3
		   ,&src_contract.   
	   order by Yr
	       ,Mon
		   ,adj_stat1
			,adj_stat2
			,adj_stat3
		   ,&src_contract.  ;			 
  quit;

  data servicemed;
    set servicemed;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;

  proc print data=servicemed (obs=25);
  run;

  %sas2sqlite( sastable = servicemed
              ,path = \\apswp2286\W\camendol\SPC\data
			  ,database = &client._base_&cycle._summ.sqlite);
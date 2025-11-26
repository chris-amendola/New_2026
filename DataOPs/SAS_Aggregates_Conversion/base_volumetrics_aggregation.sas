options sasautos=(sasautos
                 '\\nasgw8315pn.ihcis.local\Prod\trunk\SASApps\Macros\Prod'
				 '\\NASGW8315PN.ihcis.local\Imp\Users\camendol\SAS_ETL_dev\support_lib'
				 '\\NASGW8315PN\Imp\Users\camendol\DataOPs\DCM\SAS_Aggregates_Conversion'
                 '!sasfolder\sasmacro'
                 );

%let client=ucd;
%let data_type=;
%let cycle=20200731;
%let raw_base=G:\data_warehouse\UC_Davis_OPA\PD20200731\base;
	

%let src_contract=plan_source;
%let mem_keep_list=&src_contract. member eff_dt end_dt;
%let med_keep_list=member pay_dt serv_dt from_dt to_dt amt_req amt_all amt_pay qty &src_contract.;
%let phm_keep_list=member pay_dt serv_dt met_qty amt_req amt_all amt_pay &src_contract.;

libname &client._base "&raw_base.";	


%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_elg.{3}_span);

/* Appending all client source files into one dataset */
data _null_(keep=dset);
  set work.file_list;
  
  dset=scan(memname,1,'.');
  
  dsid=open("&client._base."!!dset);
  _obs=attrn(dsid,'nobs');
  
  if _obs>0 then do;
      append_code="proc append base=work._temp_ data=&client._base."!!strip(dset)!!"(keep=&mem_keep_list.) force ; run;";
	  call execute(append_code);
  end;
  
  rc=close(dsid);
run; 

data work.project_spans_months( keep=&src_contract.
                                     member 
                                     eff_dt 
									 end_dt
                                  drop=span_beg_dt 
                                      span_end_dt 
                                      effective_date 
                                      term_date);
    set work._temp_(rename=( eff_dt=effective_date 
	                         end_dt=term_date));

    format eff_dt 
           end_dt 
           span_beg_dt 
           span_end_dt yymmdd10.;

    span_beg_dt=intnx('month',effective_date,0,'s');
    span_end_dt=intnx('month',term_date,0,'s');
    /*Maybe limit span end date to current date?*/
    if span_end_dt >today() then span_end_dt=intnx('month',today(),0,'s');
  
    eff_dt=span_beg_dt;
    end_dt=intnx('month',eff_dt,0,'e');
    output;
  
    do while (end_dt < span_end_dt);
   
      eff_dt=intnx('month',eff_dt,1,'s');
      end_dt=intnx('month',eff_dt,0,'e');
      output;
    
    end;
run;	
	
	proc sql;
	  create table member as
	    select &src_contract. as contract 
              ,put(MONTH(eff_dt),2.) as Mon
              ,put(YEAR(eff_dt),4.) as Yr
        ,count(member) as mmos_sum
        ,count(distinct member) as members_sum
          from work.project_spans_months
          group by  Mon
                   ,Yr
				   ,&src_contract.
          order by  Yr
                   ,Mon
				   ,&src_contract.; 
	quit;
	
	data member;
      set member;
      array change _numeric_;
        do over change;
            if change=. then change=0;
        end;
    run;
	
    proc print data=member(obs=25);
	run;
	
	%sas2sqlite( sastable = member
              ,path = \\apswp2286\W\camendol\SPC\data
			  ,database = &client._base_&cycle._summ.sqlite); 			   

/* RX SERVICES FROM BASE */
%get_filenames( &raw_base.
               ,metadata=work.file_list
               ,filter_regex=&client._cur_phm.{3}_20);

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
		   ,&src_contract. 
	   order by Yr
	       ,Mon
		   ,&src_contract. ;			 
  quit;

  data servicerx;
    set servicerx;
    array change _numeric_;
    do over change;
      if change=. then change=0;
    end;
  run;

  proc print data=servicerx (obs=25);
  proc contents data=servicerx;
  run;

  %sas2sqlite( sastable = servicerx
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
/* Volumetrics aggregation */
proc sql;
    create table servicemed as
      select put(MONTH(serv_dt),2.) as Mon
            ,put(YEAR(serv_dt),4.) as Yr
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
		   ,&src_contract.   
	   order by Yr
	       ,Mon
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

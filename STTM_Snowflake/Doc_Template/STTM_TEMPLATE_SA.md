### Document Metadata

| Field                  | Description                                                |
| ---------------------- | ---------------------------------------------------------- |
| Mapping Document ID    | STTM0001                                                   |
| Mapping Name           | Example gGastro Mapping                                    |
| Domain                 | Finance                                                    |
| Source System(s)       | gGastro-Modmed                                             |
| Target System          | STAR                                                       |
| Target Model           | DIM_MODEL                                                  |                                    
| Architect              |                                                            |
| Analyst                |                                                            |
| Reviewer(s)            |                                                            |
| Effective Date         | {Date mapping becomes active}                              |
| Version                | Semantic version (e.g., 2.1.0)                             |
| Change Type            | New / Enhancement / Bug Fix                                |
| Approval Status        | Draft / Approved / Deprecated                              |

#### Source System Overview

|Field|Description|
|---|---|
|Source System Name|gGastro|
|Vendor|Modmed|
|Module / Subsystem||
|Source Data Owner||
|Source Update Frequency||
|Data Latency|Typical / worst case|
|Correction Behavior|Overwrite / Append / Void-Replace|
|Historical Availability||
|Known Data Quality Issues||

#### 2.2 Source Table Inventory

|Source Schema|Source Table|Description|Row Grain|
|-------------|------------|-----------|---------|
|GGASTRO|STG_APPOINTMENT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGADJUSTMENTCODE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCOSTCENTER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULEPRICEPERPROVIDER|{description}|{row_grain}|
|GGASTRO|STG_CPTCODE|{description}|{row_grain}|
|GGASTRO|STG_GUARANTOR|{description}|{row_grain}|
|GGASTRO|STG_LOCATION|{description}|{row_grain}|
|GGASTRO|STG_L_LOCATION_PEORIA|{description}|{row_grain}|
|GGASTRO|STG_REFERRINGPHYSICIAN|{description}|{row_grain}|
|GGASTRO|STG_INTERVENTION|{description}|{row_grain}|
|GGASTRO|STG_MEDICATION|{description}|{row_grain}|
|GGASTRO|STG_RECALL|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGETRANSACTIONADJUSTMENTDETAIL_BAD|{description}|{row_grain}|
|GGASTRO|STG_INSURANCE|{description}|{row_grain}|
|GGASTRO|STG_PATIENTDIAGNOSIS|{description}|{row_grain}|
|GGASTRO|STG_TASK|{description}|{row_grain}|
|GGASTRO|STG_TDDC_LOAD|{description}|{row_grain}|
|GGASTRO|STG_ANALYTICS_PROVIDER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGETRANSACTIONCOLLECTIONADJUSTMENT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCLAIM|{description}|{row_grain}|
|GGASTRO|STG_BILLINGINSURANCEPAYERGROUP|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPATIENTAUTHORIZATION|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILLIMPORTPERSONRACE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILLPROCEDURE|{description}|{row_grain}|
|GGASTRO|STG_EDW_COUNTRY|{description}|{row_grain}|
|GGASTRO|STG_FINDING|{description}|{row_grain}|
|GGASTRO|STG_PHONE|{description}|{row_grain}|
|GGASTRO|STG_PROCEDUREINTERRUPTED|{description}|{row_grain}|
|GGASTRO|STG_RESOURCEPERAPPOINTMENT|{description}|{row_grain}|
|GGASTRO|STG_RVU|{description}|{row_grain}|
|GGASTRO|STG_SERVICETIMEMARKER|{description}|{row_grain}|
|GGASTRO|STG_STAFF|{description}|{row_grain}|
|GGASTRO|STG_TASKNOTES|{description}|{row_grain}|
|GGASTRO|STG_TESTEESTRESTSETTEST|{description}|{row_grain}|
|GGASTRO|STG_APPOINTMENTSET|{description}|{row_grain}|
|GGASTRO|STG_APPOINTMENTSTATUSHISTORY|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCOSTCENTERASSIGNEDTOPROVIDER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILLPROCEDUREMODIFIER|{description}|{row_grain}|
|GGASTRO|STG_MEDICALHISTORYREVIEW|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILLDIAGNOSIS|{description}|{row_grain}|
|GGASTRO|STG_BOD_REGIONALMAP|{description}|{row_grain}|
|GGASTRO|STG_EDW_EMAIL|{description}|{row_grain}|
|GGASTRO|STG_FWK_AUDITLOG|{description}|{row_grain}|
|GGASTRO|STG_INTERFACEMAPPING|{description}|{row_grain}|
|GGASTRO|STG_INTERFACERESULT|{description}|{row_grain}|
|GGASTRO|STG_PATIENT|{description}|{row_grain}|
|GGASTRO|STG_PATIENTTEST|{description}|{row_grain}|
|GGASTRO|STG_PERSON|{description}|{row_grain}|
|GGASTRO|STG_POSTING_AND_ EDITRULES|{description}|{row_grain}|
|GGASTRO|STG_SERVICE|{description}|{row_grain}|
|GGASTRO|STG_EMAIL|{description}|{row_grain}|
|GGASTRO|STG_IGG_PEORIA_ENCOUNTERS|{description}|{row_grain}|
|GGASTRO|STG_SERVICETIMEMARKERTYPE|{description}|{row_grain}|
|GGASTRO|STG_VITALSIGNS|{description}|{row_grain}|
|GGASTRO|STG_ARK_MEDICATIONRECONCILLATION_2022|{description}|{row_grain}|
|GGASTRO|STG_BILLINGENCOUNTEREVENT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILLDIAGNOSISPERPROCEDURE|{description}|{row_grain}|
|GGASTRO|STG_BLOODPRESSURE|{description}|{row_grain}|
|GGASTRO|STG_PROVIDERPERPATIENT|{description}|{row_grain}|
|GGASTRO|STG_USAADDRESS|{description}|{row_grain}|
|GGASTRO|STG_IGG_MEDICATIONRECONCILLATION_2022|{description}|{row_grain}|
|GGASTRO|STG_INSURANCEPROVIDER|{description}|{row_grain}|
|GGASTRO|STG_INSURANCEPROVIDERCPTCODE|{description}|{row_grain}|
|GGASTRO|STG_INTERFACETEST|{description}|{row_grain}|
|GGASTRO|STG_ORDER|{description}|{row_grain}|
|GGASTRO|STG_RAM|{description}|{row_grain}|
|GGASTRO|STG_TASKTYPE|{description}|{row_grain}|
|GGASTRO|STG_PHYSICALMEASUREMENT|{description}|{row_grain}|
|GGASTRO|STG_TDDC_CHANGEREPORT_1|{description}|{row_grain}|
|GGASTRO|STG_ACTIVITY|{description}|{row_grain}|
|GGASTRO|STG_BILLINGADJUSTMENTCODETYPE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGANESTHESIASERVICETYPE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULEPRICEPERINSURANCEPROVIDER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULEPRICEPERPROCEDURECODE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPATIENTBILLINGINFORMATION|{description}|{row_grain}|
|GGASTRO|STG_CLAIM_RESPONSIBLE|{description}|{row_grain}|
|GGASTRO|STG_CODINGADVISORMDMTIMEMODE|{description}|{row_grain}|
|GGASTRO|STG_FWK_USERDETAIL|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCLAIMEVENT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGELIGIBILITYFAILUREREASON|{description}|{row_grain}|
|GGASTRO|STG_BILLINGENCOUNTEREVENTTASKRECIPIENT|{description}|{row_grain}|
|GGASTRO|STG_DHAT_DATA_LOAD_TEST_2021|{description}|{row_grain}|
|GGASTRO|STG_FINDINGDESCRIPTION|{description}|{row_grain}|
|GGASTRO|STG_FWK_LIST|{description}|{row_grain}|
|GGASTRO|STG_IGH_MEDICATIONRECONCILLATION_2022|{description}|{row_grain}|
|GGASTRO|STG_INTERFACERESULTPATHOLOGY|{description}|{row_grain}|
|GGASTRO|STG_PHARMACY|{description}|{row_grain}|
|GGASTRO|STG_RAMNEWSTORAGE|{description}|{row_grain}|
|GGASTRO|STG_RVU_CPTCODE|{description}|{row_grain}|
|GGASTRO|STG_EDW_RAPT|{description}|{row_grain}|
|GGASTRO|STG_FWK_USER|{description}|{row_grain}|
|GGASTRO|STG_PRESCRIPTIONINBOUNDQUEUE|{description}|{row_grain}|
|GGASTRO|STG_REFERRINGPHYSICIANIDENTIFICATION|{description}|{row_grain}|
|GGASTRO|STG_BILLINGBATCH|{description}|{row_grain}|
|GGASTRO|STG_BILLINGBATCH1|{description}|{row_grain}|
|GGASTRO|STG_BILLINGBATCHTEST|{description}|{row_grain}|
|GGASTRO|STG_BILLINGBUSINESSUNITPERLOCATION|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPAYMENTADJUSTMENTREFUND|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPROCEDURECODE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGTYPEOFSERVICE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGBUSINESSUNIT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGEMODIFIER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGETRANSACTIONADJUSTMENTDETAIL|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCLAIMSTATUS|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULEPRICE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGFEESCHEDULEPRICEPERLOCATION|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPATIENTLEDGER|{description}|{row_grain}|
|GGASTRO|STG_ICD10CODE|{description}|{row_grain}|
|GGASTRO|STG_IGG_PEORIA_CHARGES|{description}|{row_grain}|
|GGASTRO|STG_IMAGINGSERVICE|{description}|{row_grain}|
|GGASTRO|STG_RECALLEVENT|{description}|{row_grain}|
|GGASTRO|STG_TESTEESTRESTSET|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPROCEDURECODEMODIFIER|{description}|{row_grain}|
|GGASTRO|STG_FWK_ROLE|{description}|{row_grain}|
|GGASTRO|STG_INTERFACETESTRESULT|{description}|{row_grain}|
|GGASTRO|STG_SERVICETYPE|{description}|{row_grain}|
|GGASTRO|STG_PROVIDER|{description}|{row_grain}|
|GGASTRO|STG_ADH_HISTORICAL_LOAD|{description}|{row_grain}|
|GGASTRO|STG_BILLINGENCOUNTERDIAGNOSIS|{description}|{row_grain}|
|GGASTRO|STG_BILLINGPAYMENTCODE|{description}|{row_grain}|
|GGASTRO|STG_INSURANCEPROVIDERPLACEOFSERVICE|{description}|{row_grain}|
|GGASTRO|STG_OUTOFOFFICE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGENCOUNTERDIAGNOSISPERCHARGE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGINSURANCEDENIALCATEGORY|{description}|{row_grain}|
|GGASTRO|STG_CODINGADVISOR2021|{description}|{row_grain}|
|GGASTRO|STG_DHAT_DATA_LOAD_TEST_JAN_2021|{description}|{row_grain}|
|GGASTRO|STG_LIMITATIONCOMPLICATION|{description}|{row_grain}|
|GGASTRO|STG_PLACEOFSERVICE|{description}|{row_grain}|
|GGASTRO|STG_RESOURCEPERAPPOINTMENTHOLD|{description}|{row_grain}|
|GGASTRO|STG_L_PHYSICIAN_FF|{description}|{row_grain}|
|GGASTRO|STG_RVUHISTORY|{description}|{row_grain}|
|GGASTRO|STG_APPOINTMENTSTATUS|{description}|{row_grain}|
|GGASTRO|STG_AUDIT_USER_ROLE_CHANGES|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCHARGETRANSACTION|{description}|{row_grain}|
|GGASTRO|STG_BILLINGINSURANCEREASONCODE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGSUPERBILL|{description}|{row_grain}|
|GGASTRO|STG_DHAT_DATA_LOAD_TEST_2020|{description}|{row_grain}|
|GGASTRO|STG_GIQUICEXPORT|{description}|{row_grain}|
|GGASTRO|STG_PATIENTVIEW|{description}|{row_grain}|
|GGASTRO|STG_RECALLTYPE|{description}|{row_grain}|
|GGASTRO|STG_BILLINGCLAIMSNAPSHOT|{description}|{row_grain}|
|GGASTRO|STG_BILLINGENCOUNTER|{description}|{row_grain}|
|GGASTRO|STG_BILLINGINSURANCECATEGORY|{description}|{row_grain}|
|GGASTRO|STG_COUNTRY|{description}|{row_grain}|
|GGASTRO|STG_SERVICEPROCEDUREINDICATION|{description}|{row_grain}|
|GGASTRO|STG_STAFFIDENTIFICATION|{description}|{row_grain}|
|GGASTRO|STG_TEST_CONNECTION|{description}|{row_grain}|
|GGASTRO|STG_EDW_PHONE|{description}|{row_grain}|
|GGASTRO|STG_MATILLION|{description}|{row_grain}|
|GGASTRO|STG_PRESCRIPTION|{description}|{row_grain}|
|GGASTRO|STG_PRESCRIPTIONSAMPLE|{description}|{row_grain}|
|GGASTRO|STG_RESOURCEPERAPPOINTMENT1|{description}|{row_grain}|
|GGASTRO|STG_SERVICESTAFF|{description}|{row_grain}|
|GGASTRO|STG_TEST08272024|{description}|{row_grain}|

|Target Schema|Target Table|Descriptions|Target Grain |Slowly Changing Behavior|
|-------------|------------|------------|-------------|------------------------|
|DIM_MODEL|DIM_APPOINTMENT_STATUS|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_ESS_ATTRIBUTE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_RECALL_TYPE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_PHYS_COMP|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_PHYS_COMP_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BILLING_PAYMENT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PATIENT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_ORGANIZATION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_POSITION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT_BACKUP_LOAD_2026_01_15|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_REFUNDS|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_PLANNING|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_SUBSIDIARY|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_FINANCE_LOCATION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_SBU|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_PROCEDURE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PAY_STRUCTURE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_SBU_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_LOCATION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_POSITION_CATEGORY|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PROVIDER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_BACKUP_LOAD_2026_01_16|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BUSINESS_UNIT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_PAYMENTS_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_LEDGER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_CPT_CREDITED_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_ORGANIZATION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PAYMENT_METHOD|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_DAY_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_METRIC|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_REFUNDS_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_METRIC_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_EMPLOYEE_HEADCOUNT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_MSO_DETAIL_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_BKP_1009|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLANNING_ACCOUNT_TYPE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_RECALL|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_SERVICE_LINE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_FINANCE_LOCATION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_DATE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLANNING_VERSION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_SERVICE_LOCATION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_GL_ACCOUNT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_GL_PERIOD_MONTH_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PAY_STRUCTURE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_EMPLOYEE_HISTORY_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_EEO_ATTRIBUTE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_POSITION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_LOCATION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_RECALL_TYPE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_DATE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_INSURANCE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_WORK_LOCATION|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_LEDGER_BKP_1009|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_DEPARTMENT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PATIENT_VW_TEST|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_RECALL_STATUS_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BILLING_ADJUSTMENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_INSURANCE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_REFERRING_PROVIDER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT_WITH_ETL_DATES|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_ADJUSTMENTS|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_RECALL_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_DEPARTMENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_GL_PERIOD_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_INSURANCE_PROVIDER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|EDW_DATADICTIONARY|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_PROCEDURE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_OPERATIONAL_CENTER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENDO_RECALL_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_INSURANCE_PROVIDER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_OPERATIONAL_CENTER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PATIENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_LEDGER_BKP_1010|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_SUPERBILL_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_PHYS_COMP_PHYSICIAN_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BUSINESS_UNIT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLACE_OF_SERVICE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_MSO_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLACE_OF_SERVICE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_ADJUSTMENTS_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|EDW_NETSUIT_CROSSWALK_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_WITH_ETL_DATES|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PAYMENT_METHOD_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_PAYMENTS|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_RECALL_STATUS|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_BK_1010|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_LEDGER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_APPOINTMENT_ACTIVITY_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_GL_ACCOUNT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PROVIDER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_USER|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_WORK_LOCATION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT_BKP_1010|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES_BKP_011626|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_SERVICE_LINE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_POSITION_CATEGORY_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_APPOINTMENT_STATUS_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_GL_PERIOD|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLANNING_VERSION_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BILLING_PAYMENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_EMPLOYEE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_ESS_ATTRIBUTE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PLANNING_ACCOUNT_TYPE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_ENCOUNTER_CPT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_EMPLOYEE_HISTORY|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_APPOINTMENT_ACTIVITY|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_LEDGER_W_ETL_DATES|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PROVIDER_VW_BKP10152025|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_APPOINTMENT_BACKUP_2026_01_15|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_EMPLOYEE_HEADCOUNT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_PLANNING_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_COA_SUBSIDIARY_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_REFERRING_PROVIDER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_PROVIDER_CURRENT_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|FACT_BILLING_CHARGES|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_CPTCODE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_EEO_ATTRIBUTE_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_USER_VW|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_BILLING_ADJUSTMENT|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_CPTCODE|{description}|{row_grain}|{Slowly Changing Behavior}|
|DIM_MODEL|DIM_EMPLOYEE|{description}|{row_grain}|{Slowly Changing Behavior}|

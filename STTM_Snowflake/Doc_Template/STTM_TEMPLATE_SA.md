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

###########################################################################################
##                              DBCONTROL - CONSULTAS
## DBModule - Consultas
## Autor: Luciano Alvarenga Maciel Pires
## Data: 12/03/2018
##
###########################################################################################
## Versao 1
## 12/03/2018 1.0 Luciano - Criacao do script
## 06/08/2018 1.1 Luciano - Criacao do modulo de merge
## 07/08/2018 1.2 Luciano - Criacao do modulo de inicializacao
###########################################################################################

def ddl_consulta(monitoramento):
    if monitoramento=="tablespace_dados_v2_10":
        str="""
            select
                a.tablespace_name,--0
                cur_total_mb,--1
                nvl(freespace,0) cur_free_mb,--2
                (cur_total_mb-nvl(freespace,0)) cur_used_mb,--3
                (nvl(freespace,0)/cur_total_mb)*100 PCT_CUR_FREE,--4
                (nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100 PCT_MAX_FREE ,--5
                max_can_ext2,--6
                100 - ((nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100) PCT_MAX_USED,--7
                c.contents--8
            from
                (select tablespace_name, sum(bytes)/1048576 cur_total_mb,
                sum(decode(maxbytes,0,bytes,greatest(maxbytes,bytes)))/1048576 max_can_ext2
                from dba_data_files
                group by tablespace_name
                ) a,
                (select tablespace_name, sum(bytes)/1048576 freespace
                from dba_free_space
                group by tablespace_name
                ) b,
                ( select tablespace_name , contents
                from dba_tablespaces 
                ) c
            where a.tablespace_name = b.tablespace_name (+)
            and a.tablespace_name = c.tablespace_name (+)
            order by 8
        """
    if monitoramento=="tablespace_dados_v2_12":
        str="""
            select
                a.tablespace_name,--0
                cur_total_mb,--1
                nvl(freespace,0) cur_free_mb,--2
                (cur_total_mb-nvl(freespace,0)) cur_used_mb,--3
                (nvl(freespace,0)/cur_total_mb)*100 PCT_CUR_FREE,--4
                (nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100 PCT_MAX_FREE ,--5
                max_can_ext2,--6
                100 - ((nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100) PCT_MAX_USED,--7
                c.contents--8
            from
                (select tablespace_name, sum(bytes)/1048576 cur_total_mb,
                sum(decode(maxbytes,0,bytes,greatest(maxbytes,bytes)))/1048576 max_can_ext2
                from cdb_data_files
                group by tablespace_name
                ) a,
                (select tablespace_name, sum(bytes)/1048576 freespace
                from cdb_free_space
                group by tablespace_name
                ) b,
                ( select tablespace_name , contents
                from cdb_tablespaces 
                ) c
            where a.tablespace_name = b.tablespace_name (+)
            and a.tablespace_name = c.tablespace_name (+)
            order by 8
        """
    if monitoramento=="tablespace_temp_v1":
        str="""
            select
                a.tablespace_name,
                cur_total_mb,
                nvl(freespace,0) cur_free_mb,
                (cur_total_mb-nvl(freespace,0)) cur_used_mb,
                (nvl(freespace,0)/cur_total_mb)*100 PCT_CUR_FREE,
                max_can_ext2,
                (nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100 PCT_MAX_FREE ,
                100 - ((nvl(freespace,0)+max_can_ext2-cur_total_mb)/max_can_ext2*100) PCT_MAX_USED,
                c.contents
            from
                (select tablespace_name, sum(bytes)/1048576 cur_total_mb,
                sum(decode(maxbytes,0,bytes,greatest(maxbytes,bytes)))/1048576 max_can_ext2 --even if autoextensible, maxbytes may be < bytes
                from dba_temp_files
                group by tablespace_name
                ) a,
                (select tablespace_name, sum(bytes)/1048576 freespace
                from dba_free_space
                group by tablespace_name
                ) b,
                ( select tablespace_name , contents
                from dba_tablespaces ) c
            where a.tablespace_name = b.tablespace_name (+)
                and a.tablespace_name = c.tablespace_name (+)
            order by 8
        """

    if monitoramento=="index_unusable":
        str="""select
                OWNER, --0
                TABLE_NAME, --1
                INDEX_NAME, --2
                STATUS --3
               from dba_indexes where status='UNUSABLE'
            """
    if monitoramento=="asm_diskgroup":
        str="""select a.NAME 
       ,a.total_mb
       ,round(a.total_mb-a.free_mb,2) used_mb
       ,a.free_mb
       ,round((a.total_mb-a.free_mb)*100 / a.total_mb,2) pct_used
       ,round((a.free_mb*100) / a.total_mb,2) pct_free
        from V$ASM_DISKGROUP_STAT a
        , V$ASM_DISK_STAT b
        , V$ASM_CLIENT c
        where a.group_number = b.group_number
        and a.group_number = c.group_number
        and b.MOUNT_STATUS = 'OPENED'
        and b.MODE_STATUS = 'ONLINE'
        and b.STATE = 'NORMAL'
        and c.status = 'CONNECTED'
        and round((a.free_mb*100) / a.total_mb,2) >= 5
        union
        select a.NAME
           ,a.total_mb
           ,round(a.total_mb-a.free_mb,2) used_mb
           ,a.free_mb
           ,round((a.total_mb-a.free_mb)*100 / a.total_mb,2) pct_used
           ,round((a.free_mb*100) / a.total_mb,2) pct_free
        from V$ASM_DISKGROUP_STAT a
        , V$ASM_DISK_STAT b
        , V$ASM_CLIENT c
        where a.group_number = b.group_number
        and a.group_number = c.group_number
        and ( b.MOUNT_STATUS <> 'OPENED' or
        b.MODE_STATUS <> 'ONLINE' or
        b.STATE <> 'NORMAL' or
        c.status <> 'CONNECTED' or
        round((a.free_mb*100) / a.total_mb,2) < 5 )
        """

    if monitoramento=="rman_backup_full":
        #Luciano> Modificado 19/06/2018 Adicionado novas informacoes
        str="""select  SESSION_KEY, --0
                       INPUT_TYPE, --1
                       STATUS, --2
                       START_TIME, --3
                       END_TIME, --4
                       ELAPSED_SECONDS, --5
                       INPUT_BYTES, --6
                       OUTPUT_BYTES, --7
                       INPUT_BYTES_PER_SEC, --8
                       OUTPUT_BYTES_PER_SEC --9
               from V$RMAN_BACKUP_JOB_DETAILS
               where INPUT_TYPE in ('DB INCR','DB FULL')
               and status in ('COMPLETED','COMPLETED WITH WARNINGS','COMPLETED WITH ERRORS')
               and session_key > :Vses_key  
        """
    if monitoramento=="rman_backup_arch":
        str="""select  SESSION_KEY, --0
                       INPUT_TYPE, --1
                       STATUS, --2
                       START_TIME, --3
                       END_TIME, --4
                       ELAPSED_SECONDS --5
               from V$RMAN_BACKUP_JOB_DETAILS
               where INPUT_TYPE ='ARCHIVELOG'
               and status='COMPLETED'
               and end_time=(select max(end_time)
                             from v$RMAN_BACKUP_JOB_DETAILS
 	                         where INPUT_TYPE='ARCHIVELOG'
                             and status='COMPLETED'
 	                         )
        """
    if monitoramento=="dataguard_info":
        str="""SELECT DEST_ID, --0
                      DESTINATION, --1
                      STATUS, --2
                      REGISTER, --3
                      TRANSMIT_MODE,--4
                      SCHEDULE,--5
                      LOG_SEQUENCE,--6
                      VALID_NOW,--7
                      VALID_TYPE,--8
                      VALID_ROLE,--9
                      VERIFY,--10
                      FAIL_DATE,--11
                      FAIL_SEQUENCE,--12
                      ERROR--13
            from v$archive_dest
            where target='STANDBY'
        """
    if monitoramento=="dataguard_gap":
        str="""
            SELECT
               la.dest_id --0
               ,la.thread# --1
               ,cu.currentsequence SEQUENCE_ATIVA --2
               ,la.lastarchived    SEQUENCE_ARCHIVADA --3
               ,appl.lastapplied   SEQUENCE_REPLICA --4
               ,la.lastarchived - appl.lastapplied "GAP" --5
            FROM (
                   select gvi.thread#,
                          gvd.dest_id,
                          MAX(gvd.log_sequence) currentsequence
                   FROM gv$archive_dest gvd, 
                        gv$instance gvi
                   WHERE gvd.status = 'VALID'
                     AND gvi.inst_id = gvd.inst_id
                   GROUP BY thread#, dest_id
                   ) cu, 
                   (
                    SELECT thread#, 
                           dest_id, 
                           MAX(sequence#) lastarchived
                    FROM gv$archived_log
                    WHERE resetlogs_change# = (SELECT resetlogs_change# FROM v$database)
                      AND archived = 'YES'
                    GROUP BY thread#, dest_id
                    ) la, 
                    (
                     SELECT thread#, 
                            dest_id, 
                            MAX(sequence#) lastapplied
                     FROM gv$archived_log
                     WHERE resetlogs_change# = (SELECT resetlogs_change# FROM v$database)
                       AND applied = 'YES'
                       AND standby_dest = 'YES'
                     GROUP BY thread#, dest_id
                    ) appl
            WHERE cu.thread# = la.thread#
               AND cu.thread# = appl.thread#
               AND cu.dest_id = la.dest_id
               AND cu.dest_id = appl.dest_id
            ORDER BY 1,2
        """

    if monitoramento=="option_spatial":
        str="""
            select distinct 'Spatial and Graphs' as produto, --0
                T.DataMining as Instalado, --1
                z.QUANT_UTILIZADA, --2
                U.ATUALMENTE_UTILIZADA --3
            from (
                select case value when 'FALSE' then 'N instalado' else 'Instalado' end as DataMining
                from  v$option WHERE  parameter='Spatial'
                ) T,
                (select sum(DETECTED_USAGES) as QUANT_UTILIZADA
                from DBA_FEATURE_USAGE_STATISTICS
                WHERE name='Spatial' group by name ) z,
                (select case CURRENTLY_USED when 'FALSE' then 'NAO UTILIZADA' else 'UTILIZADA' END AS ATUALMENTE_UTILIZADA
                from DBA_FEATURE_USAGE_STATISTICS
                WHERE name='Spatial'
                and version=(select max(version) from DBA_FEATURE_USAGE_STATISTICS WHERE name='Spatial')
                ) u
        """
    if monitoramento=="database_version":
        str="""
        SELECT Database_Version.PRODUTO --0
           ,Database_Version.PRODUTO_STATUS --1
           ,Database_Version.VERSAO --2
           ,Database_Version.VERSAO_COMPLETA --3
        FROM
        (select PRODUCT PRODUTO,
                STATUS PRODUTO_STATUS,
                to_number(substr(VERSION,1,INSTR(VERSION,'.')-1) ) VERSAO,      
                VERSION VERSAO_COMPLETA
        from product_component_version
        where product like '%Database%'
        AND ROWNUM=1
        )Database_Version
        """
    if monitoramento=="database_bundle":
        str="""
        SELECT to_char(Bundle_Patch.BUNDLE_DATA,'dd/mm/yyyy hh24:mi:ss') BUNDLE_DATA --0
           ,Bundle_Patch.BUNDLE_ACTION --1
           ,Bundle_Patch.BUNDLE_VERSION --2
           ,Bundle_Patch.BUNDLE_COMMENTS --3
        FROM
         (select ACTION_TIME BUNDLE_DATA
                ,ACTION     BUNDLE_ACTION
                ,VERSION    BUNDLE_VERSION
                ,COMMENTS   BUNDLE_COMMENTS
        from sys.registry$history
        WHERE ACTION_TIME = (SELECT MAX(ACTION_TIME) FROM sys.registry$history WHERE ACTION IN ('APPLY','UPGRADE') )
        ) Bundle_Patch
        """

    if monitoramento=="database_info":
        str="""SELECT 
             NAME --0
            ,DB_UNIQUE_NAME --1
            ,CREATED --2
            ,LOG_MODE --3
            ,VERSION_TIME --4
            ,OPEN_MODE --5
            ,PROTECTION_MODE --6
            ,PROTECTION_LEVEL --7
            ,DATABASE_ROLE --8
            ,GUARD_STATUS --9
            ,SWITCHOVER_STATUS --10
            ,DATAGUARD_BROKER --11
            ,SUPPLEMENTAL_LOG_DATA_MIN --12
            ,SUPPLEMENTAL_LOG_DATA_PK --13
            ,SUPPLEMENTAL_LOG_DATA_UI --14
            ,SUPPLEMENTAL_LOG_DATA_FK --15
            ,SUPPLEMENTAL_LOG_DATA_ALL --16
            ,FORCE_LOGGING --17
            ,PLATFORM_NAME --18
            ,FLASHBACK_ON --19
            ,null AS CDB --20
        from v$database
        """
    if monitoramento=="database_info_12c":
        str="""SELECT 
             NAME --0
            ,DB_UNIQUE_NAME --1
            ,CREATED --2
            ,LOG_MODE --3
            ,VERSION_TIME --4
            ,OPEN_MODE --5
            ,PROTECTION_MODE --6
            ,PROTECTION_LEVEL --7
            ,DATABASE_ROLE --8
            ,GUARD_STATUS --9
            ,SWITCHOVER_STATUS --10
            ,DATAGUARD_BROKER --11
            ,SUPPLEMENTAL_LOG_DATA_MIN --12
            ,SUPPLEMENTAL_LOG_DATA_PK --13
            ,SUPPLEMENTAL_LOG_DATA_UI --14
            ,SUPPLEMENTAL_LOG_DATA_FK --15
            ,SUPPLEMENTAL_LOG_DATA_ALL --16
            ,FORCE_LOGGING --17
            ,PLATFORM_NAME --18
            ,FLASHBACK_ON --19
            ,CDB --20
        from v$database
        """

    if monitoramento=="instance_info":
        str="""SELECT INSTANCE_NUMBER, --0
                      INSTANCE_NAME, --1
                      HOST_NAME, --2
                      VERSION, --3
                      STARTUP_TIME, --4
                      STATUS, --5
                      PARALLEL, --6
                      THREAD#, --7
                      ARCHIVER, --8
                      LOG_SWITCH_WAIT, --9
                      SHUTDOWN_PENDING, --10
                      DATABASE_STATUS, --11
                      INSTANCE_ROLE --12
                FROM GV$INSTANCE"""

    if monitoramento=="database_services":
        str="""select inst_id, --0
                      service_id, --1
                      name --2
                from gV$ACTIVE_SERVICES 
                where network_name is not null
                and name not like 'SYS.KUP%'
        """

    if monitoramento=="users_10g":
        str="""select 
                     USERNAME
                     ,ACCOUNT_STATUS
                     ,LOCK_DATE
                     ,EXPIRY_DATE
                     ,CREATED
                     ,PROFILE
               from dba_users
        """

    if monitoramento=="users_12c":
        str="""select 
                  USERNAME
                  ,ACCOUNT_STATUS
                  ,LOCK_DATE
                  ,EXPIRY_DATE
                  ,CREATED
                  ,PROFILE
                  ,COMMON
                  ,LAST_LOGIN 
               from cdb_users
        """
    if monitoramento=="SegInfo_CkPrograms":
        str="""select distinct USERNAME, 
                      OSUSER, 
                      MACHINE, 
                      PROGRAM, 
                      MODULE, 
                      ACTION
               FROM GV$SESSION
               WHERE TYPE='USER'
               AND USERNAME NOT IN ('SYS','SYSTEM','DBSNMP','SYSRAC','PUBLIC')
        """
    #Retorno da consulta
    return(str)

def ddl_insert(monitoramento):
    if monitoramento=="tablespace_dados_v1":
        str="""
             INSERT INTO ORA_TABLESPACE    (TARGET,
                                            ID_CHECKLIST,
                                            DATA_COLETA,
                                            TABLESPACE,
                                            CURR_TOTAL_MB,
                                            CUR_FREE_MB,
                                            CUR_USED_MB,
                                            PCT_CUR_FREE,
                                            MAX_CAN_EXT2,
                                            PCT_MAX_FREE,
                                            PCT_MAX_USED,
                                            CONTENTS)
            VALUES (:vTARGET,
                    :ID_CHECKLIST,
                    sysdate,
                    :TABLESPACE,
                    :CURR_TOTAL_MB,
                    :CUR_FREE_MB,
                    :CUR_USED_MB,
                    :PCT_CUR_FREE,
                    :MAX_CAN_EXT2,
                    :PCT_MAX_FREE,
                    :PCT_MAX_USED,
                    :CONTENTS)
            """
    if monitoramento=="tablespace_temp_v1":
        str="""
             INSERT INTO ORA_TABLESPACE (TARGET,
                                            ID_CHECKLIST,
                                            DATA_COLETA,
                                            TABLESPACE,
                                            CURR_TOTAL_MB,
                                            CUR_FREE_MB,
                                            CUR_USED_MB,
                                            PCT_CUR_FREE,
                                            MAX_CAN_EXT2,
                                            PCT_MAX_FREE,
                                            PCT_MAX_USED,
                                            CONTENTS)
            VALUES (:vTARGET,
                    :ID_CHECKLIST,
                    sysdate,
                    :TABLESPACE,
                    :CURR_TOTAL_MB,
                    :CUR_FREE_MB,
                    :CUR_USED_MB,
                    :PCT_CUR_FREE,
                    :MAX_CAN_EXT2,
                    :PCT_MAX_FREE,
                    :PCT_MAX_USED,
                    :CONTENTS)
            """

    if monitoramento=="index_unusable":
        str="""
            INSERT INTO ORA_INDICE_UNUSABLE(TARGET,
                                              ID_CHECKLIST,
                                              DATA_COLETA,
                                              OWNER,
                                              TABLE_NAME,
                                              INDEX_NAME,
                                              STATUS)
            VALUES (:vTARGET
                   ,:ID_CHECKLIST
                   ,sysdate
                   ,:OWNER
                   ,:TABLE_NAME
                   ,:INDEX_NAME
                   ,:STATUS)
            """
    if monitoramento=="asm_diskgroup":
        str="""INSERT INTO ORA_ASM_DISKGROUP (
                                                TARGET
                                                ,ID_CHECKLIST
                                                ,DATA_COLETA
                                                ,NAME
                                                ,TOTAL_MB
                                                ,USED_MB
                                                ,FREE_MB
                                                ,PCT_USED
                                                ,PCT_FREE) VALUES(
                                                :vTARGET
                                                ,:ID_CHECKLIST
                                                ,sysdate
                                                ,:NAME
                                                ,:TOTAL_MB
                                                ,:USED_MB
                                                ,:FREE_MB
                                                ,:PCT_USED
                                                ,:PCT_FREE)
        """
    if monitoramento=="rman_backup_full":
        str="""INSERT INTO ORA_RMAN (TARGET,
	                     ID_CHECKLIST,
	                     DATA_COLETA,
	                     TIPO,
	                     SESSION_KEY,
	                     STATUS,
	                     START_TIME,
	                     END_TIME,
	                     TEMPO_SEGUNDOS,
                         INPUT_BYTES, --6
                         OUTPUT_BYTES, --7
                         INPUT_BYTES_PER_SEC, --9
                         OUTPUT_BYTES_PER_SEC, --10
                         CANAIS
                         )
             VALUES (:vTARGET, 
                     :ID_CHECKLIST, 
                     sysdate, 
                     'F',
                     :SESSION_KEY, 
                     :STATUS, 
                     :START_TIME, 
                     :END_TIME, 
                     :TEMPO_SEGUNDOS,
                     :INPUT_BYTES,
                     :OUTPUT_BYTES,
                     :INPUT_BYTES_PER_SEC,
                     :OUTPUT_BYTES_PER_SEC,
                     :CANAIS)
        """
    if monitoramento=="rman_backup_arch":
        str="""INSERT INTO ORA_RMAN (TARGET ,
	                     ID_CHECKLIST,
	                     DATA_COLETA,
	                     TIPO,
	                     SESSION_KEY,
	                     STATUS,
	                     START_TIME,
	                     END_TIME,
	                     TEMPO_SEGUNDOS,
	                     THRESHOLD_HR)
             VALUES (:vTARGET, 
                     :ID_CHECKLIST, 
                     sysdate,
                     'A',
                     :SESSION_KEY, 
                     :STATUS, 
                     :START_TIME, 
                     :END_TIME, 
                     :TEMPO_SEGUNDOS)
        """
    if monitoramento=="dataguard_info":
        str="""
                INSERT INTO ORA_DATAGUARD_INFO (
                    TARGET, 
                    ID_CHECKLIST, 
                    DATA_COLETA,
                    DEST_ID,
                    DESTINATION,
                    STATUS,
                    REGISTER,
                    TRANSMIT_MODE,
                    SCHEDULE,
                    LOG_SEQUENCE,
                    VALID_NOW,
                    VALID_TYPE,
                    VALID_ROLE,
                    VERIFY,
                    FAIL_DATE,
                    FAIL_SEQUENCE,
                    ERROR)
                VALUES (:vTARGET,
                        :ID_CHECKLIST,
                        sysdate,
                        :DEST_ID,
                        :DESTINATION,
                        :STATUS,
                        :REGISTER,
                        :TRANSMIT_MODE,
                        :SCHEDULE,
                        :LOG_SEQUENCE,
                        :VALID_NOW,
                        :VALID_TYPE,
                        :VALID_ROLE,
                        :VERIFY,
                        :FAIL_DATE,
                        :FAIL_SEQUENCE,
                        :ERROR
                )
        """
    if monitoramento=="users":
        str="""INSERT INTO ORA_USERS  (TARGET
                                      ,ID_CHECKLIST
                                      ,DATA_COLETA
                                      ,USERNAME
                                      ,ACCOUNT_STATUS
                                      ,LOCK_DATE
                                      ,EXPIRY_DATE
                                      ,CREATED
                                      ,PROFILE
                                      ,COMMON 
                                      ,LAST_LOGIN) 
                              VALUES (:vTARGET
                                      ,:ID_CHECKLIST
                                      ,SYSDATE
                                      ,:USERNAME
                                      ,:ACCOUNT_STATUS
                                      ,:LOCK_DATE
                                      ,:EXPIRY_DATE
                                      ,:CREATED
                                      ,:PROFILE
                                      ,:COMMON
                                      ,:LAST_LOGIN)
        """
    if monitoramento=="dataguard_gap":
        str="""
          INSERT INTO ORA_DATAGUARD_GAP (
                                        TARGET
                                        ,ID_CHECKLIST
                                        ,DATA_COLETA
                                        ,DEST_ID
                                        ,THREAD
                                        ,SEQUENCIA_ONLINE
                                        ,SEQUENCIA_APLICADA
                                        ,SEQUENCIA_REPLICA
                                        ,GAP
                                        )VALUES(
                                         :vTARGET
                                        ,:ID_CHECKLIST
                                        ,sysdate
                                        ,:DEST_ID
                                        ,:THREAD
                                        ,:SEQUENCIA_ONLINE
                                        ,:SEQUENCIA_APLICADA
                                        ,:SEQUENCIA_REPLICA
                                        ,:GAP)
        """
    if monitoramento=="option_spatial":
        str="""
        INSERT INTO ORA_OPTIONS (TARGET
                                    ,ID_CHECKLIST
                                    ,DATA_COLETA
                                    ,PRODUTO
                                    ,INSTALADO
                                    ,QUANT_UTILIZADA
                                    ,ATUALMENTE_UTILIZADA)VALUES
                                    (:vTARGET
                                    ,:ID_CHECKLIST
                                    ,sysdate
                                    ,:PRODUTO
                                    ,:INSTALADO
                                    ,:QUANT_UTILIZADA
                                    ,:ATUALMENTE_UTILIZADA)
        """
    if monitoramento=="database_info":
         str="""INSERT INTO STAGE_DATABASE (TARGET,ID_CHECKLIST,DATA_COLETA
                                            ,PRODUTO,PRODUTO_STATUS, VERSAO, VERSAO_COMPLETA
                                            ,BUNDLE_DATA, BUNDLE_ACTION, BUNDLE_INFORMACAO
                                            ,NAME,DB_UNIQUE_NAME
                                            ,LOG_MODE,VERSION_TIME,OPEN_MODE,PROTECTION_MODE,PROTECTION_LEVEL
                                            ,DATABASE_ROLE,GUARD_STATUS,SWITCHOVER_STATUS
                                            ,DATAGUARD_BROKER,SUPPLEMENTAL_LOG_DATA_MIN,SUPPLEMENTAL_LOG_DATA_PK,SUPPLEMENTAL_LOG_DATA_UI
                                            ,SUPPLEMENTAL_LOG_DATA_FK,SUPPLEMENTAL_LOG_DATA_ALL
                                            ,FORCE_LOGGING,PLATFORM_NAME,FLASHBACK_ON,CDB) 
                                     values (:vTARGET,:ID_CHECKLIST,sysdate
                                            ,:PRODUTO,:PRODUTO_STATUS, :VERSAO, :VERSAO_COMPLETA
                                            ,to_date(:BUNDLE_DATA,'dd/mm/yyyy hh24:mi:ss'), :BUNDLE_ACTION, :BUNDLE_INFORMACAO
                                            ,:NAME,:DB_UNIQUE_NAME
                                            ,:LOG_MODE,:VERSION_TIME,:OPEN_MODE,:PROTECTION_MODE,:PROTECTION_LEVEL
                                            ,:DATABASE_ROLE,:GUARD_STATUS,:SWITCHOVER_STATUS
                                            ,:DATAGUARD_BROKER,:SUPPLEMENTAL_LOG_DATA_MIN,:SUPPLEMENTAL_LOG_DATA_PK,:SUPPLEMENTAL_LOG_DATA_UI
                                            ,:SUPPLEMENTAL_LOG_DATA_FK,:SUPPLEMENTAL_LOG_DATA_ALL
                                            ,:FORCE_LOGGING,:PLATFORM_NAME,:FLASHBACK_ON,nvl(:CDB,'NO'))
                                            """
    if monitoramento=="instance_info":
        str="""INSERT INTO STAGE_INSTANCE (TARGET, 
                            ID_CHECKLIST, DATA_COLETA, 
                            INSTANCE_NUMBER, INSTANCE_NAME, 
                            HOST_NAME, VERSION,
                            STARTUP_TIME, STATUS, 
                            PARALLEL, THREAD, 
                            ARCHIVER, LOG_SWITCH_WAIT,
                            SHUTDOWN_PENDING, DATABASE_STATUS, 
                            INSTANCE_ROLE)
                            VALUES (:vTARGET, 
                            :ID_CHECKLIST, sysdate, 
                            :INSTANCE_NUMBER, :INSTANCE_NAME, 
                            :HOST_NAME, :VERSION, 
                            :STARTUP_TIME, :STATUS, 
                            :PARALLEL, :THREAD, 
                            :ARCHIVER, :LOG_SWITCH_WAIT, 
                            :SHUTDOWN_PENDING, :DATABASE_STATUS, 
                            :INSTANCE_ROLE
                            )"""

    if monitoramento=="database_services":
        str="""INSERT INTO ORA_DBSERVICE (TARGET
                                             ,ID_CHECKLIST
                                             ,INST_ID
                                             ,SERVICE_ID
                                             ,NAME)
                                    values (:vTARGET, 
                                            :ID_CHECKLIST, 
                                            :INST_ID, 
                                            :SERVICE_ID,
                                            :NAME )"""
    ##Retornos
    return(str)

def ddl_merge(monitoramento):
    if monitoramento=="database_services":
        str="""
        MERGE INTO ORA_DBSERVICE T
        USING (SELECT *
               FROM STAGE_DBSERVICE
               WHERE TARGET=:vTARGET
               AND ID_CHECKLIST=:vID_CHECKLIST) STAGE
        ON (   T.TARGET = STAGE.TARGET
           AND T.INST_ID = STAGE.INST_ID
           AND T.NAME = STAGE.NAME
           )
        WHEN MATCHED THEN
            UPDATE SET T.ID_CHECKLIST = STAGE.ID_CHECKLIST, LUUP_CHECK='ON'
        WHEN NOT MATCHED THEN
            INSERT (TARGET
                   ,DATA_COLETA
                   ,ID_CHECKLIST
                   ,INST_ID
                   ,SERVICE_ID
                   ,NAME
                   ,LUUP_CHECK)
            VALUES (STAGE.TARGET
                    ,STAGE.DATA_COLETA
                    ,STAGE.ID_CHECKLIST
                    ,STAGE.INST_ID
                    ,STAGE.SERVICE_ID
                    ,STAGE.NAME
                    ,'ON')
        """
    if monitoramento=="instance_info":
        str="""
        MERGE INTO ORA_INSTANCE T
        USING (SELECT *
               FROM STAGE_INSTANCE
               WHERE TARGET=:vTARGET
               AND ID_CHECKLIST=:vID_CHECKLIST) STAGE
        ON (   T.TARGET = STAGE.TARGET
           AND T.INST_ID = STAGE.INST_ID
           )
        WHEN MATCHED THEN
            UPDATE SET T.ID_CHECKLIST = STAGE.ID_CHECKLIST, STATUS='ON'
        WHEN NOT MATCHED THEN
            INSERT (TARGET
                   ,DATA_COLETA
                   ,ID_CHECKLIST
                   ,INST_ID
                   ,SERVICE_ID
                   ,NAME
                   ,STATUS)
            VALUES (STAGE.TARGET
                    ,STAGE.DATA_COLETA
                    ,STAGE.ID_CHECKLIST
                    ,STAGE.INST_ID
                    ,STAGE.SERVICE_ID
                    ,STAGE.NAME
                    ,STAGE.STATUS)
        """
    if monitoramento=="SegInfo_CkPrograms":
        str="""
        MERGE INTO ORA_SI_SESSION_PROGRAM T
        USING (SELECT * FROM Stage_SegInfo_CkPrograms
               WHERE TARGET=:vTARGET) STAGE
        ON (  T.TARGET = STAGE.TARGET
              AND T.USERNAME = STAGE.USERNAME
              AND T.OSUSER = STAGE.OSUSER
              AND T.MACHINE = STAGE.MACHINE
              AND T.PROGRAM = STAGE.PROGRAM
              AND T.MODULE = STAGE.MODULE
              AND T.ACTION = STAGE.ACTION  )
        WHEN MATCHED THEN
           UPDATE SET T.DATA_ULTIMA_COLETA = SYSDATE
                      ,T.QUANTIDADE=QUANTIDADE+1
           WHERE T.TARGET=:vTARGET
        WHEN NOT MATCHED THEN
            INSERT (TARGET,
                   USERNAME,
                   OSUSER,
                   MACHINE,
                   PROGRAM,
                   MODULE,
                   ACTION,
                   DATA_COLETA,
                   DATA_ULTIMA_COLETA,
                   QUANTIDADE)
            VALUES (STAGE.TARGET,
                    STAGE.USERNAME,
                    STAGE.OSUSER,
                    STAGE.MACHINE,
                    STAGE.PROGRAM,
                    STAGE.MODULE,
                    STAGE.ACTION,
                    SYSDATE,
                    SYSDATE,
                    1)"""

    if monitoramento=="database_info":
        str="""
        MERGE INTO ORA_DATABASE T
        USING (SELECT *
               FROM STAGE_DATABASE
               WHERE TARGET=:vTARGET
               AND ID_CHECKLIST=:vID_CHECKLIST) STAGE
        ON (   T.TARGET = STAGE.TARGET )
        WHEN MATCHED THEN
           UPDATE SET T.ID_CHECKLIST = STAGE.ID_CHECKLIST
                      ,ULTIMA_COLETA=SYSDATE
                      ,VERSAO=STAGE.VERSAO
                      ,VERSAO_COMPLETA=STAGE.VERSAO_COMPLETA
                      ,PRODUTO=STAGE.PRODUTO
                      ,BUNDLE_DATA=STAGE.BUNDLE_DATA
                      ,BUNDLE_ACTION=STAGE.BUNDLE_ACTION
                      ,BUNDLE_INFORMACAO=STAGE.BUNDLE_INFORMACAO
                      ,LOG_MODE=STAGE.LOG_MODE
                      ,PROTECTION_MODE=STAGE.PROTECTION_MODE
                      ,PROTECTION_LEVEL=STAGE.PROTECTION_LEVEL
                      ,DATABASE_ROLE=STAGE.DATABASE_ROLE
                      ,GUARD_STATUS=STAGE.GUARD_STATUS
                      ,SWITCHOVER_STATUS=STAGE.SWITCHOVER_STATUS
                      ,DATAGUARD_BROKER=STAGE.DATAGUARD_BROKER
                      ,SUPPLEMENTAL_LOG_DATA_MIN=STAGE.SUPPLEMENTAL_LOG_DATA_MIN
                      ,SUPPLEMENTAL_LOG_DATA_PK=STAGE.SUPPLEMENTAL_LOG_DATA_PK
                      ,SUPPLEMENTAL_LOG_DATA_UI=STAGE.SUPPLEMENTAL_LOG_DATA_UI
                      ,SUPPLEMENTAL_LOG_DATA_FK=STAGE.SUPPLEMENTAL_LOG_DATA_FK
                      ,SUPPLEMENTAL_LOG_DATA_ALL=STAGE.SUPPLEMENTAL_LOG_DATA_ALL
                      ,FORCE_LOGGING=STAGE.FORCE_LOGGING
                      ,PLATFORM_NAME=STAGE.PLATFORM_NAME
                      ,FLASHBACK_ON=STAGE.FLASHBACK_ON
                      ,CDB=STAGE.CDB
        WHEN NOT MATCHED THEN
            INSERT (TARGET
                   ,ID_CHECKLIST
                   ,DATA_COLETA
                   ,COLETA_ATIVA
                   ,PRODUTO
                   ,PRODUTO_STATUS
                   ,VERSAO
                   ,VERSAO_COMPLETA
                   ,BUNDLE_DATA
                   ,BUNDLE_ACTION
                   ,BUNDLE_INFORMACAO
                   ,NAME
                   ,DB_UNIQUE_NAME
                   ,CREATED
                   ,LOG_MODE
                   ,VERSION_TIME
                   ,OPEN_MODE
                   ,PROTECTION_MODE
                   ,PROTECTION_LEVEL
                   ,DATABASE_ROLE
                   ,GUARD_STATUS
                   ,SWITCHOVER_STATUS
                   ,DATAGUARD_BROKER
                   ,SUPPLEMENTAL_LOG_DATA_MIN
                   ,SUPPLEMENTAL_LOG_DATA_PK
                   ,SUPPLEMENTAL_LOG_DATA_UI
                   ,SUPPLEMENTAL_LOG_DATA_FK
                   ,SUPPLEMENTAL_LOG_DATA_ALL
                   ,FORCE_LOGGING
                   ,PLATFORM_NAME
                   ,FLASHBACK_ON
                   ,CDB)
            VALUES (STAGE.TARGET
                   ,STAGE.ID_CHECKLIST
                   ,STAGE.DATA_COLETA
                   ,STAGE.COLETA_ATIVA
                   ,STAGE.PRODUTO
                   ,STAGE.PRODUTO_STATUS
                   ,STAGE.VERSAO
                   ,STAGE.VERSAO_COMPLETA
                   ,STAGE.BUNDLE_DATA
                   ,STAGE.BUNDLE_ACTION
                   ,STAGE.BUNDLE_INFORMACAO
                   ,STAGE.NAME
                   ,STAGE.DB_UNIQUE_NAME
                   ,STAGE.CREATED
                   ,STAGE.LOG_MODE
                   ,STAGE.VERSION_TIME
                   ,STAGE.OPEN_MODE
                   ,STAGE.PROTECTION_MODE
                   ,STAGE.PROTECTION_LEVEL
                   ,STAGE.DATABASE_ROLE
                   ,STAGE.GUARD_STATUS
                   ,STAGE.SWITCHOVER_STATUS
                   ,STAGE.DATAGUARD_BROKER
                   ,STAGE.SUPPLEMENTAL_LOG_DATA_MIN
                   ,STAGE.SUPPLEMENTAL_LOG_DATA_PK
                   ,STAGE.SUPPLEMENTAL_LOG_DATA_UI
                   ,STAGE.SUPPLEMENTAL_LOG_DATA_FK
                   ,STAGE.SUPPLEMENTAL_LOG_DATA_ALL
                   ,STAGE.FORCE_LOGGING
                   ,STAGE.PLATFORM_NAME
                   ,STAGE.FLASHBACK_ON
                   ,NVL(STAGE.CDB,'NO') )
        """
    return(str)

def ddl_init(monitoramento):
    if monitoramento=="database_services":
        str="""UPDATE ORA_DBSERVICE 
                  SET LUUP_CHECK='OFF' 
               WHERE target=:vTARGET
            """
    
    if monitoramento=="instance_info":
        str="""UPDATE ORA_INSTANCE
                SET LUUP_CHECK='OFF'
           WHERE target=:vTARGET
        """
    #Retornos
    return(str)

def ddl_check(check_name):
    if check_name=="ck_database_services":
        str="""SELECT NAME from ora_dbservice
               where target=:vTARGET
               and NAME not in (SELECT ORA_DBSERVICE.NAME FROM
                                          (SELECT TARGET, NAME, COUNT(1) QUANTIDADE 
                                              FROM  ORA_DBSERVICE WHERE TARGET=:vTARGET GROUP BY TARGET, NAME) ORA_DBSERVICE
                                INNER JOIN STAGE_DBSERVICE ON
                                            ORA_DBSERVICE.TARGET = STAGE_DBSERVICE.TARGET
                                        AND ORA_DBSERVICE.NAME = STAGE_DBSERVICE.NAME
                                        AND ORA_DBSERVICE.QUANTIDADE = STAGE_DBSERVICE.QUANTIDADE
                                        AND STAGE_DBSERVICE.TARGET=:vTARGET)
        """

    if check_name=="ck_instance_status":
        str="""
             SELECT INSTANCE_NAME, HOST_NAME, STATUS
             FROM ORA_INSTANCE
             WHERE TARGET=:vTARGET
             AND STATUS <>'OPEN'
             
        """
    if check_name=="ck_instance_startup":
        str="""
             SELECT INSTANCE_NAME, HOST_NAME, TO_CHAR(STARTUP_TIME,'DD-MM-YYYY HH24:MI:SS') DATA_STARTUP
             FROM ORA_INSTANCE
             WHERE TARGET=:vTARGET
             AND STARTUP_TIME > SYSDATE-0.5/24
        """
    return(str)

def ddl_check_diff(check_name,str_campos):
    if check_name=="ck_database_diff":
        str=""" select count(1) from (
                    select target,"""+str_campos+"""
                    from stage_database
                    where id_checklist=:vID_CHECKLIST
                    and target=:vTARGET
                    minus 
                    select target,"""+str_campos+"""
                    from ora_database
                    where target=:vTARGET
             )"""
    return(str)

import sys
import monitoramentos as monit

valor=sys.argv[1]
monit.dispara_coleta(valor,'tablespace_dados',0)
monit.dispara_coleta(valor,'index_unusable',0)
monit.dispara_coleta(valor,'asm_diskgroup',0)
monit.dispara_coleta(valor,'rman_backup_full',0)
monit.dispara_coleta(valor,'dataguard_info',0)
monit.dispara_coleta(valor,'dataguard_gap',0)
monit.dispara_coleta(valor,'database_info',0)
monit.dispara_coleta(valor,'database_services',0)
monit.dispara_coleta(valor,'database_instance',0)
monit.dispara_coleta(valor,'users',0)


import sys

sys.path.insert(0, "../lib")

import utils
import ingestors

tablename   = dbutils.widgets.get("tablename")
idfield     = dbutils.widgets.get("id_field")

catalog     = "silver"
schemaname  = "saude_dados"

remove_checkpoint = False

if not utils.table_exists(spark, catalog, schemaname, tablename):
    # Criando a tabela caso não exista 
    query = utils.import_query(f"{tablename}.sql")
    (spark.sql(query)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{catalog}.{schemaname}.{tablename}"))
    remove_checkpoint = True

# CDF
ingest = ingestors.Ingestor_silver(spark       = spark,
                                   catalog     = catalog,
                                   schemaname  = schemaname,
                                   tablename   = tablename,
                                   id_field    = idfield)
if remove_checkpoint:
   dbutils.fs.rm(ingest.checkpoint_location, True)

carrega_silver = ingest.execute()


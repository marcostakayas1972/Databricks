# Databricks notebook source
import delta
import sys

# insere a biblioteca
sys.path.insert(0, "../lib/")

# importa as classes criadas na lib 
import utils
import ingestors

# Recebe os parâmetros 
tablename       = dbutils.widgets.get("tablename")
id_field        = dbutils.widgets.get("id_field")
timestamp_field = dbutils.widgets.get("timestamp_field")
datetime        = dbutils.widgets.get("datetime")

# pode não vir por parâmetro (questão de segurança)
user            = dbutils.widgets.get("user")
password        = dbutils.widgets.get("password")

# Conexão Oracle / query
jdbc_url        = "jdbc:oracle:thin:@your_oracle_host:1521:your_service_name"
db_properties   = {"user": user, "password": password}
query           = f"select * from {tablename} where {timestamp_field} BETWEEN {datetime} AND {datetime} + (30 / (24 * 60))"

# Catálogo é a camada em si
catalog         =  "bronze"
# schemaname é o banco de dados
schemaname      = "ss"

# Verifica se a tabela existe, caso contrário deverá ser criada
tabela_existe   = utils.table_exists(spark, catalog, schemaname, tablename)
   
# Ingestão
ingest_bronze = ingestors.Ingestor_bronze(spark           = spark,
                                          catalog         = catalog,
                                          schemaname      = schemaname,
                                          tablename       = tablename,
                                          jdbc_url        = jdbc_url,
                                          db_properties   = db_properties,
                                          query           = query,
                                          id_field        = id_field,
                                          timestamp_field = timestamp_field,
                                          tabela_existe   = tabela_existe
                                         )
ingest_bronze.execute()



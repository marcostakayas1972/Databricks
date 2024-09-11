import delta
import utils
import tqdm

class Ingestor_bronze:
    # init da classe #
    def __init__(self, spark, 
                       catalog, 
                       schemaname, 
                       tablename, 
                       jdbc_url,
                       db_properties, 
                       query, 
                       id_field,
                       timestamp_field,
                       tabela_existe):
        self.spark           = spark
        self.catalog         = catalog
        self.schemaname      = schemaname
        self.tablename       = tablename
        self.jdbc_url        = jdbc_url
        self.db_properties   = db_properties
        self.query           = query 
        self.id_field        = id_field
        self.timestamp_field = timestamp_field
        self.tabela_existe   = tabela_existe
        self.deltatable      = delta.DeltaTable.forName(self.spark, f"{self.catalog}.
                                                                   {self.schemaname}.
                                                                    {self.tablename}")    
    # Lê do Oracle
    def load(self):
        # AVISO: Senão performar por causa da inferência do schema, passar ele pronto via JSON.
        df = (self.spark.read
                        .format('jdbc')
                        .option("url",      self.jdbc_url)
                        .option("user",     self.db_properties["user"])
                        .option("password", self.db_properties["password"])
                        .option("dbtable", f"({self.query}) as tmp_table")  
                        .option("driver", "oracle.jdbc.driver.OracleDriver")
                        .load())
        return df

    # Grava no DataBricks
    def save(self, source_df):
        # Monta dinamicamente o dicionário de colunas para a atualização e inserção
        dicionario = {col_name: col(f"source.{col_name}") for col_name in source_df.columns}

        if not self.tabela_existe:
           # Cria a tabela target senão existir
           source_df.write.format("delta").saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}")
        else: 
           # Realiza o merge
           (deltatable.alias("target")
                   .merge(source_df.alias("source"), f"target.{self.id_field} = source.{self.id_field}")
                   .whenMatchedUpdate(condition    = f"source.{self.id_field} IS NOT NULL AND target.{self.id_field} IS NOT NULL",set = dicionario)
                   .whenMatchedDelete(condition    = f"source.{self.id_field} IS NULL     AND target.{self.id_field} IS NULL")
                   .whenNotMatchedInsert(condition = f"source.{self.id_field} IS NOT NULL AND target.{self.id_field} IS NULL",values = dicionario)
                   .execute()
           )
        return True        

    # Chamada
    def execute(self):
        df = self.load() 
        return self.save(df)

class Ingestor_silver:
    # Init da Classe #
    def __init__(self, spark, catalog, schemaname, tablename, id_field):
        self.spark           = spark
        self.catalog         = catalog
        self.schemaname      = schemaname
        self.tablename       = tablename
        self.id_field        = id_field
        self.set_query()
        self.checkpoint_location = f"/Volumes/raw/{schemaname}/cdf/{catalog}_{tablename}_checkpoint/"
        self.deltatable      = delta.DeltaTable.forName(self.spark, f"{self.catalog}.
                                                                   {self.schemaname}.
                                                                    {self.tablename}")    
    def set_query(self):
        query               = utils.import_query(f"{self.tablename}.sql")
        self.from_table     = utils.extract_from(query=query)

    # Leitura dos arquivos no formato delta # 
    def load(self):
        df = (self.spark.readStream
                        .format('delta')
                        .option("readChangeFeed", "true")
                        .table(self.from_table))
        return df
    
    # grava os arquivos silver, mas para cada batch usa o upsert
    def save(self, df):
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_location)
                    .foreachBatch(lambda df, batchID: self.upsert(df) )
                 )
        return stream.start()
    
    def upsert(self, df):
        # deltatable é a silver corrente (s)
        (self.deltatable
             .alias("s")
             .merge(df.alias("d"), f"s.{self.id_field} = d.{self.id_field} and d._change_type <> 'update_preimage' ")
             .whenMatchedDelete      (condition = "d._change_type = 'delete'")
             .whenMatchedUpdateAll   (condition = "d._change_type = 'update_postimage'")
             .whenNotMatchedInsertAll(condition = "d._change_type = 'insert'")
             .execute())

    def execute(self):
        df = self.load()
        return self.save(df)
    
class IngestorCubo:
    def __init__(self, spark, catalog, schemaname, tablename):
        self.spark      = spark
        self.catalog    = catalog
        self.schemaname = schemaname
        self.tablename  = tablename
        self.table      = f"{catalog}.{schemaname}.{tablename}"
        self.set_query()

    def set_query(self):
        self.query = utils.import_query(f"{self.tablename}.sql")

    def load(self, **kwargs):
        df = self.spark.sql(self.query.format(**kwargs))
        return df
    
    def save(self, df, dt_ref):
        self.spark.sql(f"DELETE FROM {self.table} WHERE dtRef = '{dt_ref}'")
        (df.write
           .mode("append")
           .saveAsTable(self.table))
        
    def backfill(self, dt_start, dt_stop):
        dates = utils.date_range(dt_start, dt_stop)

        if not utils.table_exists(self.spark, self.catalog, self.schemaname, self.tablename):
            df = self.load(dt_ref=dates.pop(0))
            df.write.saveAsTable(self.table)

        for dt in tqdm.tqdm(dates):
            df = self.load(dt_ref=dt)
            self.save(df=df, dt_ref=dt)

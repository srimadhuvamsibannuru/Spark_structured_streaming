# Databricks notebook source
class bronze():
    def __init__(self):
        self.dir = "dbfs:/FileStore/tables/"

    def getschema(self):
        return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>"""
    def read_invoices(self):
        return (spark.readStream
                .format("json")\
                .schema(self.getschema())\
                .option("cleanSource","Archive")\
                .option("SourceArchiveDir",f"{self.dir}/poc/archive")\
                .load(f"{self.dir}/spark_structured_streaming/invoices/"))
    def process(self):
        invoice_df = self.read_invoices()
        display(invoice_df)
        query = (invoice_df.writeStream
                    .queryName("invoicesquery_1")\
                    .option("CheckPointLocation",f"{self.dir}/cpl_1/logs")\
                    .outputMode("append")\
                    .toTable("brinvoices_1"))
        return query

# COMMAND ----------

class silver():
    def __init__(self):
        self.dir = "FileStore/tables/spark_structured_streaming"
    def read_inv(self):
        return (spark.readStream.table("brinvoices_1"))
    def explode_df(self,inv_df):
        from pyspark.sql.functions import explode,expr 
        return (inv_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                      "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                      "DeliveryAddress.State","DeliveryAddress.PinCode", 
                                      "explode(InvoiceLineItems) as LineItem"))
    def flattend_df(self,explode_df):
        from pyspark.sql.functions import expr
        return (explode_df.withColumn("ItemCode", expr("LineItem.ItemCode"))
                        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
                        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
                        .withColumn("ItemQty", expr("LineItem.ItemQty"))
                        .withColumn("TotalValue", expr("LineItem.TotalValue"))
                        .drop("LineItem"))
    def append_data(self,flattend_df):
        return (flattend_df.writeStream
                 .format("delta")\
                 .option("checkpointLocation",f"{self.dir}/cpl_1/logs")\
                 .queryName("test_1")\
                 .outputMode("append")\
                 .toTable("slinvoices_1"))
    def process(self):
        inv_df = self.read_inv()
        explode_df = self.explode_df(inv_df)
        flatt_df = self.flattend_df(explode_df)
        app = self.append_data(flatt_df)
        return app


# COMMAND ----------

br = bronze()
sl = silver()
b = br.process()
s = sl.process()

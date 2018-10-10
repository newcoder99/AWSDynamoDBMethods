using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using System;
using System.Collections.Generic;

namespace ConsoleApp2
{
    internal class Program
    {
        private AmazonDynamoDBClient client;
        public Program()
        {
            string awsAccessKeyId = "Your Access Key Id";
            string awsSecretAccessKey = "Your Secret Access Key";
            BasicAWSCredentials credentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
            client = new AmazonDynamoDBClient(credentials, RegionEndpoint.USEast1);
        }
        private static void Main(string[] args)
        {
            Program p = new Program();
            //string str = p.FuncCreateTableDynamoDBAsync("TBL_SPLITWISEBOTCONFIGVALUES");
            string status = p.FuncIsTableReadOnly("TBL_SPLITWISEBOTCONFIGVALUES");
            bool IsInserted = p.FuncInsertData("TBL_SPLITWISEBOTCONFIGVALUES");
            p.FuncReadTable("TBL_SPLITWISEBOTCONFIGVALUES", true);
        }




        #region Low Level Document .NET Core Methods
        /// <summary>
        /// Method to create table in Dynamo DB
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>RequestId is not empty it means table is created or table doesnot exists</returns>
        public string FuncCreateTableDynamoDBAsync(string tableName)
        {
            string requestId = string.Empty;
            try
            {

                System.Threading.Tasks.Task<ListTablesResponse> currentTablesAsync = client.ListTablesAsync();
                List<string> lstTableNames = currentTablesAsync.Result.TableNames;
                if (!lstTableNames.Contains(tableName))
                {
                    CreateTableRequest request = new CreateTableRequest
                    {
                        TableName = "TBL_SPLITWISEBOTCONFIGVALUES",
                        AttributeDefinitions = new List<AttributeDefinition>
                          {
                            new AttributeDefinition
                            {
                              AttributeName = "KEY",
                              // "S" = string, "N" = number, and so on.
                              AttributeType = "S"
                            },
                            new AttributeDefinition
                            {
                              AttributeName = "TYPE",
                              AttributeType = "S"
                            }
                          },
                        KeySchema = new List<KeySchemaElement>
                          {
                            new KeySchemaElement
                            {
                              AttributeName = "KEY",
                              // "HASH" = hash key, "RANGE" = range key.
                              KeyType = "HASH"
                            },
                            new KeySchemaElement
                            {
                              AttributeName = "TYPE",
                              KeyType = "RANGE"
                            },
                          },
                        ProvisionedThroughput = new ProvisionedThroughput
                        {
                            ReadCapacityUnits = 10,
                            WriteCapacityUnits = 5
                        },
                    };

                    System.Threading.Tasks.Task<CreateTableResponse> response = client.CreateTableAsync(request);

                    requestId = response.Result.ResponseMetadata.RequestId.ToString();

                }
                return requestId;
            }
            catch (Exception)
            {
                return requestId;
            }
        }
        /// <summary>
        /// Method to check if table is ready to be modified
        /// </summary>
        /// <param name="tableName">pass table name</param>
        /// <returns>When the status is set to ACTIVE, the table is ready to be modified. </returns>
        public string FuncIsTableReadOnly(string tableName)
        {
            string status = string.Empty;
            try
            {
                do
                {
                    // Wait 5 seconds before checking (again).
                    System.Threading.Thread.Sleep(TimeSpan.FromSeconds(5));

                    try
                    {
                        System.Threading.Tasks.Task<DescribeTableResponse> response = client.DescribeTableAsync(new DescribeTableRequest
                        {
                            TableName = tableName
                        });



                        status = response.Result.Table.TableStatus;
                    }
                    catch (ResourceNotFoundException)
                    {
                        // DescribeTable is eventually consistent. So you might
                        //   get resource not found. 
                    }

                } while (status != TableStatus.ACTIVE);
                return status;
            }
            catch (Exception)
            {
                return status;
            }
        }
        /// <summary>
        /// Method to insert data into DynamoDB
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>True if data is inserted</returns>
        public bool FuncInsertData(string tableName)
        {
            bool IsInserted = false;
            try
            {


                PutItemRequest request1 = new PutItemRequest
                {
                    TableName = tableName,
                    Item = new Dictionary<string, AttributeValue>
                  {
                    { "KEY", new AttributeValue { S = "2" }},
                    { "TYPE", new AttributeValue { S = "23" }},
                    { "VALUE", new AttributeValue { S = "Sample" }}

                  }
                };

                client.PutItemAsync(request1);
                IsInserted = true;
                return IsInserted;
            }
            catch (Exception)
            {
                return IsInserted;
            }
        }
        #endregion


        #region Document Model .NET Core Methods.
        /// <summary>
        /// Return the table contents
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="isDocumentModelType"> true for Document Model and false for Local Mode</param>
        /// <returns>Array of Values</returns>
        public Document[] FuncReadTable(string tableName, bool isDocumentModelType)
        {
            if (isDocumentModelType)
            {
                try
                {
                    Table table = Table.LoadTable(client, tableName);
                    ScanFilter scanFilter = new ScanFilter();
                    Search getAllItems = table.Scan(scanFilter);
                    Document[] allItems = getAllItems.GetRemainingAsync().Result.ToArray();
                    return allItems;
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                Dictionary<string, AttributeValue> dictObj = new Dictionary<string, AttributeValue>
                {
                    { "1", null }
                };
                try
                {
                    System.Threading.Tasks.Task<GetItemResponse> request = client.GetItemAsync(tableName, dictObj);


                }
                catch (Exception)
                {

                }
                return null;
            }
        }
        #endregion

    }
}

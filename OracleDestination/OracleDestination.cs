using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.Globalization;

using System.IO;

namespace DavidStafford.Dts.Pipeline.OracleDestination
{
    [DtsPipelineComponent
    (
     DisplayName = "David Stafford Oracle Destination",
     ComponentType = ComponentType.DestinationAdapter,
     Description = "Insert Data stream into Oracle",
     IconResource = "DavidStafford.Dts.Pipeline.OracleDestination.OracleDest.ico"
    )]
    public class OracleDestination : PipelineComponent
    {
        #region variables

            #region design variables

            private const string COMPONENT_NAME = "Oracle Destination";
            private const string COMPONENT_DESCRIPTION = "Insert into an Oracle destination via ODP.Net";

            private const string CONNECTION_NAME = "ADO Net Connection";
            private const string CONNECTION_DESCRIPTION = "ADO Net Connection via ODP.NET";

            private const string INPUT_NAME = "Oracle Input";
            private const string INPUT_DESCRIPTION = "Oracle Input";
            private const string ORACLE_TYPE = "OracleType";

            private const string TABLE_NAME = "Table Name (Required)";
            private const string TABLE_DESCRIPTION = "Name of the table that the data will be inserted into";
            private const string PARTITION_NAME = "Partition Name (Optional)";
            private const string PARTITION_DESCRIPTION = "If required, enter the name of the partition that will be inserted into";
            private const string BATCH_SIZE = "Batch Size (Required)";
            private const string BATCH_SIZE_DESCRIPTION = "Batch size / Array Bind Count";
            private const string PERFORM_AS_TRANSACTION_NAME = "Perform in single transaction";
            private const string PERFORM_AS_TRANSACTION_DESCRIPTION = "True: Commit at end.. False: Commit for each batch size";

            private OracleConnection odpConnection;

            #endregion

            #region runtime variables

            private OracleTransaction odpTran;
            private OracleCommand odpCmd;
            private OracleParameter odpParam;

            private int batchSize;
            private string tableName;
            private string partitionName;
            private bool perfromAsTransaction;

            private int[] inputIndexes;
            private List<ColumnInfo> columnInfos;

            private int currentRow;
            private uint rowCount;
            private object[,] rowsObject;
            private int colsCount;

            #endregion

        #endregion

        #region design time

        public override void ProvideComponentProperties()
        {
            base.ProvideComponentProperties();
            this.RemoveAllInputsOutputsAndCustomProperties();
            this. ComponentMetaData.RuntimeConnectionCollection.RemoveAll();

            this.ComponentMetaData.Name = COMPONENT_NAME;
            this.ComponentMetaData.Description = COMPONENT_DESCRIPTION;

            this.GetRuntimeConnection();
            this.AddComponentInputs();
            this.EnableExternalMetadata();
            this.AddCustomProperties();
        }

        //Setup ODP.NET Connection
        private void GetRuntimeConnection()
        {
            IDTSRuntimeConnection100 odpConn;
            try
            {
                odpConn = this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME];
            }
            catch (Exception)
            {
                odpConn = this.ComponentMetaData.RuntimeConnectionCollection.New();
                odpConn.Name = CONNECTION_NAME;
                odpConn.Description = CONNECTION_DESCRIPTION;
            }
        }

        //Setup Oracle Input
        private void AddComponentInputs()
        {
            var input = this.ComponentMetaData.InputCollection.New();
            input.Name = INPUT_NAME;
            input.Description = INPUT_DESCRIPTION;
            input.HasSideEffects = true;
        }

        //Enable External Metadata -- Seen on Column mapping as the external columns
        private void EnableExternalMetadata()
        {
            var input = this.ComponentMetaData.InputCollection[INPUT_NAME];
            input.ExternalMetadataColumnCollection.IsUsed = true;
            this.ComponentMetaData.ValidateExternalMetadata = true;
        }

        private void AddCustomProperties()
        {
            Func<string, string, IDTSCustomProperty100> addProperty = delegate(string name, string description)
            {
                var property = this.ComponentMetaData.CustomPropertyCollection.New();
                property.Name = name;
                property.Description = description;
                property.ExpressionType = DTSCustomPropertyExpressionType.CPET_NOTIFY;

                return property;
            };

            addProperty(TABLE_NAME, TABLE_DESCRIPTION).Value = string.Empty;            
            addProperty(PARTITION_NAME, PARTITION_DESCRIPTION).Value = string.Empty;
            addProperty(BATCH_SIZE, BATCH_SIZE_DESCRIPTION).Value = "1000";
            addProperty(PERFORM_AS_TRANSACTION_NAME, PERFORM_AS_TRANSACTION_DESCRIPTION).Value = (object)true;
        }

        public override void ReinitializeMetaData()
        {
            base.ReinitializeMetaData();

            var input = base.ComponentMetaData.InputCollection[INPUT_NAME];
            input.ExternalMetadataColumnCollection.RemoveAll();
            input.InputColumnCollection.RemoveAll();

            this.GenerateExternalColumns();

        }

        private void GenerateExternalColumns()
        {
            var tableName = this.GetMemberTableName();
            using (DataTable metaData = this.GetMemberTableMetaData(tableName))
            {
                if (metaData != null)
                {
                    var input = this.ComponentMetaData.InputCollection[INPUT_NAME];
                    var external = input.ExternalMetadataColumnCollection;

                    var count = metaData.Rows.Count;
                    for (int i = 0; i < count; i++)
                    {
                        var dataRow = metaData.Rows[i];
                        var externalColumn = external.NewAt(i);
                        externalColumn.Name = (string)dataRow["ColumnName"];
                        var type = (Type)dataRow["DataType"];
                        var dataType = PipelineComponent.DataRecordTypeToBufferType(type);
                        if ((bool)dataRow["IsLong"])
                        {
                            switch (dataType)
                            {
                                case DataType.DT_BYTES:
                                    dataType = DataType.DT_IMAGE;
                                    break;
                                case DataType.DT_STR:
                                    dataType = DataType.DT_TEXT;
                                    break;
                                case DataType.DT_WSTR:
                                    dataType = DataType.DT_NTEXT;
                                    break;
                            }
                        }
                        int length = 0;
                        if (dataType == DataType.DT_WSTR || dataType == DataType.DT_STR || dataType == DataType.DT_BYTES || dataType == DataType.DT_IMAGE || dataType == DataType.DT_NTEXT || dataType == DataType.DT_TEXT)
                        {
                            length = (int)dataRow["ColumnSize"];
                        }
                        int codePage = 0;
                        if (dataType == DataType.DT_STR)
                        {
                            codePage = -1;
                        }
                        int precision = 0;
                        int num = 0;
                        if (dataType == DataType.DT_NUMERIC || dataType == DataType.DT_DECIMAL)
                        {
                            if (dataRow.IsNull("NumericPrecision"))
                            {
                                precision = 29;
                            }
                            else
                            {
                                precision = (int)Convert.ToInt16(dataRow["NumericPrecision"]);
                            }
                            if (dataRow.IsNull("NumericScale"))
                            {
                                num = 0;
                            }
                            else
                            {
                                num = (int)Convert.ToInt16(dataRow["NumericScale"]);
                            }
                            if (num == 255)
                            {
                                num = 0;
                            }
                        }
                        externalColumn.DataType = dataType;
                        externalColumn.Length = length;
                        externalColumn.Precision = precision;
                        externalColumn.Scale = num;
                        externalColumn.CodePage = codePage;

                        IDTSCustomProperty100 propType = externalColumn.CustomPropertyCollection.New();
                        propType.Name = ORACLE_TYPE;
                        propType.Value = dataRow["ProviderType"];
                    }
                }
            }
        }

        private string GetMemberTableName()
        {
            var tableName = ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString();
            if (string.IsNullOrEmpty(tableName))
            {
                FireEvent(EventType.Error, "Table name needs to be provided");
                throw new PipelineComponentHResultException(HResults.DTS_E_ADODESTFAILEDTOACQUIRECONNECTION);
            }
            return tableName;
        }

        private DataTable GetMemberTableMetaData(string tableName)
        {
            DataTable metaData = null;
            if (!string.IsNullOrEmpty(tableName))
            {
                using (OracleCommand cmd = odpConnection.CreateCommand())
                {
                    cmd.CommandType = CommandType.TableDirect;
                    cmd.CommandText = tableName;
                    metaData = cmd.ExecuteReader(CommandBehavior.SchemaOnly).GetSchemaTable();
                }
            }
            return metaData;
        }


        #endregion

        #region runtime

        public override void PreExecute()
        {
            base.PreExecute();

            var input = base.ComponentMetaData.InputCollection[INPUT_NAME];
            colsCount = input.InputColumnCollection.Count;

            StoreInputColumns(input,colsCount);
            StoreUserVariables();

            odpCmd = new OracleCommand();
            odpCmd.Connection = odpConnection;
            odpCmd.BindByName = true;
            odpCmd.CommandType = CommandType.Text;
            odpCmd.ArrayBindCount = batchSize;
            if (perfromAsTransaction)
            {
                odpTran = odpConnection.BeginTransaction();
                odpCmd.Transaction = odpTran;
            }

            string colsList = null;
            string paramsList = null;
            for (int colIndex = 0; colIndex <= colsCount - 1; colIndex++)
            {
                var externalColumnName = columnInfos[colIndex].ExternalColumnName;
                var externalOracleDbType = columnInfos[colIndex].ExternalOracleDbType;
                if (!string.IsNullOrEmpty(colsList))
                {
                    colsList += ", ";
                    paramsList += ", ";
                }
                colsList += string.Format("\"{0}\"", externalColumnName);
                paramsList += string.Format(":{0}", externalColumnName);

                odpParam = new OracleParameter(":" + externalColumnName, externalOracleDbType, ParameterDirection.Input);
                odpCmd.Parameters.Add(odpParam);
            }
            if (string.IsNullOrEmpty(partitionName))
            {
                odpCmd.CommandText = string.Format("INSERT INTO {0} ( {1} ) VALUES ( {2} )", tableName, colsList, paramsList);
            }
            else
            {
                odpCmd.CommandText = string.Format("INSERT INTO {0} PARTITION({1})( {2} ) VALUES ( {3} )", tableName, partitionName, colsList, paramsList); 
            }
        }

        //Use user inputs to initalise run time variables
        private void StoreUserVariables()
        {
            batchSize = Convert.ToInt32(base.ComponentMetaData.CustomPropertyCollection[BATCH_SIZE].Value.ToString());
            tableName = base.ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString();
            partitionName = base.ComponentMetaData.CustomPropertyCollection[PARTITION_NAME].Value.ToString();
            currentRow = 0;
            rowsObject = new object[colsCount, batchSize];
            rowCount = 0;
            perfromAsTransaction = (bool)base.ComponentMetaData.CustomPropertyCollection[PERFORM_AS_TRANSACTION_NAME].Value;  
        }

        //Store all input column related data into the instantiated ColumnInfo class
        private void StoreInputColumns(IDTSInput100 input, int colsCount)
        {
            inputIndexes = new int[colsCount];
            columnInfos = new List<ColumnInfo>(this.ComponentMetaData.InputCollection[INPUT_NAME].InputColumnCollection.Count);
            for (int i = 0; i <= colsCount - 1; i++)
            {
                //Add each input column into an INT array                
                //Map all input and external input column data
                var col = input.InputColumnCollection[i];
                var externalColumnName = input.ExternalMetadataColumnCollection.GetObjectByID(input.InputColumnCollection[i].ExternalMetadataColumnID).Name;
                var externalOracleDbType = (OracleDbType)input.ExternalMetadataColumnCollection.GetObjectByID(input.InputColumnCollection[i].ExternalMetadataColumnID).CustomPropertyCollection[ORACLE_TYPE].Value;
                columnInfos.Add(new ColumnInfo(col.Name,
                                               col.DataType,
                                               col.LineageID,
                                               this.BufferManager.FindColumnByLineageID(input.Buffer, col.LineageID),
                                               col.Length,
                                               col.Precision,
                                               col.Scale,
                                               externalColumnName,
                                               externalOracleDbType));
            }
        }

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            try
            {
                while (buffer.NextRow())
                {
                    for (int i = 0; i <= colsCount - 1; i++)
                    {
                        rowsObject[i, currentRow] = buffer[i];
                    }
                    if (currentRow == batchSize - 1)
                    {
                        for (int i = 0; i <= colsCount - 1; i++)
                        {
                            object[] column = new object[batchSize];
                            for (int i2 = 0; i2 <= batchSize - 1; i2++)
                            {
                                column[i2] = rowsObject[columnInfos[i].BufferIndex, i2];
                            }
                            odpCmd.Parameters[":" + columnInfos[i].ExternalColumnName].Value = column;
                        }
                        odpCmd.ExecuteNonQuery();
                        currentRow = -1;
                    }
                    currentRow++;
                    rowCount++;
                }
            }
            catch (Exception e)
            {
                if (perfromAsTransaction)
                {
                    odpTran.Rollback();
                }
                throw (e);                
            }             
        }

        public override void PostExecute()
        {
            base.PostExecute();
            try
            {
                if (currentRow > 0)
                {
                    var finalBatchSize = currentRow;

                    for (int i = 0; i <= colsCount - 1; i++)
                    {
                        odpCmd.ArrayBindCount = finalBatchSize;
                        object[] column = new object[finalBatchSize];
                        for (int i2 = 0; i2 <= finalBatchSize - 1; i2++)
                        {
                            column[i2] = rowsObject[i, i2];
                        }
                        odpCmd.Parameters[":" + columnInfos[i].ExternalColumnName].Value = column;
                    }
                    odpCmd.ExecuteNonQuery();
                }
                if (perfromAsTransaction)
                {
                    odpTran.Commit();
                }
                ComponentMetaData.IncrementPipelinePerfCounter(103, rowCount);
            }
            catch (Exception e)
            {
                if (perfromAsTransaction)
                {
                    odpTran.Rollback();
                }
                throw (e);
            }
        }

        #endregion
        
        #region Connection handling

        public override void AcquireConnections(object transaction)
        {
            

            if (!string.IsNullOrEmpty(this.ComponentMetaData.CustomPropertyCollection[TABLE_NAME].Value.ToString()))
            {
                try
                {
                    odpConnection = (OracleConnection)this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME].ConnectionManager.AcquireConnection(transaction);
                    if (odpConnection.State == System.Data.ConnectionState.Closed)
                    {
                        odpConnection.Open();
                    }
                }
                catch (Exception)
                {
                    throw new PipelineComponentHResultException(HResults.DTS_E_ADODESTFAILEDTOACQUIRECONNECTION);
                }
            }
            base.AcquireConnections(transaction);
        }

        #endregion

        #region DTS Error Handling

        enum EventType
        {
            Information = 0,
            Progress = 1,
            Warning = 2,
            Error = 3
        }
        private void FireEvent(EventType eventType, string description)
        {
            bool cancel = false;

            switch (eventType)
            {
                case EventType.Information:
                    this.ComponentMetaData.FireInformation(0, this.ComponentMetaData.Name, description, string.Empty, 0, ref cancel);
                    break;
                case EventType.Progress:
                    throw new NotImplementedException("Progress messages are not implemented");
                case EventType.Warning:
                    this.ComponentMetaData.FireWarning(0, this.ComponentMetaData.Name, description, string.Empty, 0);
                    break;
                case EventType.Error:
                    this.ComponentMetaData.FireError(0, this.ComponentMetaData.Name, description, string.Empty, 0, out cancel);
                    break;
                default:
                    this.ComponentMetaData.FireError(0, this.ComponentMetaData.Name, description, string.Empty, 0, out cancel);
                    break;
            }
        }        

        public override DTSValidationStatus Validate()
        {
            try
            {
                var status = base.Validate();

                if (status != DTSValidationStatus.VS_ISVALID)
                {
                    return status;
                }

                if ((status = ValidateConnection()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                if ((status = ValidateCustomProperties()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                if ((status = ValidateInputs()) != DTSValidationStatus.VS_ISVALID)
                    return status;

                return status;

            }
            catch (Exception)
            {
                return DTSValidationStatus.VS_ISCORRUPT;
            }
        }

        private DTSValidationStatus ValidateConnection()
        {
            if (this.ComponentMetaData.RuntimeConnectionCollection[CONNECTION_NAME].ConnectionManager == null)
            {
                FireEvent(EventType.Error, "No connection manager");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            //Property is valid
            return DTSValidationStatus.VS_ISVALID;
        }

        private DTSValidationStatus ValidateCustomProperties()
        {
            if (this.ComponentMetaData.CustomPropertyCollection[TABLE_NAME] == null)
            {
                FireEvent(EventType.Error, "No table name provided");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            var batchSize = this.ComponentMetaData.CustomPropertyCollection[BATCH_SIZE].Value.ToString();
            int intBatchSize;
            if (batchSize == null || !(int.TryParse(batchSize,out intBatchSize)))
            {
                FireEvent(EventType.Error, "Invalid Batch Size");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (intBatchSize < 50)
            {
                FireEvent(EventType.Warning, "Batch Size is set at " + batchSize + ". This could lead to slow performance");
            }


            return DTSValidationStatus.VS_ISVALID;
        }

        //Validate Inputs  Both InputColumns and ExternalColumns
        private DTSValidationStatus ValidateInputs()
        {
            bool cancel;

            //Component should have a single input
            if (this.ComponentMetaData.InputCollection.Count != 1)
            {
                ErrorSupport.FireErrorWithArgs(HResults.DTS_E_INCORRECTEXACTNUMBEROFINPUTS, out cancel, 1);
                return DTSValidationStatus.VS_ISCORRUPT;
            }

            //Check input has columns
            var input = ComponentMetaData.InputCollection[INPUT_NAME];
            if (input.ExternalMetadataColumnCollection.Count == 0)
            {
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            //Check input columns are valid
            if (!this.ComponentMetaData.AreInputColumnsValid)
            {
                input.InputColumnCollection.RemoveAll();
                return DTSValidationStatus.VS_NEEDSNEWMETADATA;
            }

            //Input truncation disposition not supported
            if (input.TruncationRowDisposition != DTSRowDisposition.RD_NotUsed)
            {
                ErrorSupport.FireError(HResults.DTS_E_ADODESTINPUTTRUNDISPNOTSUPPORTED, out cancel);
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        #endregion

    }

    internal class ColumnInfo
    {
        private string _columnName = string.Empty;
        private DataType _dataType = DataType.DT_STR;
        private int _lineageID = 0;
        private int _bufferIndex = 0;
        private int _precision = 0;
        private int _scale = 0;
        private int _length;
        private string _externalColumnName = string.Empty;
        private OracleDbType _externalOracleDbType = OracleDbType.Char;

        public ColumnInfo(string columnName, DataType dataType, int lineageID, int bufferIndex, int length, int precision, int scale, string externalColumnName, OracleDbType externalOracleDbType)
        {
            _columnName = columnName;
            _dataType = dataType;
            _lineageID = lineageID;
            _bufferIndex = bufferIndex;
            _precision = precision;
            _scale = scale;
            _length = length;
            _externalColumnName = externalColumnName;
            _externalOracleDbType = externalOracleDbType;
        }

        public int BufferIndex
        { get { return _bufferIndex; } }

        public DataType ColumnDataType
        { get { return _dataType; } }

        public int LineageID
        { get { return _lineageID; } }

        public string ColumnName
        { get { return _columnName; } }

        public int Precision
        { get { return _precision; } }

        public int Length
        { get { return _length; } }

        public int Scale
        { get { return _scale; } }

        public string ExternalColumnName
        { get { return _externalColumnName; } }

        public OracleDbType ExternalOracleDbType
        { get { return _externalOracleDbType; } }


    }

}

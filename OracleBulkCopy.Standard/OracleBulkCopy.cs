using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Oracle.ManagedDataAccess.Client.BulkCopy
{
    public class OracleBulkCopy : IDisposable
    {
        // https://github.com/Microsoft/referencesource/blob/master/System.Data/System/Data/SqlClient/SqlBulkCopy.cs
        // https://stackoverflow.com/questions/47942691/how-to-make-a-bulk-insert-using-oracle-managed-data-acess-c-sharp
        // https://github.com/DigitalPlatform/dp2/blob/master/DigitalPlatform.rms.db/OracleBulkCopy.cs
        // https://msdn.microsoft.com/en-us/library/system.data.oracleclient.oracletype(v=vs.110).aspx

        private OracleConnection _connection;
        private OracleTransaction _externalTransaction { get; set; }

        /// <summary>
        /// Set to TRUE if the BulkCopy object was not instantiated with an external OracleConnection
        /// and thus it is up to the BulkCopy object to open and close connections
        /// </summary>
        private bool _ownsTheConnection = false;

        public OracleBulkCopy(string connectionString) : this(new OracleConnection(connectionString))
        {
            _ownsTheConnection = true;
        }

        public OracleBulkCopy(OracleConnection connection) : this(connection, null) { }

        public OracleBulkCopy(OracleConnection connection, OracleTransaction transation = null)
        {
            _connection = connection;
            _externalTransaction = transation;
        }


        private string _destinationTableName;
        
        public string DestinationTableName {
            get { return _destinationTableName; }
            set
            {
                if (value == null || value.Length == 0)
                    throw new ArgumentException("Destination Table Name cannot be null or empty string");
                _destinationTableName = value;
            }
        }


        private int _batchSize = 0;

        public int BatchSize {
            get { return _batchSize; }
            set {
                if (value < 0)
                    throw new ArgumentException("Batch Size must be a positive integer");
                _batchSize = value;
            }
        }

        private bool UploadEverythingInSingleBatch { get { return _batchSize == 0; } }




        private void ValidateConnection()
        {
            if (_connection == null)
                throw new Exception("Oracle Database Connection is required");

            if (_externalTransaction != null && _externalTransaction.Connection != _connection)
                throw new Exception("Oracle Transaction mismatch with Oracle Database Connection");
        }

        private void OpenConnection()
        {
            if (this._ownsTheConnection && _connection.State != ConnectionState.Open)
                _connection.Open();
        }


        // TODO: Implement WriteToServer for a IDataReader input


        public void WriteToServer(DataTable table)
        {
            // https://stackoverflow.com/questions/47942691/how-to-make-a-bulk-insert-using-oracle-managed-data-acess-c-sharp
            // https://github.com/Microsoft/referencesource/blob/master/System.Data/System/Data/SqlClient/SqlBulkCopy.cs

            if (table == null)
                throw new ArgumentNullException("table");

            // TODO: Validate TableName to prevent SQL Injection
            // https://oracle-base.com/articles/10g/dbms_assert_10gR2
            // SELECT SYS.DBMS_ASSERT.qualified_sql_name('object_name') FROM dual;

            if (UploadEverythingInSingleBatch)
                WriteToServerInSingleBatch(table);
            else
                WriteToServerInMultipleBatches(table);
        }

        private void WriteToServerInSingleBatch(DataTable table)
        {
            // Build the command string
            string commandText = BuildCommandText(table);

            WriteSingleBatchOfData(table, 0, commandText, table.Rows.Count);
        }


        private void WriteToServerInMultipleBatches(DataTable table)
        {
            // Calculate number of batches
            int numBatchesRequired = (int)Math.Ceiling(table.Rows.Count / (double)BatchSize);

            // Build the command string
            string commandText = BuildCommandText(table);

            for (int i = 0; i < numBatchesRequired; i++)
            {
                int skipOffset = i * BatchSize;
                int batchSize = Math.Min(BatchSize, table.Rows.Count - skipOffset);
                WriteSingleBatchOfData(table, skipOffset, commandText, batchSize);
            }
        }

        private string BuildCommandText(DataTable table)
        {
            // Build the command string
            string commandText = "Insert Into " + DestinationTableName + " ( @@ColumnList@@ ) Values ( @@ValueList@@ )";
            string columnList = GetColumnList(table);
            string valueList = GetValueList(table);

            // Replace the placeholders with actual values
            commandText = commandText.Replace("@@ColumnList@@", columnList);
            commandText = commandText.Replace("@@ValueList@@", valueList);

            // TODO: Validate commandText to prevent SQL Injection
            // https://oracle-base.com/articles/10g/dbms_assert_10gR2

            return commandText;
        }


        private void WriteSingleBatchOfData(DataTable table, int skipOffset, string commandText, int batchSize)
        {
            // Get array of row data for all columns in the table
            List<OracleParameter> parameters = GetParameters(table, batchSize, skipOffset);

            // Create the OracleCommand and bind the data
            OracleCommand cmd = _connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.ArrayBindCount = batchSize;
            parameters.ForEach(p => cmd.Parameters.Add(p));

            // Validate and open the connection
            ValidateConnection();
            OpenConnection();

            // Upload the data
            cmd.ExecuteNonQuery();

            // Commit Transaction
            //CommitTransaction(); // ????
        }




        private List<OracleParameter> GetParameters(DataTable data, int batchSize, int skipOffset = 0)
        {

            List<OracleParameter> parameters = new List<OracleParameter>();
            foreach (DataColumn c in data.Columns)
            {

                OracleDbType dbType = GetOracleDbTypeFromDotnetType(c.DataType);

                // https://stackoverflow.com/a/23735845/2442468
                // https://stackoverflow.com/a/17595403/2442468

                var columnData = data.AsEnumerable().Select(r => r.Field<object>(c.ColumnName));
                object[] paramDataArray = (UploadEverythingInSingleBatch)
                    ? columnData.ToArray()
                    : columnData.Skip(skipOffset).Take(batchSize).ToArray();

                OracleParameter param = new OracleParameter();
                param.OracleDbType = dbType;
                param.Value = paramDataArray;

                parameters.Add(param);
            }

            return parameters;
        }




        private string GetColumnList(DataTable data)
        {
            string[] columnNames = data.Columns.Cast<DataColumn>().Select(x => x.ColumnName).ToArray();
            string columnList = string.Join(",", columnNames);
            return columnList;
        }

        private string GetValueList(DataTable data)
        {
            const string Delimiter = ", ";
            
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= data.Columns.Count; i++)
            {
                sb.Append(string.Format(":{0}", i));
                sb.Append(Delimiter);
            }

            sb.Length -= Delimiter.Length;

            string valueList = sb.ToString();
            return valueList;
        }




        public void Dispose()
        {
            if (_connection != null)
            {
                // Only close the connection if the BulkCopy instance owns the connection
                if(this._ownsTheConnection)
                    _connection.Dispose();
                
                // Always set to null
                _connection = null;
            }
        }

        public void Close() { Dispose(); }


        private static OracleDbType GetOracleDbType(object o) 
        {
            // https://stackoverflow.com/questions/1583150/c-oracle-data-type-equivalence-with-oracledbtype#1583197
            // https://docs.oracle.com/cd/B19306_01/win.102/b14307/OracleDbTypeEnumerationType.htm
            // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/oracle-data-type-mappings

            if (o is byte[]) return OracleDbType.Blob;
            if (o is string) return OracleDbType.Varchar2;
            if (o is DateTime) return OracleDbType.Date;
            if (o is decimal) return OracleDbType.Decimal;
            if (o is Int32) return OracleDbType.Int32;

            if (o is Int64) return OracleDbType.Int64;
            if (o is Int16) return OracleDbType.Int16;
            if (o is sbyte) return OracleDbType.Byte;
            if (o is byte) return OracleDbType.Int16;    // <== unverified
            if (o is float) return OracleDbType.Single;
            if (o is double) return OracleDbType.Double;


            // Tylers
            //if (o is bool) return OracleDbType.Boolean;
            //if (o is char) return OracleDbType.Char;


            return OracleDbType.Varchar2;
        }

        private static OracleDbType GetOracleDbTypeFromDotnetType(Type t)
        {

            if (t == typeof(byte[])) return OracleDbType.Blob;
            if (t == typeof(string)) return OracleDbType.Varchar2;
            if (t == typeof(DateTime)) return OracleDbType.Date;
            if (t == typeof(decimal)) return OracleDbType.Decimal;
            if (t == typeof(Int32)) return OracleDbType.Int32;

            if (t == typeof(Int64)) return OracleDbType.Int64;
            if (t == typeof(Int16)) return OracleDbType.Int16;
            if (t == typeof(sbyte)) return OracleDbType.Byte;
            if (t == typeof(byte)) return OracleDbType.Int16;    // <== unverified
            if (t == typeof(float)) return OracleDbType.Single;
            if (t == typeof(double)) return OracleDbType.Double;


            // Tylers
            //if (o is bool) return OracleDbType.Boolean;
            //if (o is char) return OracleDbType.Char;


            return OracleDbType.Varchar2;
        }



    }
}

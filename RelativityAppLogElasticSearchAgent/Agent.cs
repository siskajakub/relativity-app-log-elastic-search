using kCura.Agent;
using kCura.Agent.CustomAttributes;
using Relativity.API;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Runtime.InteropServices;

namespace RelativityAppLogElasticSearchAgent
{
    [Name("App Log Elastic Search Agent")]
    [Guid("74637f18-cb80-4c34-ad8a-d058566ed403")]
    public class Agent : AgentBase
    {
        public override void Execute()
        {
            // Update Security Protocol
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            // Get logger
            Relativity.API.IAPILog _logger = this.Helper.GetLoggerFactory().GetLogger().ForContext<Agent>();

            // Get current Agent ID
            int agentArtifactId = this.AgentID;
            _logger.LogDebug("App Log Elastic Search, current Agent ID: {agentArtifactId}", agentArtifactId.ToString());

            // Display initial message
            this.RaiseMessageNoLogging("Getting Instance Settings.", 10);

            // Get ES URI Instance Settings

            // Get ES authentication API Key Instance Settings

            // Get ES index name Instance Settings (must by lowercase)

            // Get ES index number of replicas Instance Settings

            // Get ES index number of shards Instance Settings

            // Get ES synchronization threshold for one agent run

            // Construct connector to ES cluster
            Nest.ElasticClient elasticClient = null;
            try
            {
                Elasticsearch.Net.StaticConnectionPool pool = new Elasticsearch.Net.StaticConnectionPool(elasticUris, true);
                elasticClient = new Nest.ElasticClient(new Nest.ConnectionSettings(pool).DefaultIndex(elasticIndexName).ApiKeyAuthentication(elasticApiKey[0], elasticApiKey[1]).EnableHttpCompression());
            }
            catch (Exception e)
            {
                _logger.LogError(e, "App Log Elastic Search, Agent ({agentArtifactId}) Elastic Search connection call error ({elasticUris}, {indexName})", agentArtifactId.ToString(), string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName);
                this.RaiseMessageNoLogging(string.Format("Elastic Search connection call error ({0}, {1}).", string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName), 1);
                return;
            }

            // Check ES cluster connection
            Nest.PingResponse pingResponse = elasticClient.Ping();
            if (pingResponse.IsValid)
            {
                _logger.LogDebug("App Log Elastic Search, Agent ({agentArtifactId}), Ping succeeded ({elasticUris}, {indexName})", agentArtifactId.ToString(), string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName);
            }
            else
            {
                _logger.LogError("App Log Elastic Search, Agent ({agentArtifactId}), Ping failed, check cluster health and connection settings ({elasticUris}, {indexName}, {elasticError})", agentArtifactId.ToString(), string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName, pingResponse.DebugInformation);
                this.RaiseMessageNoLogging(string.Format("Elastic Search ping failed, check cluster health and connection settings ({0}, {1}, {2}).", string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName, pingResponse.DebugInformation), 1);
                return;
            }

            // Get latest Log ID in ES
            long logId = 0;

            // Get database context of the instance
            IDBContext instanceContext = Helper.GetDBContext(-1);

            // Synchronize until threshold is reached
            int syncCount = 0;
            while (syncCount < elasticSyncSize)
            {
                try
                {
                    // Get App Log to synchronize
                    SqlParameter logIdParam = new SqlParameter("@logId", logId);
                    DataTable dataTable = instanceContext.ExecuteSqlStatementAsDataTable(@"
                                    SELECT TOP (1000)
	                                    [RelativityLogs].[ID],
	                                    [RelativityLogs].[Message],
	                                    [RelativityLogs].[Level],
	                                    [RelativityLogs].[TimeStamp],
	                                    [RelativityLogs].[Exception],
	                                    [RelativityLogs].[Properties]
                                    FROM [EDDSLogging].[eddsdbo].[RelativityLogs] WITH (NOLOCK)
                                    WHERE [RelativityLogs].[ID] > @logId
                                    ORDER BY [RelativityLogs].[ID] ASC
                                ", new SqlParameter[] { logIdParam });

                    // If there is nothing to synchronize end
                    _logger.LogDebug("App Log Elastic Search, Agent ({agentArtifactId}), App Log row count to synchronize: {count}", agentArtifactId.ToString(), dataTable.Rows.Count.ToString());
                    if (dataTable.Rows.Count == 0)
                    {
                        // Log end of Agent execution
                        _logger.LogDebug("App Log Elastic Search, Agent ({agentArtifactId}), completed, nothing to synchronize", agentArtifactId.ToString());
                        this.RaiseMessageNoLogging("Completed.", 10);
                        return;
                    }
                    // Else synchronize workspace Log with ES index
                    else
                    {
                        // Synchronizing workspace Log with ES index
                        List<Log> logs = new List<Log>();
                        long newLogId = logId;
                        for (int i = 0; i < dataTable.Rows.Count; i++)
                        {
                            // Read Log data
                            Log log = new Log();
                            DataRow dataRow = dataTable.Rows[i];
                            log.LogId = Convert.ToInt64(dataRow["ID"]);
                            log.Message = dataRow["Message"] is DBNull ? default : Convert.ToString(dataRow["Message"]);
                            log.Level = Convert.ToString(dataRow["Level"]);
                            log.TimeStamp = Convert.ToDateTime(dataRow["TimeStamp"]);
                            log.Exception = dataRow["Exception"] is DBNull ? default : Convert.ToString(dataRow["Exception"]);
                            log.Properties = dataRow["Properties"] is DBNull ? default : Convert.ToString(dataRow["Properties"]);
                            logs.Add(log);

                            // Record last Log ID
                            if (newLogId < log.LogId)
                            {
                                newLogId = log.LogId;
                            }

                            // Index data in threshold is reached or we are at the last row
                            if (logs.Count >= 500 || i + 1 >= dataTable.Rows.Count)
                            {
                                try
                                {
                                    Nest.BulkResponse bulkResponse = elasticClient.Bulk(b => b.Index(elasticIndexName).IndexMany(logs, (descriptor, s) => descriptor.Id(s.LogId.ToString())));
                                    if (!bulkResponse.Errors)
                                    {
                                        logs.Clear();
                                        _logger.LogDebug("App Log Elastic Search, Agent ({agentArtifactId}), documents synchronized to Elastic Serach index ({indexName})", agentArtifactId.ToString(), elasticIndexName);
                                    }
                                    else
                                    {
                                        foreach (Nest.BulkResponseItemBase itemWithError in bulkResponse.ItemsWithErrors)
                                        {
                                            _logger.LogError("App Log Elastic Search, Agent ({agentArtifactId}), Elastic Serach bulk index error to index {indexName} ({elasticUris}) on document {docIs}:{docError}", agentArtifactId.ToString(), elasticIndexName, string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), itemWithError.Id, itemWithError.Error.ToString());
                                        }
                                        this.RaiseMessageNoLogging(string.Format("Elastic Serach bulk index error to index {0} ({1}).", elasticIndexName, string.Join(";", elasticUris.Select(x => x.ToString()).ToArray())), 1);
                                        return;
                                    }
                                }
                                catch (Exception e)
                                {
                                    _logger.LogError(e, "App Log Elastic Search, Agent ({agentArtifactId}) Elastic Search bulk index call error ({elasticUris}, {indexName})", agentArtifactId.ToString(), string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName);
                                    this.RaiseMessageNoLogging(string.Format("Elastic Search bulk index call error ({0}, {1}).", string.Join(";", elasticUris.Select(x => x.ToString()).ToArray()), elasticIndexName), 1);
                                    return;
                                }
                            }
                        }

                        // After successful indexing assign new Log ID
                        logId = newLogId;
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "App Log Elastic Search, Agent ({agentArtifactId}), app log table querying error", agentArtifactId.ToString());
                    this.RaiseMessageNoLogging("App log table querying error.", 1);
                    return;
                }

                syncCount += 1000;
            }

            // Log end of Agent execution
            _logger.LogDebug("App Log Elastic Search, Agent ({agentArtifactId}), completed", agentArtifactId.ToString());
            this.RaiseMessageNoLogging("Completed.", 10);
        }

        public override string Name
        {
            get
            {
                return "App Log Elastic Search Agent";
            }
        }
    }
}
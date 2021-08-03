using Relativity.API;
using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;

namespace RelativityAppLogElasticSearchEventHandlers
{
    [Description("App Log Elastic Search Pre Install EventHandler")]
    [Guid("6f3d5282-6bef-4196-be07-bb5bce9e6a62")]

    /*
     * Pre Install EventHandler Class
     * Class's only method Execute() overrides default method and it is executed before App Log Elastic Search application is installed to the workspace
     */
    public class PreInstallEventHandler : kCura.EventHandler.PreInstallEventHandler
    {
        // Minimal required Relativity version
        private string MinRelativityVersion = "10.0.318.5";

        public override kCura.EventHandler.Response Execute()
        {
            // Update Security Protocol
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            // Get logger
            Relativity.API.IAPILog _logger = this.Helper.GetLoggerFactory().GetLogger().ForContext<PreInstallEventHandler>();

            // Init general response
            kCura.EventHandler.Response response = new kCura.EventHandler.Response()
            {
                Success = true,
                Message = ""
            };

            // Get current Workspace ID
            int workspaceId = this.Helper.GetActiveCaseID();
            _logger.LogDebug("App Log Elastic Search, current Workspace ID: {workspaceId}", workspaceId.ToString());

            // Check Workspace ID, allow installation only on instance level
            if (workspaceId != -1)
            {
                _logger.LogError("App Log Elastic Search, not elliglble for workspace installation");

                response.Success = false;
                response.Message = "This application cannot be installed on the Workspace level";
                return response;
            }

            // Get database context of the instance
            IDBContext instanceContext = Helper.GetDBContext(-1);

            // Current Relativity version
            string currentRelativityVersion = "";

            try
            {
                // Get Relativity version
                currentRelativityVersion = instanceContext.ExecuteSqlStatementAsScalar("SELECT [Value] FROM [eddsdbo].[Relativity] WHERE [Key] = 'Version'").ToString();
                _logger.LogDebug("App Log Elastic Search, current Relativity version: {version}", currentRelativityVersion);
                _logger.LogDebug("App Log Elastic Search, minimum required Relativity version: {version}", this.MinRelativityVersion);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "App Log Elastic Search, Pre Install EventHandler Relativity version error");

                response.Success = false;
                response.Message = "Pre Install EventHandler Relativity version error";
                return response;
            }

            // Check Relativity version requirements
            if (new Version(currentRelativityVersion) < new Version(this.MinRelativityVersion))
            {
                _logger.LogError("App Log Elastic Search, old Relativity instance (version: {version})", currentRelativityVersion);

                response.Success = false;
                response.Message = string.Format("This application requires Relativity {0} or later", this.MinRelativityVersion);
                return response;
            }

            // Log end of Pre Install EventHandler
            _logger.LogDebug("App Log Elastic Search, Pre Install EventHandler successfully finished");

            return response;
        }
    }
}
using Microsoft.SqlServer.Dac;
using Mono.Options;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqlDbSchemaCompare
{
    class Program
    {
        private readonly CancellationTokenSource Ctc = new CancellationTokenSource();

        static void Main(string[] args)
        {
            Program p = new Program();
            p.Run(args);

        }
        public string Run(string[] args)
        {
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                Ctc.Cancel();
            };

            var cmd = new Dictionary<string, object> { { "impersonate", false }, { "blob", null } };

            var p = new OptionSet
                        {
                            {
                                "sdp=|SourceDacPath=", "Source Dacpac File path",
                                s => cmd["sdp"] = s
                            },

                            {
                                "scs=|SourceConnectionString=", "source connection string",
                                s => cmd["scs"] = s
                            },
                             {
                                "tcs=|TargetConnectionString=", "Target connection string",
                                t => cmd["tcs"] = t
                            },
                             {
                                "apn=|appName=", "Application Name which is used to log into Db for activities.",
                                s => cmd["appName"] = s
                            },
                              {
                                "u=|username=", "UserName to use to execute this program. e.g. domain\\user",
                                s => cmd["username"] = s
                            },
                            {
                                "p=|password=", "user Password to be used for authentication.",
                                s => cmd["password"] = s.ToString().Trim()
                            },
                            {
                                "h|help", "show this message and exit",
                                s => cmd["help"] = s
                            }
                        };

            p.Parse(args);

            if (cmd.ContainsKey("help") && cmd["help"] != null)
            {
                ShowHelp(p);
                return "0";
            }

            if (cmd.ContainsKey("sdp")
                && cmd.ContainsKey("scs")
                && cmd.ContainsKey("tcs")
                && cmd.ContainsKey("username")
                && cmd.ContainsKey("password"))
            {
                var sdp = cmd["sdp"].ToString();
                var scs = cmd["scs"].ToString();
                var tcs = cmd["tcs"].ToString();
                var user = cmd["username"].ToString();
                var pass = cmd["password"].ToString();

                var rs = MainExec(sdp,scs, tcs, user, pass, C_Token: Ctc);

                return rs;
            }
            else
                ShowHelp(p);

            return "0";
        }
        private static string MainExec(string sourceDacFilePath, string sourceConnectionString, string targerConnectionString, string username, string password,  DacDeployOptions options = null, CancellationTokenSource C_Token = null)
        {   
            using (var impersonator = new ImpersonateIt())
            {
                impersonator.Impersonate(username, password);
                //if (!System.IO.File.Exists(sourceDacFilePath))
                //{
                //    Console.WriteLine("source dac file does not exists, Creating new file. ");
                //    if (string.IsNullOrWhiteSpace(sourceConnectionString))
                //    {
                //        Console.Error.WriteLine("Source Connection string is required for creating a bac file.");
                //        return string.Empty;
                //    }
                
                //}
                Export(sourceConnectionString, @"C:\Temp\Source_dacFile.dacpac");
                Export(targerConnectionString, @"C:\Temp\Target_dacFile.dacpac");

                var TargetCon = new SqlConnectionStringBuilder(targerConnectionString);
                var TargetdacServices = new DacServices(TargetCon.ConnectionString);

                TargetdacServices.Message += ((s, e) => { Console.WriteLine(e?.Message.ToString()); });
                TargetdacServices.ProgressChanged += ((s, e) => { Console.WriteLine("Status:{0}, Message:{1}", e?.Status, e?.Message.ToString()); });

                if (options == null)
                {
                    options = new DacDeployOptions();
                }

                using (DacPackage dacpac = DacPackage.Load(sourceDacFilePath, DacSchemaModelStorageType.Memory))
                {

                    // Script then deploy, to support debugging of the generated plan
                    // string script = dacServices.GenerateDeployScript(dacpac, dbName, options);
                    var deployReport = TargetdacServices.GenerateDeployReport(dacpac, TargetCon.InitialCatalog);
                    
                    var deployScript = TargetdacServices.GenerateDeployScript(dacpac, TargetCon.InitialCatalog);

                    var DiffReport = TargetdacServices.GenerateDriftReport(TargetCon.InitialCatalog);

                    var outReportPath = Path.Combine(@"C:\Temp\", "DeployReport_" + DateTime.Now.ToString("yyyyMMMdd HHmmsstt") + ".sql");
                    System.IO.File.WriteAllText(outReportPath, deployReport);
                    var outScriptPath = Path.Combine(@"C:\Temp\", "DeployScript_" + DateTime.Now.ToString("yyyyMMMdd HHmmsstt") + ".sql");
                    System.IO.File.WriteAllText(outScriptPath, deployScript);
                    var outDiffReport = Path.Combine(@"C:\Temp\", "DeployDiff_" + DateTime.Now.ToString("yyyyMMMdd HHmmsstt") + ".sql");
                    System.IO.File.WriteAllText(outDiffReport, DiffReport);

                    Console.WriteLine("output Report and script generated.");
                    Console.WriteLine("DeployReport.{0}", deployReport);
                    Console.WriteLine("DiffReport.{0}", DiffReport);
                    Console.WriteLine("DeployScript.{0}", deployScript);
                    

                    return "Done.";
                }
            }
            return "";
        }


        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("SQL Db Schema Compare Tool");
            Console.WriteLine("Export SQL databases as bacpac.");
            Console.WriteLine();

            p.WriteOptionDescriptions(Console.Out);
        }




        public static bool Export(string sourceDbConString, string outDacFilePath)
        {
            bool result = false;
            var sourceConBuilder = new SqlConnectionStringBuilder(sourceDbConString);
            var services = new DacServices(sourceConBuilder.ConnectionString);

            services.ProgressChanged += ((s, e) => { Console.WriteLine("Ëxporting Dacpack Status:{0} , Message:{1}.", e.Status, e.Message); });

            string blobName;

            if (System.IO.File.Exists(outDacFilePath))
            {
                System.IO.File.Delete(outDacFilePath);
            }

            using (FileStream stream = File.Open(outDacFilePath, FileMode.Create, FileAccess.ReadWrite))
            {
                Console.WriteLine("starting bacpac export");

                 DacExportOptions opts = new DacExportOptions() {
                      TargetEngineVersion =EngineVersion.Default,
                      Storage = DacSchemaModelStorageType.Memory,
                      VerifyFullTextDocumentTypesSupported =false

                 };
                services.Extract(packageStream:stream, databaseName: sourceConBuilder.InitialCatalog, applicationName: "Schema_Exporter", applicationVersion: Version.Parse("1.0.0.0"));
                //services.ExportBacpac(stream, sourceConBuilder.InitialCatalog, options:opts,tables:null);

                stream.Flush();

                return true;
            }
            return result;

        }
        public static void Deploy(Stream dacpac, string connectionString, string databaseName)
        {
            var options = new DacDeployOptions()
            {
                BlockOnPossibleDataLoss = true,
                IncludeTransactionalScripts = true,
                DropConstraintsNotInSource = false,
                DropIndexesNotInSource = false,
                DropDmlTriggersNotInSource = false,
                DropObjectsNotInSource = false,
                DropExtendedPropertiesNotInSource = false,
                DropPermissionsNotInSource = false,
                DropStatisticsNotInSource = false,
                DropRoleMembersNotInSource = false,
            };

            var service = new DacServices(connectionString);
            service.Message += (x, y) =>
            {
                Console.WriteLine(y.Message.Message);
            };
            try
            {
                using (var package = DacPackage.Load(dacpac))
                {
                    service.Deploy(package, databaseName, true, options);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message, true);
            }
        }


    }
}

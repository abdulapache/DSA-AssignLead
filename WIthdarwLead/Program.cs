using System.Data;
using System.Data.SqlClient;

namespace WithdarwLead
{
    class Program
    {
        static string connectionstring = "Server=ec2-3-143-227-246.us-east-2.compute.amazonaws.com,1433;Database=wasitee;User Id= WSUsr; Password=1234!@#$; TrustServerCertificate=True";

        static void Main(string[] args)
        {
        Start:
            var dsa = GoSalesAgentList();
             if (dsa != null)
                {
                    foreach (DataRow row in dsa.Rows)
                    {
                        var userId = Convert.ToInt32(row[0].ToString());
                        if (userId > 0)
                        {
                            AssignLeads(userId);
                        }
                    }
                }


            DataTable GoSalesAgentList()
            {
                DataTable dt = new DataTable();
                SqlDataAdapter adapter = new SqlDataAdapter();
                try
                {
                    SqlConnection con = new SqlConnection();
                    con.ConnectionString = connectionstring;
                    con.Open();
                    SqlCommand cmd = new SqlCommand();
                    cmd.Connection = con;
                    cmd.CommandText = "SELECT Id FROM BusinessUsers WHERE IsActive = 0 and Roles IN('MM2','MM1','MM3') AND Id IN ( SELECT CurrentAssignTo FROM LeadEngine  WHERE ProcessedStatus = 0 GROUP BY CurrentAssignTo HAVING COUNT(*) > 0); ";
                    adapter.SelectCommand = cmd;
                    adapter.Fill(dt);
                    con.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("There is an error on businese user fetching!");
                    Console.WriteLine(ex.Message);
                }
                return dt;
            }

            int AssignLeads(int agentId)
            {
                int leadCount = GetLeadCount(agentId);
                if (leadCount > 0)
                {
                    var otherAgentId = GetAgentWithLeastLeads();
                    if (otherAgentId != -1)
                    {
                        ReassignLeads(agentId, otherAgentId);
                        Console.WriteLine($"100 leads reassigned from AgentId: {agentId} to AgentId: {otherAgentId}");
                    }
                    else
                    {
                        Console.WriteLine("No agents available for lead reassignment.");
                    }
                }
                else
                {
                    Console.WriteLine($"AgentId: {agentId} does not have more than 100 leads.");
                }
                return leadCount;
            }

            int GetLeadCount(int agentId)
            {
                using (SqlConnection conn = new SqlConnection(connectionstring))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("SELECT COUNT(*) FROM LeadEngine WHERE CurrentAssignTo = @AgentId AND ProcessedStatus = 0", conn))
                    {
                        cmd.Parameters.AddWithValue("@AgentId", agentId);
                        return (int)cmd.ExecuteScalar();
                    }
                }
            }

            int GetAgentWithLeastLeads()
            {
                int agentId = -1;
                int minLeadCount = int.MaxValue;

                using (SqlConnection conn = new SqlConnection(connectionstring))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("SELECT CurrentAssignTo, COUNT(*) AS LeadCount FROM LeadEngine WHERE ProcessedStatus = 0 AND CurrentAssignTo IN (SELECT Id FROM BusinessUsers WHERE IsActive = 1  AND Roles IN ('MM2', 'MM1', 'MM3')) GROUP BY CurrentAssignTo HAVING COUNT(*) < 100 ORDER BY LeadCount ASC", conn))
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            //int currentAgentId = reader.GetInt32(0);
                            //int leadCount = reader.GetInt32(1);

                            //if (leadCount < 100)
                            //{
                            //    minLeadCount = leadCount;
                            //    agentId = currentAgentId;
                            //}
                            agentId = reader.GetInt32(0);
                        }
                    }
                }

                return agentId;
            }

            void ReassignLeads(int sourceAgentId, int targetAgentId)
            {
                using (SqlConnection conn = new SqlConnection(connectionstring))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("UPDATE TOP (80) LeadEngine SET CurrentAssignTo = @TargetAgentId WHERE CurrentAssignTo = @SourceAgentId", conn))
                    {
                        cmd.Parameters.AddWithValue("@SourceAgentId", sourceAgentId);
                        cmd.Parameters.AddWithValue("@TargetAgentId", targetAgentId);
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            Thread.Sleep(86400000);
            Console.WriteLine("Lead Assignement has done successfully for total:" + DateTime.Now.ToString());
            goto Start;
        }
       
    }
}
using System.Data;
using System.Data.SqlClient;

namespace WithdarwLead
{
    class Program
    {
        static string connectionstring = "Server=ec2-3-143-227-246.us-east-2.compute.amazonaws.com,1433;Database=gosales;User Id= WSUsr; Password=1234!@#$; TrustServerCertificate=True";


        static Timer timer;

        static void Main(string[] args)
        {
            // Start the timer to execute the program every 30 minutes
            SetTimer();

            // Keep the program running
            Console.ReadLine();
        }

        static void SetTimer()
        {
            // Calculate the time until the next 30-minute interval
            DateTime now = DateTime.Now;
            DateTime nextRunTime = now.AddMinutes(5- now.Minute % 5).AddSeconds(-now.Second);

            // Check if it's Sunday, if yes, set the next run time to next Monday
            if (nextRunTime.DayOfWeek == DayOfWeek.Sunday)
            {
                nextRunTime = nextRunTime.AddDays(1);
            }

            // Calculate the delay until the next run time
            TimeSpan delay = nextRunTime - now;

            // Create a timer to trigger the program execution
            timer = new Timer(ExecuteProgram, null, delay, TimeSpan.FromMinutes(5));
        }

        static void ExecuteProgram(object state)
        {

            if (IsWithinAllowedTimeRange())
            {

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
            }
            else
            {
                Console.WriteLine("Program execution skipped due to time restriction.");
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
                    cmd.CommandText = "select Id from BusinessUsers where IsActive=1 and Roles IN('MM2','MM1','MM3') ";
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
                if (leadCount > 100)
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
                    using (SqlCommand cmd = new SqlCommand("SELECT CurrentAssignTo, COUNT(*) AS LeadCount FROM LeadEngine WHERE ProcessedStatus = 0 AND CurrentAssignTo IN (SELECT Id FROM BusinessUsers WHERE IsActive = 1  AND Roles IN ('MM2', 'MM1', 'MM3')) GROUP BY CurrentAssignTo ORDER BY LeadCount ASC", conn))
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int currentAgentId = reader.GetInt32(0);
                            int leadCount = reader.GetInt32(1);

                            if (leadCount < 100)
                            {
                                minLeadCount = leadCount;
                                agentId = currentAgentId;
                            }
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
                    using (SqlCommand cmd = new SqlCommand("UPDATE TOP (100) LeadEngine SET CurrentAssignTo = @TargetAgentId WHERE CurrentAssignTo = @SourceAgentId", conn))
                    {
                        cmd.Parameters.AddWithValue("@SourceAgentId", sourceAgentId);
                        cmd.Parameters.AddWithValue("@TargetAgentId", targetAgentId);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            
            
            Console.WriteLine("with draw lead every 15 mint and assign to other user");
            
        }


        static bool IsWithinAllowedTimeRange()
        {
            // Check if the current time is between 9 AM and 7 PM
            TimeSpan now = DateTime.Now.TimeOfDay;
            TimeSpan startTime = TimeSpan.FromHours(4);
            TimeSpan endTime = TimeSpan.FromHours(14);

            return now >= startTime && now <= endTime;
        }
    }
}
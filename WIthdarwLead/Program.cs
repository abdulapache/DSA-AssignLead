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
            DateTime nextRunTime = now.AddMinutes(5 - now.Minute % 5).AddSeconds(-now.Second);

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

                var activeAgent = ActiveAgentList();
                var inActiveAgent = InActiveAgentList();
                if (activeAgent != null)
                {
                    foreach (DataRow row in activeAgent.Rows)
                    {
                        var userId = Convert.ToInt32(row[0].ToString());
                        if (userId > 0)
                        {
                            checkCountActiveLead(userId);
                        }
                    }
                }
                if(inActiveAgent != null)
                {
                    foreach (DataRow row in inActiveAgent.Rows)
                    {
                        var userId = Convert.ToInt32(row[0].ToString());
                        if (userId > 0)
                        {
                            UpdateLead(userId);
                        }
                    }
                }
            }
            else
            {
                Console.WriteLine("Program execution skipped due to time restriction.");
            }
            DataTable ActiveAgentList()
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
                    cmd.CommandText = "SELECT Id FROM BusinessUsers WHERE IsActive = 1 and Roles IN('MM2','MM1','MM3') AND Id IN ( SELECT CurrentAssignTo FROM LeadEngine  WHERE ProcessedStatus = 0 GROUP BY CurrentAssignTo HAVING COUNT(*) < 100); ";
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

            DataTable InActiveAgentList()
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
                    cmd.CommandText = "select Id from BusinessUsers where IsActive=0 and Roles IN('MM2','MM1','MM3') ";
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

            void checkCountActiveLead(int agentId)
            {
                SqlDataAdapter ad = new SqlDataAdapter();
                DataTable dt = new DataTable();
                int leadCount = GetLeadCount(agentId);
                var totalAssginLead = 100 - leadCount;
                try
                {
                    SqlConnection conn = new SqlConnection(connectionstring);
                    conn.Open();
                    SqlCommand cmd = conn.CreateCommand();

                    cmd.CommandText = " SELECT TOP ({totalAssginLead}) Email, Phone, FirstName, LastName, SourceEmail  ,id   FROM FreshLeadsTbl      WHERE isdeployed = 0  and SourceProject IN('Hemingway')  and Phone not in(select phone from Customers)";


                    ad.SelectCommand = cmd;
                    ad.Fill(dt);

                    // upload this cutomer

                    conn.Close();
                    var c = assignLead(dt, agentId);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex);
                }

            }

            void UpdateLead(int agentId)
            {
                using (SqlConnection conn = new SqlConnection(connectionstring))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand("UPDATE TOP (100) LeadEngine SET CurrentAssignTo =  WHERE CurrentAssignTo = @SourceAgentId", conn))
                    {
                        cmd.Parameters.AddWithValue("@SourceAgentId", agentId);
                        cmd.Parameters.AddWithValue("@TargetAgentId", 4000);
                        cmd.ExecuteNonQuery();
                    }
                }
            }


            int assignLead(DataTable dt, int agentId)
            {
                var count = 0;
                if (dt.Rows.Count > 0)
                {
                    foreach (DataRow item in dt.Rows)
                    {
                        try
                        {
                            String FirstName = string.Empty;
                            string LastName = string.Empty;
                            string Phone = string.Empty;
                            string Email = string.Empty;
                            string SourceEmail = string.Empty;
                            string FlLead = string.Empty;

                            Email = item[0].ToString() ?? string.Empty;
                            Phone = item[1].ToString() ?? string.Empty;
                            FirstName = item[2].ToString() ?? string.Empty;
                            LastName = item[3].ToString() ?? string.Empty;
                            SourceEmail = item[4].ToString() ?? string.Empty;


                            var re = checkEmailPhone(Phone, Email);
                            if (re == 0)
                            {
                                SqlConnection con = new SqlConnection(connectionstring);
                                con.Open();

                                FlLead = item[5].ToString() ?? "0";
                                using (SqlCommand cmd = new SqlCommand())
                                {
                                    cmd.Connection = con;
                                    string insertQuery = "INSERT INTO Customers ([Email], [FirstName], [LastName], [LeadSourceEmail], [Phone]) VALUES (@Email, @FirstName, @LastName, @SourceEmail, @Phone); SELECT SCOPE_IDENTITY();";
                                    cmd.CommandText = insertQuery;
                                    cmd.Parameters.AddWithValue("@Email", Email);
                                    cmd.Parameters.AddWithValue("@FirstName", FirstName);
                                    cmd.Parameters.AddWithValue("@LastName", LastName);
                                    cmd.Parameters.AddWithValue("@SourceEmail", SourceEmail);
                                    cmd.Parameters.AddWithValue("@Phone", Phone);
                                    int customerId = Convert.ToInt32(cmd.ExecuteScalar());
                                    con.Close();

                                    con.Open();
                                    string insertQuery1 = "INSERT INTO LeadEngine ([CustomerID], [CurrentAssignTo], [IsActive], [LeadBy], [ProcessedStatus], [AssignDate], [FreshLeadId]) VALUES (@CustomerId, @AgentId, 1, 0, 0, GETDATE(), @FlLead)";
                                    cmd.CommandText = insertQuery1;
                                    cmd.Parameters.Clear(); // Clear previous parameters
                                    cmd.Parameters.AddWithValue("@CustomerId", customerId);
                                    cmd.Parameters.AddWithValue("@AgentId", agentId);
                                    cmd.Parameters.AddWithValue("@FlLead", FlLead);
                                    cmd.ExecuteNonQuery();
                                    con.Close();
                                    count++;
                                }

                                
                            }
                            else
                            {
                                Console.WriteLine("customer assigned already!");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("There is an error on businese uploadFreshLead!");
                            Console.WriteLine(ex.Message);
                        }


                    }
                }
                else
                {

                    Console.WriteLine("All Leads from Fresh-Databse has been deployed!");

                }
                return count;
            }
            int checkEmailPhone(string phone, string email)
            {
                var res = 0;
                SqlConnection con = new SqlConnection(connectionstring);
                con.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;
                string query = "SELECT COUNT(*) FROM Customers WHERE phone = @Phone";
                cmd.CommandText = query;
                cmd.Parameters.AddWithValue("@Phone", phone); // Assuming 'phone' is a string variable
                object result = cmd.ExecuteScalar();
                if (result != null && result != DBNull.Value)
                {
                    int count = Convert.ToInt32(result);
                    if (count > 0)
                    {
                        Console.WriteLine("Email and Phone already exist: Mobile:" + phone + "-Email:" + email);
                        res = 1;
                    }
                }
                con.Close();
                return res;
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

































            //int GetAgentWithLeastLeads()
            //{
            //    int agentId = -1;
            //    int minLeadCount = int.MaxValue;

            //    using (SqlConnection conn = new SqlConnection(connectionstring))
            //    {
            //        conn.Open();
            //        using (SqlCommand cmd = new SqlCommand("SELECT CurrentAssignTo, COUNT(*) AS LeadCount FROM LeadEngine WHERE ProcessedStatus = 0 AND CurrentAssignTo IN (SELECT Id FROM BusinessUsers WHERE IsActive = 1  AND Roles IN ('MM2', 'MM1', 'MM3')) GROUP BY CurrentAssignTo ORDER BY LeadCount ASC", conn))
            //        using (SqlDataReader reader = cmd.ExecuteReader())
            //        {
            //            while (reader.Read())
            //            {
            //                int currentAgentId = reader.GetInt32(0);
            //                int leadCount = reader.GetInt32(1);

            //                if (leadCount < 100)
            //                {
            //                    minLeadCount = leadCount;
            //                    agentId = currentAgentId;
            //                }
            //            }
            //        }
            //    }

            //    return agentId;
            //}

            //void ReassignLeads(int sourceAgentId, int targetAgentId)
            //{
            //    using (SqlConnection conn = new SqlConnection(connectionstring))
            //    {
            //        conn.Open();
            //        using (SqlCommand cmd = new SqlCommand("UPDATE TOP (100) LeadEngine SET CurrentAssignTo = @TargetAgentId WHERE CurrentAssignTo = @SourceAgentId", conn))
            //        {
            //            cmd.Parameters.AddWithValue("@SourceAgentId", sourceAgentId);
            //            cmd.Parameters.AddWithValue("@TargetAgentId", targetAgentId);
            //            cmd.ExecuteNonQuery();
            //        }
            //    }
            //}


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